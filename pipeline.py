#!/usr/bin/env python3
"""
Cashback Property - technical automation proof-of-concept.

Import -> normalise -> dedupe -> consent/suppression gate -> segment -> draft queue
-> simulated reply triage -> CRM-style tasks -> audit log -> report.

SAFETY: this program has NO sending code path. Drafts are rows in SQLite with
send_state locked to 'NOT_SENT' by a CHECK constraint. Sample data only.

Usage:
    python3 pipeline.py run                      # full end-to-end run (safe to repeat)
    python3 pipeline.py run --inject crash_after:E012      # deliberate failure
    python3 pipeline.py run --inject flaky_destination:E010:2   # retry path
    python3 pipeline.py reset                    # delete the tracker database
    python3 pipeline.py export                   # re-export CSVs + report only
"""
import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
OUT = Path(os.environ.get("CASHBACK_OUT", ROOT / "output"))
DB_PATH = OUT / "tracker.db"
LEADS_CSV = DATA / "Cashback_Candidate_Sample_Leads_90_Raw.csv"
REPLIES_CSV = DATA / "Cashback_Candidate_Sample_Replies_25_Raw.csv"

REQUIRED_LEAD_COLS = ["lead_id", "first_name", "last_name", "company", "email", "role",
                      "city", "source", "source_date", "consent_status", "suppressed", "notes"]
REQUIRED_REPLY_COLS = ["event_id", "lead_id", "reply_text"]

SEGMENTS = {
    "mortgage broker": ("MORTGAGE_BROKER", "Mortgage Broker", "mortgage brokers"),
    "accountant": ("ACCOUNTANT", "Accountant", "accountants"),
    "financial planner": ("FINANCIAL_PLANNER", "Financial Planner", "financial planners"),
}
# Anything else ("Broker", "Solicitor", "Buyer Agent", "Real Estate Agent", blank) goes to review.

EMAIL_RE = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9\-]+(\.[a-z0-9\-]+)+$")
STATUS_RANK = {"ELIGIBLE_FOR_DRAFT": 1, "HOLD_REVIEW": 2, "INVALID": 3, "SUPPRESSED": 4}

DOMAIN_TYPOS = {
    "gmaiil.com": "gmail.com", "gmai.com": "gmail.com", "gmial.com": "gmail.com",
    "gmal.com": "gmail.com", "gmaill.com": "gmail.com", "gamil.com": "gmail.com",
    "gnail.com": "gmail.com", "gmail.co": "gmail.com", "gmail.con": "gmail.com",
    "gmailcom": "gmail.com", "gmaail.com": "gmail.com", "gmil.com": "gmail.com",
    "yahooo.com": "yahoo.com", "yahooo.com.au": "yahoo.com.au", "yaho.com": "yahoo.com",
    "yaoo.com": "yahoo.com", "yahoo.con": "yahoo.com", "yahoocom": "yahoo.com",
    "hotmal.com": "hotmail.com", "hotmai.com": "hotmail.com", "hotmaill.com": "hotmail.com",
    "hotmial.com": "hotmail.com", "hotmail.con": "hotmail.com",
    "outlok.com": "outlook.com", "outllook.com": "outlook.com", "outlook.con": "outlook.com",
}
TLD_TYPOS = {".comm": ".com", ".con": ".com", ".coom": ".com", ".nett": ".net", ".orgg": ".org",
             ".co.auu": ".co.au", ".com.auu": ".com.au"}


def fix_email(raw_email):
    e = (raw_email or "").strip().lower()
    if not e:
        return e
    while "@@" in e:
        e = e.replace("@@", "@")
    e = re.sub(r"\.{2,}", ".", e)
    e = e.rstrip(".")
    if "@" not in e:
        return e
    local, domain = e.rsplit("@", 1)
    if domain in DOMAIN_TYPOS:
        domain = DOMAIN_TYPOS[domain]
    else:
        for bad, good in TLD_TYPOS.items():
            if domain.endswith(bad):
                domain = domain[:-len(bad)] + good
                break
    return f"{local}@{domain}"
STAGE_RANK = {"NEW": 0, "NOT_NOW": 20, "WRONG_PERSON": 30, "QUESTION": 40,
              "INTERESTED": 50, "UNSUBSCRIBED": 100}

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, status TEXT, args TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS raw_leads (            -- untouched originals = provenance
  lead_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, company TEXT, email TEXT,
  role TEXT, city TEXT, source TEXT, source_date TEXT, consent_status TEXT,
  suppressed TEXT, notes TEXT, imported_run TEXT);
CREATE TABLE IF NOT EXISTS leads (                -- one row per unique lead (derived, rebuilt each run)
  lead_id TEXT PRIMARY KEY, email_norm TEXT, email_valid INTEGER, dedupe_key TEXT UNIQUE,
  first_name TEXT, last_name TEXT, company TEXT, role_norm TEXT, segment TEXT, city_norm TEXT,
  source TEXT, source_date TEXT, consent_status TEXT, status TEXT NOT NULL,
  status_reasons TEXT, member_ids TEXT, merged_count INTEGER, updated_at TEXT);
CREATE TABLE IF NOT EXISTS lead_members (         -- which raw rows were merged into which lead
  raw_lead_id TEXT PRIMARY KEY, master_lead_id TEXT, dedupe_key TEXT, is_master INTEGER,
  source TEXT, source_date TEXT, differences TEXT);
CREATE TABLE IF NOT EXISTS lead_stage (           -- CRM stage, only ever moves up
  lead_id TEXT PRIMARY KEY, stage TEXT, last_event_id TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS suppression_list (     -- PERSISTENT, never cleared by reruns
  key TEXT PRIMARY KEY, key_type TEXT, reason TEXT, source_event TEXT, added_at TEXT);
CREATE TABLE IF NOT EXISTS drafts (
  draft_id TEXT PRIMARY KEY, lead_id TEXT, segment TEXT, to_email TEXT, subject TEXT, body TEXT,
  approval_state TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
  send_state TEXT NOT NULL DEFAULT 'NOT_SENT' CHECK (send_state = 'NOT_SENT'),
  state_reason TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS events (               -- PRIMARY KEY on event_id = idempotency
  event_id TEXT PRIMARY KEY, lead_id TEXT, master_lead_id TEXT, reply_text TEXT,
  category TEXT, payload_hash TEXT, received_run TEXT, processed_at TEXT);
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, event_id TEXT, lead_id TEXT, task_type TEXT, title TEXT,
  status TEXT, due_date TEXT, created_at TEXT, UNIQUE(event_id, task_type));
CREATE TABLE IF NOT EXISTS dead_letter (          -- rejected input, kept so nothing is lost
  dl_id TEXT PRIMARY KEY, source_file TEXT, row_num INTEGER, reason TEXT, raw_json TEXT,
  run_id TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, run_id TEXT, action TEXT,
  entity_type TEXT, entity_id TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS ai_content (
  id TEXT PRIMARY KEY, lead_id TEXT, content_type TEXT, subject TEXT, body TEXT,
  sample_incoming TEXT DEFAULT '', model TEXT, created_at TEXT);
"""


# ----------------------------------------------------------------------------- helpers
def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect():
    OUT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def audit(conn, run_id, action, etype, eid, detail=""):
    conn.execute("INSERT INTO audit_log(ts,run_id,action,entity_type,entity_id,detail) VALUES(?,?,?,?,?,?)",
                 (now(), run_id, action, etype, eid, detail))


def clean_name(s):
    s = re.sub(r"\s+", " ", (s or "").strip())
    return " ".join("-".join(p[:1].upper() + p[1:].lower() for p in w.split("-")) for w in s.split(" "))


def clean_label(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def clean_city(s):
    return clean_name(s)


def parse_date(s):
    try:
        return datetime.strptime((s or "").strip(), "%d-%m-%Y").date().isoformat()
    except ValueError:
        return None


def lead_sort_key(lead_id):
    m = re.search(r"\d+", lead_id or "")
    return (int(m.group()) if m else 10 ** 9, lead_id)


class ConfigError(Exception):
    """Unrecoverable input problem (e.g. file missing / wrong header). Fails the run loudly."""


class Faults:
    """Deliberate-failure switches for the demo. Parsed from --inject."""
    def __init__(self, specs):
        self.crash_after = None
        self.flaky = {}          # event_id -> remaining failures
        for s in specs or []:
            parts = s.split(":")
            if parts[0] == "crash_after":
                self.crash_after = parts[1]
            elif parts[0] == "flaky_destination":
                self.flaky[parts[1]] = int(parts[2]) if len(parts) > 2 else 1
            else:
                raise ConfigError(f"unknown --inject spec: {s}")

    def maybe_fail_write(self, event_id):
        if self.flaky.get(event_id, 0) > 0:
            self.flaky[event_id] -= 1
            raise sqlite3.OperationalError("simulated: destination unavailable")


def with_retry(fn, conn, run_id, label, attempts=3, base_delay=0.2):
    """Retry transient destination errors with exponential backoff; give up -> dead letter by caller."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            audit(conn, run_id, "RETRY", "write", label, f"attempt {i}/{attempts} failed: {e}")
            conn.commit()
            if i == attempts:
                raise
            time.sleep(base_delay * 2 ** (i - 1))


def read_csv(path, required_cols):
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"input file not found: {path}")
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        missing = [c for c in required_cols if c not in (reader.fieldnames or [])]
        if missing:
            raise ConfigError(f"{path.name}: missing required columns {missing}")
        return list(reader)


def dead_letter(conn, run_id, source_file, row_num, reason, row):
    raw = json.dumps(row, sort_keys=True)
    dl_id = hashlib.sha1(f"{source_file}|{row_num}|{raw}".encode()).hexdigest()[:16]
    cur = conn.execute("INSERT OR IGNORE INTO dead_letter VALUES(?,?,?,?,?,?,?)",
                       (dl_id, source_file, row_num, reason, raw, run_id, now()))
    if cur.rowcount:
        audit(conn, run_id, "DEAD_LETTER", "row", f"{source_file}#{row_num}", reason)


# ----------------------------------------------------------------------------- A. leads
def import_leads(conn, run_id, path):
    rows = read_csv(path, REQUIRED_LEAD_COLS)
    new = 0
    with conn:
        for n, r in enumerate(rows, start=2):
            if not (r.get("lead_id") or "").strip():
                dead_letter(conn, run_id, Path(path).name, n, "missing lead_id", r)
                continue
            vals = [r[c] for c in REQUIRED_LEAD_COLS]
            cur = conn.execute(
                f"INSERT OR IGNORE INTO raw_leads({','.join(REQUIRED_LEAD_COLS)},imported_run) "
                f"VALUES({','.join('?' * len(REQUIRED_LEAD_COLS))},?)", vals + [run_id])
            if cur.rowcount:
                new += 1
                audit(conn, run_id, "LEAD_IMPORTED", "lead", r["lead_id"], f"source={r['source']}")
    return len(rows), new


def suppression_keys(conn):
    keys = {"email": set(), "lead_id": set()}
    for r in conn.execute("SELECT key,key_type FROM suppression_list"):
        keys[r["key_type"]].add(r["key"])
    return keys


def member_checks(raw, email_norm, valid, supp):
    """Per raw-row findings: list of (status, reason)."""
    out = []
    if (raw["suppressed"] or "").strip().lower() == "yes":
        out.append(("SUPPRESSED", f"{raw['lead_id']}:SUPPRESSED_FLAG"))
    if (valid and email_norm in supp["email"]) or raw["lead_id"] in supp["lead_id"]:
        out.append(("SUPPRESSED", f"{raw['lead_id']}:ON_SUPPRESSION_LIST"))
    if not valid:
        reason = "MISSING_EMAIL" if not email_norm else "MALFORMED_EMAIL"
        out.append(("INVALID", f"{raw['lead_id']}:{reason}"))
    consent = (raw["consent_status"] or "").strip().lower()
    if consent != "documented":
        out.append(("HOLD_REVIEW", f"{raw['lead_id']}:CONSENT_{(consent or 'blank').upper()}"))
    if (raw["source"] or "").strip().lower() == "purchased_list":
        out.append(("HOLD_REVIEW", f"{raw['lead_id']}:PURCHASED_LIST"))
    return out


def build_leads(conn, run_id):
    """Rebuild the derived leads/lead_members tables from raw_leads + persistent suppression list."""
    raws = sorted(conn.execute("SELECT * FROM raw_leads").fetchall(), key=lambda r: lead_sort_key(r["lead_id"]))
    supp = suppression_keys(conn)
    previous = {r["lead_id"]: r["status"] for r in conn.execute("SELECT lead_id,status FROM leads")}

    groups, order = {}, []
    for r in raws:
        raw_email = (r["email"] or "").strip().lower()
        email = fix_email(raw_email)
        if email != raw_email and raw_email:
            audit(conn, run_id, "EMAIL_FIXED", "lead", r["lead_id"],
                  f"{raw_email!r} -> {email!r}")
        valid = bool(EMAIL_RE.match(email))
        key = email if valid else f"__row__{r['lead_id']}"      # invalid emails never merge with each other
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append((r, email, valid))

    with conn:
        conn.execute("DELETE FROM leads")
        conn.execute("DELETE FROM lead_members")
        for key in order:
            members = groups[key]
            master, email, valid = members[0]               # earliest lead_id wins
            seg_key = clean_label(master["role"]).lower()
            segment = SEGMENTS.get(seg_key)
            role_norm = segment[1] if segment else clean_label(master["role"])
            findings = []
            for raw, e, v in members:
                findings += member_checks(raw, e, v, supp)
            sd = parse_date(master["source_date"])
            if segment is None:
                why = "ROLE_BLANK" if not role_norm else f"ROLE_NOT_IN_SEGMENTS({role_norm})"
                findings.append(("HOLD_REVIEW", f"{master['lead_id']}:{why}"))
            if sd is None:
                findings.append(("HOLD_REVIEW", f"{master['lead_id']}:BAD_SOURCE_DATE"))
            status = max((f[0] for f in findings), key=STATUS_RANK.get) if findings else "ELIGIBLE_FOR_DRAFT"
            reasons = "; ".join(f[1] for f in findings if f[0] == status) if findings else "ALL_GATES_PASSED"
            lid = master["lead_id"]
            conn.execute("INSERT INTO leads VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                lid, email, int(valid), key if valid else None,
                clean_name(master["first_name"]), clean_name(master["last_name"]),
                clean_label(master["company"]), role_norm, segment[0] if segment else None,
                clean_city(master["city"]), master["source"], sd, master["consent_status"],
                status, reasons, ",".join(m[0]["lead_id"] for m in members), len(members), now()))
            for raw, e, v in members:
                diffs = {}
                if raw is not master:
                    for col in ("first_name", "last_name", "company", "role", "city", "source", "consent_status"):
                        a, b = clean_label(raw[col]).lower(), clean_label(master[col]).lower()
                        if a != b:
                            diffs[col] = f"{clean_label(raw[col])!r} vs master {clean_label(master[col])!r}"
                conn.execute("INSERT INTO lead_members VALUES(?,?,?,?,?,?,?)", (
                    raw["lead_id"], lid, key if v else None, int(raw is master),
                    raw["source"], parse_date(raw["source_date"]), json.dumps(diffs) if diffs else ""))
                if raw is not master:
                    audit(conn, run_id, "LEAD_MERGED", "lead", raw["lead_id"],
                          f"merged into {lid} on email {e}")
            old = previous.get(lid)
            if old is None:
                audit(conn, run_id, "STATUS_SET", "lead", lid, f"{status} ({reasons})")
            elif old != status:
                audit(conn, run_id, "STATUS_CHANGED", "lead", lid, f"{old} -> {status} ({reasons})")
            conn.execute("INSERT OR IGNORE INTO lead_stage VALUES(?,?,?,?)", (lid, "NEW", None, now()))


# ----------------------------------------------------------------------------- B. drafts
def make_draft(lead):
    seg = [v for v in SEGMENTS.values() if v[0] == lead["segment"]][0]
    first, company = lead["first_name"], lead["company"]
    subject = f"Referral partnership - 15 minutes, {company}?"
    body = (
        f"Hi {first},\n\n"
        f"I'm writing from Cashback Property. We're looking to work with Australian {seg[2]} "
        f"as referral partners, and I'd like to explain how our referral program works and "
        f"whether it could suit {company}'s clients.\n\n"
        f"Would you be open to a 15-minute meeting next week? Happy to work around your calendar.\n\n"
        f"Kind regards,\n{{{{SENDER_NAME}}}}\nCashback Property\n{{{{SENDER_BUSINESS_DETAILS}}}}\n\n"
        f"If you'd rather not hear from us, reply 'unsubscribe' or use {{{{OPT_OUT_LINK}}}} "
        f"and we'll remove you straight away.\n")
    return subject, body


def build_drafts(conn, run_id):
    created = cancelled = 0
    with conn:
        for lead in conn.execute("SELECT * FROM leads").fetchall():
            did = f"D-{lead['lead_id']}-v1"
            existing = conn.execute("SELECT approval_state FROM drafts WHERE draft_id=?", (did,)).fetchone()
            if lead["status"] == "ELIGIBLE_FOR_DRAFT":
                if existing is None:
                    subject, body = make_draft(lead)
                    conn.execute("INSERT INTO drafts(draft_id,lead_id,segment,to_email,subject,body,"
                                 "approval_state,send_state,state_reason,created_at,updated_at) "
                                 "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                 (did, lead["lead_id"], lead["segment"], lead["email_norm"], subject, body,
                                  "PENDING_REVIEW", "NOT_SENT", "awaiting human approval", now(), now()))
                    audit(conn, run_id, "DRAFT_QUEUED", "draft", did, f"to {lead['email_norm']} - PENDING_REVIEW, NOT_SENT")
                    created += 1
            elif existing and existing["approval_state"] != "CANCELLED":
                conn.execute("UPDATE drafts SET approval_state='CANCELLED', state_reason=?, updated_at=? WHERE draft_id=?",
                             (f"lead now {lead['status']}: {lead['status_reasons']}", now(), did))
                audit(conn, run_id, "DRAFT_CANCELLED", "draft", did, f"lead now {lead['status']}")
                cancelled += 1
    return created, cancelled


# ----------------------------------------------------------------------------- C. replies
RULES = [  # ordered: first match wins. Deterministic - no API key needed.
    ("UNSUBSCRIBE", r"unsubscribe|remove me|remove my (contact|details|email)|stop (emailing|contacting)|do not contact|take me off"),
    ("OUT_OF_OFFICE", r"out of (the )?office|automatic reply|auto-?reply|annual leave|\baway (until|from|on)\b|on leave"),
    ("WRONG_PERSON", r"wrong (person|department|contact)|not responsible|contact (our|the) (director|manager|colleague)|not the right person"),
    ("NOT_NOW", r"not interested|no thanks|no thank you|maybe later|not now|later this|contact me in|in (january|february|march|april|may|june|july|august|september|october|november|december)\b|next (quarter|year)"),
    ("INTERESTED", r"interested|\byes\b|happy to|let'?s (speak|talk|chat)|schedule|book a|quick call|share some times|\bmeeting\b|sounds good"),
    ("QUESTION", r"\?|what is|how (does|do)|explain|overview|more information|more info|tell me more|send (me|a|some)|details"),
]


def classify(text):
    t = (text or "").lower()
    for cat, pat in RULES:
        if re.search(pat, t):
            return cat
    return "NEEDS_REVIEW"


def master_of(conn, raw_lead_id):
    r = conn.execute("SELECT master_lead_id FROM lead_members WHERE raw_lead_id=?", (raw_lead_id,)).fetchone()
    return r["master_lead_id"] if r else None


def raise_stage(conn, lead_id, stage, event_id):
    cur = conn.execute("SELECT stage FROM lead_stage WHERE lead_id=?", (lead_id,)).fetchone()
    if cur is None or STAGE_RANK[stage] > STAGE_RANK[cur["stage"]]:
        conn.execute("INSERT OR REPLACE INTO lead_stage VALUES(?,?,?,?)", (lead_id, stage, event_id, now()))
        return True
    return False


def apply_event(conn, run_id, row, faults):
    """One transaction per event: event row + all side effects commit together or not at all."""
    eid, raw_lid, text = row["event_id"].strip(), row["lead_id"].strip(), row["reply_text"].strip()
    payload_hash = hashlib.sha1(f"{eid}|{raw_lid}|{text}".encode()).hexdigest()[:12]
    with conn:
        existing = conn.execute("SELECT payload_hash FROM events WHERE event_id=?", (eid,)).fetchone()
        if existing:
            same = "identical payload" if existing["payload_hash"] == payload_hash else "DIFFERENT payload - kept first, review"
            audit(conn, run_id, "DUPLICATE_EVENT_IGNORED", "event", eid, same)
            return "duplicate"
        faults.maybe_fail_write(eid)                       # simulated flaky destination (rolls back)
        category = classify(text)
        master = master_of(conn, raw_lid)
        if master is None:
            category, note = "NEEDS_REVIEW", "unknown lead_id"
        else:
            note = ""
        conn.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?)",
                     (eid, raw_lid, master, text, category, payload_hash, run_id, now()))
        audit(conn, run_id, "EVENT_STORED", "event", eid, f"{category} lead={raw_lid}->{master} {note}".strip())
        if master is None:
            return "stored"
        lead = conn.execute("SELECT * FROM leads WHERE lead_id=?", (master,)).fetchone()
        blocked = lead["status"] == "SUPPRESSED"

        if category == "UNSUBSCRIBE":
            for key, ktype in ((lead["email_norm"] if lead["email_valid"] else None, "email"),
                               (raw_lid, "lead_id"), (master, "lead_id")):
                if key:
                    conn.execute("INSERT OR IGNORE INTO suppression_list VALUES(?,?,?,?,?)",
                                 (key, ktype, "unsubscribe reply", eid, now()))
            conn.execute("UPDATE leads SET status='SUPPRESSED', status_reasons=?, updated_at=? WHERE lead_id=?",
                         (f"{master}:UNSUBSCRIBE_REPLY({eid})", now(), master))
            conn.execute("UPDATE drafts SET approval_state='CANCELLED', state_reason=?, updated_at=? "
                         "WHERE lead_id=? AND approval_state!='CANCELLED'", (f"unsubscribe {eid}", now(), master))
            conn.execute("UPDATE tasks SET status='CANCELLED_SUPPRESSED' WHERE lead_id=? AND status='OPEN'", (master,))
            raise_stage(conn, master, "UNSUBSCRIBED", eid)
            audit(conn, run_id, "SUPPRESSED", "lead", master, f"unsubscribe via {eid}; persistent suppression written")
            if "remove my contact details" in text.lower():
                audit(conn, run_id, "PRIVACY_FLAG", "lead", master, f"{eid} reads as a data-removal request - route to privacy owner")
        elif category in ("INTERESTED", "QUESTION"):
            ttype = "FOLLOW_UP" if category == "INTERESTED" else "DRAFT_RESPONSE"
            if blocked:
                audit(conn, run_id, "TASK_BLOCKED", "lead", master, f"{eid}: lead is SUPPRESSED, no task created")
            else:
                title = ("Follow up: book 15-min meeting" if ttype == "FOLLOW_UP"
                         else "Prepare reply DRAFT for human approval (do not auto-send)")
                due = (datetime.now(timezone.utc) + timedelta(days=2)).date().isoformat()
                cur = conn.execute("INSERT OR IGNORE INTO tasks VALUES(?,?,?,?,?,?,?,?)",
                                   (f"T-{eid}-{ttype}", eid, master, ttype, title, "OPEN", due, now()))
                if cur.rowcount:
                    audit(conn, run_id, "TASK_CREATED", "task", f"T-{eid}-{ttype}", f"{ttype} for {master}")
            raise_stage(conn, master, category, eid)
        elif category in ("NOT_NOW", "WRONG_PERSON"):
            raise_stage(conn, master, category, eid)
        return "stored"


def import_replies(conn, run_id, path, faults):
    rows = read_csv(path, REQUIRED_REPLY_COLS)
    stats = {"rows": len(rows), "stored": 0, "duplicate": 0, "dead_letter": 0}
    for n, r in enumerate(rows, start=2):
        missing = [c for c in REQUIRED_REPLY_COLS if not (r.get(c) or "").strip()]
        if missing:
            with conn:
                dead_letter(conn, run_id, Path(path).name, n, f"missing field(s): {', '.join(missing)}", r)
            stats["dead_letter"] += 1
            continue
        try:
            result = with_retry(lambda: apply_event(conn, run_id, r, faults), conn, run_id, r["event_id"])
            stats[result] += 1
        except sqlite3.OperationalError as e:               # retries exhausted -> park it, keep going
            with conn:
                dead_letter(conn, run_id, Path(path).name, n, f"destination failed after retries: {e}", r)
            stats["dead_letter"] += 1
        if faults.crash_after == r["event_id"].strip():
            raise RuntimeError(f"simulated crash after {faults.crash_after}")
    return stats


# ----------------------------------------------------------------------------- D. reporting
def q(conn, sql, *a):
    return conn.execute(sql, a).fetchall()


def summary(conn):
    s = {}
    s["raw_rows"] = q(conn, "SELECT COUNT(*) c FROM raw_leads")[0]["c"]
    s["unique_leads"] = q(conn, "SELECT COUNT(*) c FROM leads")[0]["c"]
    s["rows_merged_away"] = s["raw_rows"] - s["unique_leads"]
    st = {r["status"]: r["c"] for r in q(conn, "SELECT status, COUNT(*) c FROM leads GROUP BY status")}
    s["suppressed"] = st.get("SUPPRESSED", 0)
    s["held_for_review"] = st.get("HOLD_REVIEW", 0)
    s["invalid"] = st.get("INVALID", 0)
    s["draft_ready"] = st.get("ELIGIBLE_FOR_DRAFT", 0)
    s["drafts_pending_review"] = q(conn, "SELECT COUNT(*) c FROM drafts WHERE approval_state='PENDING_REVIEW'")[0]["c"]
    s["drafts_cancelled"] = q(conn, "SELECT COUNT(*) c FROM drafts WHERE approval_state='CANCELLED'")[0]["c"]
    s["drafts_sent"] = q(conn, "SELECT COUNT(*) c FROM drafts WHERE send_state!='NOT_SENT'")[0]["c"]
    s["reply_events_stored"] = q(conn, "SELECT COUNT(*) c FROM events")[0]["c"]
    s["replies_by_category"] = {r["category"]: r["c"] for r in
                                q(conn, "SELECT category, COUNT(*) c FROM events GROUP BY category ORDER BY category")}
    s["open_tasks"] = q(conn, "SELECT COUNT(*) c FROM tasks WHERE status='OPEN'")[0]["c"]
    s["open_tasks_by_type"] = {r["task_type"]: r["c"] for r in
                               q(conn, "SELECT task_type, COUNT(*) c FROM tasks WHERE status='OPEN' GROUP BY task_type")}
    s["suppression_list_size"] = q(conn, "SELECT COUNT(*) c FROM suppression_list")[0]["c"]
    s["dead_letter_rows"] = q(conn, "SELECT COUNT(*) c FROM dead_letter")[0]["c"]
    s["segments_of_draft_ready"] = {r["segment"]: r["c"] for r in
                                    q(conn, "SELECT segment, COUNT(*) c FROM leads WHERE status='ELIGIBLE_FOR_DRAFT' GROUP BY segment")}
    return s


def cleaning_report(conn):
    """Compare raw_leads vs cleaned leads to show what was transformed."""
    report = {
        "names_cleaned": 0, "emails_fixed": 0, "dates_normalized": 0,
        "roles_normalized": 0, "cities_cleaned": 0, "duplicates_merged": 0,
        "examples": []
    }
    raws = {r["lead_id"]: r for r in conn.execute("SELECT * FROM raw_leads").fetchall()}
    leads = {r["lead_id"]: r for r in conn.execute("SELECT * FROM leads").fetchall()}
    members = conn.execute("SELECT master_lead_id, COUNT(*) c FROM lead_members GROUP BY master_lead_id HAVING c > 1").fetchall()
    report["duplicates_merged"] = sum(m["c"] - 1 for m in members)

    for lid, lead in leads.items():
        raw = raws.get(lid)
        if not raw:
            continue
        changes = []
        raw_name = f"{(raw['first_name'] or '').strip()} {(raw['last_name'] or '').strip()}"
        clean = f"{lead['first_name']} {lead['last_name']}"
        if raw_name != clean and raw_name.strip():
            report["names_cleaned"] += 1
            changes.append(("Name", raw_name, clean))
        raw_email = (raw["email"] or "").strip().lower()
        fixed = fix_email(raw_email)
        if raw_email != fixed and raw_email:
            report["emails_fixed"] += 1
            changes.append(("Email", raw_email, fixed))
        raw_date = (raw["source_date"] or "").strip()
        clean_date = lead["source_date"] or ""
        if raw_date and clean_date and raw_date != clean_date:
            report["dates_normalized"] += 1
            changes.append(("Date", raw_date, clean_date))
        raw_role = (raw["role"] or "").strip()
        if raw_role and lead["role_norm"] and raw_role != lead["role_norm"]:
            report["roles_normalized"] += 1
            changes.append(("Role", raw_role, lead["role_norm"]))
        raw_city = (raw["city"] or "").strip()
        if raw_city and lead["city_norm"] and raw_city != lead["city_norm"]:
            report["cities_cleaned"] += 1
            changes.append(("City", raw_city, lead["city_norm"]))
        if changes and len(report["examples"]) < 10:
            report["examples"].append({"lead_id": lid, "name": clean, "changes": changes})
    return report


EXPORTS = {
    "leads_clean.csv": "SELECT l.*, COALESCE(s.stage,'NEW') AS stage FROM leads l LEFT JOIN lead_stage s USING(lead_id) ORDER BY CAST(SUBSTR(l.lead_id,2) AS INT)",
    "merges.csv": "SELECT * FROM lead_members WHERE master_lead_id IN (SELECT master_lead_id FROM lead_members GROUP BY master_lead_id HAVING COUNT(*)>1) ORDER BY master_lead_id, is_master DESC, raw_lead_id",
    "draft_queue.csv": "SELECT * FROM drafts ORDER BY draft_id",
    "events.csv": "SELECT * FROM events ORDER BY event_id",
    "tasks.csv": "SELECT * FROM tasks ORDER BY task_id",
    "suppression_list.csv": "SELECT * FROM suppression_list ORDER BY added_at, key",
    "dead_letter.csv": "SELECT * FROM dead_letter ORDER BY created_at",
    "audit_log.csv": "SELECT * FROM audit_log ORDER BY id",
}


def export_all(conn):
    OUT.mkdir(exist_ok=True)
    for name, sql in EXPORTS.items():
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        with open(OUT / name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(cur.fetchall())
    s = summary(conn)
    (OUT / "summary.json").write_text(json.dumps(s, indent=2))
    write_report_md(s)
    write_dashboard(conn, s)
    return s


def write_report_md(s):
    cats = "\n".join(f"| {k} | {v} |" for k, v in s["replies_by_category"].items())
    tasks = ", ".join(f"{v} {k}" for k, v in s["open_tasks_by_type"].items()) or "none"
    md = f"""# Cashback Property PoC - run summary

| Metric | Count |
|---|---|
| Raw lead rows imported | {s['raw_rows']} |
| Unique leads (after email dedupe) | {s['unique_leads']} ({s['rows_merged_away']} rows merged away) |
| SUPPRESSED | {s['suppressed']} |
| HOLD_REVIEW | {s['held_for_review']} |
| INVALID | {s['invalid']} |
| ELIGIBLE_FOR_DRAFT (draft-ready) | {s['draft_ready']} |
| Drafts pending human review | {s['drafts_pending_review']} |
| Drafts cancelled (lead later suppressed) | {s['drafts_cancelled']} |
| Drafts actually sent | **{s['drafts_sent']}** |
| Reply events stored (unique event_id) | {s['reply_events_stored']} |
| Open tasks | {s['open_tasks']} ({tasks}) |
| Persistent suppression list entries | {s['suppression_list_size']} |
| Dead-letter rows (rejected input) | {s['dead_letter_rows']} |

## Replies by category
| Category | Count |
|---|---|
{cats}
"""
    (OUT / "summary_report.md").write_text(md)


def write_dashboard(conn, s):
    def bars(d, color):
        m = max(d.values()) if d else 1
        return "".join(f"<div class=r><span>{k}</span><i style='width:{int(300 * v / m)}px;background:{color}'></i><b>{v}</b></div>"
                       for k, v in d.items())
    status = {"ELIGIBLE_FOR_DRAFT": s["draft_ready"], "HOLD_REVIEW": s["held_for_review"],
              "SUPPRESSED": s["suppressed"], "INVALID": s["invalid"]}
    tiles = "".join(f"<div class=t><b>{v}</b><span>{k}</span></div>" for k, v in [
        ("raw rows", s["raw_rows"]), ("unique leads", s["unique_leads"]), ("draft-ready", s["draft_ready"]),
        ("open tasks", s["open_tasks"]), ("emails sent", s["drafts_sent"])])
    html = f"""<!doctype html><meta charset=utf-8><title>Cashback PoC dashboard</title>
<style>body{{font:14px system-ui;margin:24px;color:#222;max-width:760px}}.t{{display:inline-block;padding:12px 18px;margin:0 8px 8px 0;border:1px solid #ddd;border-radius:8px}}
.t b{{display:block;font-size:26px}}.r{{display:flex;align-items:center;gap:8px;margin:4px 0}}.r span{{width:170px}}.r i{{height:14px;border-radius:3px;display:inline-block}}h2{{margin-top:28px}}</style>
<h1>Cashback Property PoC</h1><p>Simulated run - sample data only, nothing is ever sent.</p>{tiles}
<h2>Lead status</h2>{bars(status, '#3b6fd4')}<h2>Replies by category</h2>{bars(s['replies_by_category'], '#2a9d6e')}
<h2>Open tasks</h2>{bars(s['open_tasks_by_type'], '#c9822b')}"""
    (OUT / "dashboard.html").write_text(html)


# ----------------------------------------------------------------------------- run / CLI
def run(args):
    conn = connect()
    run_id = "R" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")[:-3]   # ms resolution: unique per run
    faults = Faults(args.inject)
    conn.execute("INSERT INTO runs VALUES(?,?,?,?,?,?)", (run_id, now(), None, "RUNNING", " ".join(sys.argv[1:]), ""))
    conn.commit()
    try:
        total, new = import_leads(conn, run_id, args.leads)
        print(f"[{run_id}] leads: {total} rows read, {new} new in tracker")
        build_leads(conn, run_id)
        created, _ = build_drafts(conn, run_id)
        print(f"[{run_id}] drafts queued this run: {created}")
        stats = import_replies(conn, run_id, args.replies, faults)
        print(f"[{run_id}] replies: {stats}")
        build_drafts(conn, run_id)                         # re-check gates after suppression events
        with conn:
            conn.execute("UPDATE runs SET finished_at=?, status='OK', note=? WHERE run_id=?",
                         (now(), json.dumps(stats), run_id))
            audit(conn, run_id, "RUN_FINISHED", "run", run_id, json.dumps(stats))
        s = export_all(conn)
        print(f"[{run_id}] OK  suppressed={s['suppressed']} held={s['held_for_review']} invalid={s['invalid']} "
              f"draft_ready={s['draft_ready']} open_tasks={s['open_tasks']} sent={s['drafts_sent']}")
        return 0
    except Exception as e:                                  # error branch: record, keep data, non-zero exit
        conn.rollback()
        with conn:
            conn.execute("UPDATE runs SET finished_at=?, status='FAILED', note=? WHERE run_id=?", (now(), str(e), run_id))
            audit(conn, run_id, "RUN_FAILED", "run", run_id, str(e))
        export_all(conn)
        print(f"[{run_id}] FAILED: {e}  (committed data kept; rerun is safe)", file=sys.stderr)
        return 2


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--leads", default=str(LEADS_CSV))
    r.add_argument("--replies", default=str(REPLIES_CSV))
    r.add_argument("--inject", action="append", help="crash_after:E012 | flaky_destination:E010:2")
    sub.add_parser("reset")
    sub.add_parser("export")
    a = p.parse_args()
    if a.cmd == "reset":
        for f in OUT.glob("*"):
            f.unlink()
        print("tracker and exports deleted")
        return 0
    if a.cmd == "export":
        print(json.dumps(export_all(connect()), indent=2))
        return 0
    try:
        return run(a)
    except ConfigError as e:
        print(f"CONFIG ERROR: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
