#!/usr/bin/env python3
"""
Acceptance tests from the assignment (section 6) plus rerun / failure-recovery checks.
Each test class run uses a throw-away database so results never depend on earlier runs.

    python3 tests/test_acceptance.py          # prints PASS/FAIL per check, exit 1 on any failure
"""
import csv
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PIPE = [sys.executable, str(ROOT / "pipeline.py")]
results = []


def check(name, cond, detail=""):
    results.append(cond)
    print(("PASS  " if cond else "FAIL  ") + name + (f"   [{detail}]" if detail and not cond else ""))


def run(out, *extra):
    env = dict(os.environ, CASHBACK_OUT=str(out))
    return subprocess.run(PIPE + ["run", *extra], env=env, capture_output=True, text=True)


def db(out):
    c = sqlite3.connect(Path(out) / "tracker.db")
    c.row_factory = sqlite3.Row
    return c


def lead(c, lid):
    return c.execute("SELECT * FROM leads WHERE lead_id=?", (lid,)).fetchone()


def snapshot(c):
    return {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("raw_leads", "leads", "events", "tasks", "drafts", "suppression_list")}


def main():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        out = Path(tmp) / "a"
        r = run(out)
        check("pipeline run exits 0", r.returncode == 0, r.stderr)
        c = db(out)

        # 1 duplicate merge
        m = {x["raw_lead_id"]: x["master_lead_id"] for x in c.execute("SELECT * FROM lead_members")}
        check("1. L002 merges with L001 after normalisation", m.get("L002") == "L001" and lead(c, "L002") is None)

        # 2 suppressed L005, also after rerun
        check("2a. L005 is SUPPRESSED, no draft", lead(c, "L005")["status"] == "SUPPRESSED"
              and c.execute("SELECT COUNT(*) FROM drafts WHERE lead_id='L005'").fetchone()[0] == 0)
        before = snapshot(c)
        c.close()
        r2 = run(out)
        c = db(out)
        check("2b. L005 still blocked after rerun", lead(c, "L005")["status"] == "SUPPRESSED"
              and c.execute("SELECT COUNT(*) FROM drafts WHERE lead_id='L005'").fetchone()[0] == 0)

        # 3 purchased list held
        ok = all(lead(c, x)["status"] == "HOLD_REVIEW" and
                 c.execute("SELECT COUNT(*) FROM drafts WHERE lead_id=? AND approval_state='PENDING_REVIEW'", (x,)).fetchone()[0] == 0
                 for x in ("L003", "L011"))
        check("3. L003 and L011 are HOLD_REVIEW, not draft-ready", ok)

        # 4 malformed email
        check("4. L007 is INVALID", lead(c, "L007")["status"] == "INVALID")

        # 5 E002 suppresses L004, survives repeat import
        check("5a. E002 -> L004 SUPPRESSED and on persistent list",
              lead(c, "L004")["status"] == "SUPPRESSED"
              and c.execute("SELECT COUNT(*) FROM suppression_list WHERE key='clara@pacific.example'").fetchone()[0] == 1)
        check("5b. L004 stays suppressed after repeat import (and merged L049 cannot draft)",
              lead(c, "L004")["status"] == "SUPPRESSED"
              and c.execute("SELECT COUNT(*) FROM drafts WHERE lead_id IN ('L004','L049') AND approval_state='PENDING_REVIEW'").fetchone()[0] == 0)

        # 6 repeated E005
        check("6. E005 -> exactly 1 event and 1 task",
              c.execute("SELECT COUNT(*) FROM events WHERE event_id='E005'").fetchone()[0] == 1
              and c.execute("SELECT COUNT(*) FROM tasks WHERE event_id='E005'").fetchone()[0] == 1)

        # 7 nothing sent
        check("7. no draft has send_state other than NOT_SENT",
              c.execute("SELECT COUNT(*) FROM drafts WHERE send_state!='NOT_SENT'").fetchone()[0] == 0)
        try:
            c.execute("UPDATE drafts SET send_state='SENT'")
            blocked = False
        except sqlite3.IntegrityError:
            blocked = True
        check("7b. database refuses to mark a draft SENT (CHECK constraint)", blocked)

        # safe rerun
        check("R. rerun creates no duplicate rows", snapshot(c) == before, f"{before} vs {snapshot(c)}")

        # unsubscribe cancels existing draft
        check("U. drafts of unsubscribed leads are CANCELLED",
              c.execute("SELECT approval_state FROM drafts WHERE lead_id='L004'").fetchone()[0] == "CANCELLED")
        c.close()

    # deliberate failure 1: crash mid-run, then recover
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        out = Path(tmp) / "b"
        r = run(out, "--inject", "crash_after:E012")
        check("F1. injected crash exits non-zero", r.returncode == 2)
        c = db(out)
        partial = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        check("F1. partial data is kept (12 events), run marked FAILED",
              partial == 12 and c.execute("SELECT status FROM runs").fetchone()[0] == "FAILED")
        c.close()
        r = run(out)
        c = db(out)
        check("F1. recovery run completes", r.returncode == 0)
        check("F1. after recovery: 24 events, 9 tasks, no duplicates",
              c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 24
              and c.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 9
              and c.execute("SELECT COUNT(*) FROM (SELECT event_id,task_type FROM tasks GROUP BY 1,2 HAVING COUNT(*)>1)").fetchone()[0] == 0)
        c.close()

    # deliberate failure 2: flaky destination -> retry succeeds
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        out = Path(tmp) / "c"
        r = run(out, "--inject", "flaky_destination:E010:2")
        c = db(out)
        retries = c.execute("SELECT COUNT(*) FROM audit_log WHERE action='RETRY' AND entity_id='E010'").fetchone()[0]
        check("F2. flaky destination: 2 retries logged then success",
              r.returncode == 0 and retries == 2
              and c.execute("SELECT COUNT(*) FROM tasks WHERE event_id='E010'").fetchone()[0] == 1)
        c.close()

    # deliberate failure 3: destination never recovers -> dead letter, rest unaffected
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        out = Path(tmp) / "d"
        r = run(out, "--inject", "flaky_destination:E010:99")
        c = db(out)
        check("F3. exhausted retries -> E010 in dead_letter, other 23 events stored, no duplicates",
              c.execute("SELECT COUNT(*) FROM dead_letter WHERE raw_json LIKE '%E010%'").fetchone()[0] == 1
              and c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 23)
        r = run(out)                                         # E010 is picked up by a clean rerun
        c = db(out)
        check("F3. clean rerun stores the parked event; 24 events, 1 E010 task",
              c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 24
              and c.execute("SELECT COUNT(*) FROM tasks WHERE event_id='E010'").fetchone()[0] == 1)
        c.close()

    # deliberate failure 4: malformed input rows
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        bad = Path(tmp) / "bad_replies.csv"
        bad.write_text("event_id,lead_id,reply_text\nE900,L001,Interested please call\n,L002,missing event id\nE902,,missing lead id\n")
        out = Path(tmp) / "e"
        r = run(out, "--replies", str(bad))
        c = db(out)
        check("F4. rows with missing fields go to dead_letter; valid row still processed",
              c.execute("SELECT COUNT(*) FROM dead_letter").fetchone()[0] == 2
              and c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1)
        c.close()

    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
