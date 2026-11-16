#!/usr/bin/env python3
"""
Cashback Property - Web-based CRM automation system.
Flask app wrapping the pipeline with Gemini AI integration.

    pip install flask google-generativeai
    python app.py                          # http://127.0.0.1:5000
"""
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import (Flask, flash, jsonify, redirect, render_template,
                   request, url_for)

import gemini_ai
import pipeline as p

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "cashback-poc-dev-key")

UPLOAD_DIR = Path(__file__).resolve().parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)


def get_conn():
    conn = p.connect()
    api_key = conn.execute("SELECT value FROM settings WHERE key='gemini_api_key'").fetchone()
    if api_key and api_key["value"] and not gemini_ai.is_configured():
        gemini_ai.configure(api_key["value"])
    return conn


def lead_dict(row):
    return {k: row[k] for k in row.keys()} if row else None


# ------------------------------------------------------------------ routes

@app.route("/")
def index():
    conn = get_conn()
    has_data = conn.execute("SELECT COUNT(*) c FROM raw_leads").fetchone()["c"] > 0
    if has_data:
        return redirect(url_for("dashboard"))
    return render_template("index.html")


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "GET":
        return render_template("upload.html")

    files = request.files.getlist("csvfiles")
    if not files or all(f.filename == "" for f in files):
        flash("Please select at least one CSV file.", "error")
        return redirect(url_for("upload"))

    conn = get_conn()
    run_id = "W" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")[:-3]
    conn.execute("INSERT OR IGNORE INTO runs VALUES(?,?,?,?,?,?)",
                 (run_id, p.now(), None, "RUNNING", "web-upload", ""))
    conn.commit()
    results = []

    for f in files:
        if not f.filename:
            continue
        dest = UPLOAD_DIR / f.filename
        f.save(str(dest))
        try:
            import csv as csv_mod
            with open(dest, newline="", encoding="utf-8-sig") as fh:
                reader = csv_mod.DictReader(fh)
                cols = reader.fieldnames or []

            if "lead_id" in cols and "email" in cols and "consent_status" in cols:
                total, new = p.import_leads(conn, run_id, str(dest))
                p.build_leads(conn, run_id)
                created, cancelled = p.build_drafts(conn, run_id)
                results.append(f"Leads: {total} rows, {new} new, {created} drafts queued")
            elif "event_id" in cols and "reply_text" in cols:
                stats = p.import_replies(conn, run_id, str(dest), p.Faults([]))
                p.build_leads(conn, run_id)
                p.build_drafts(conn, run_id)
                results.append(f"Replies: {stats['rows']} rows, {stats['stored']} stored, {stats['duplicate']} duplicates")
            else:
                results.append(f"{f.filename}: unrecognized CSV format (need lead or reply columns)")
        except p.ConfigError as e:
            results.append(f"{f.filename}: {e}")
        except Exception as e:
            results.append(f"{f.filename}: error - {e}")

    with conn:
        conn.execute("UPDATE runs SET finished_at=?, status='OK', note=? WHERE run_id=?",
                     (p.now(), "; ".join(results), run_id))
        p.audit(conn, run_id, "RUN_FINISHED", "run", run_id, "; ".join(results))
    p.export_all(conn)

    for r in results:
        flash(r, "success" if "error" not in r.lower() else "error")
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    conn = get_conn()
    s = p.summary(conn)
    recent = conn.execute(
        "SELECT * FROM audit_log ORDER BY id DESC LIMIT 20"
    ).fetchall()
    cr = p.cleaning_report(conn) if s["raw_rows"] > 0 else None
    return render_template("dashboard.html", stats=s, recent=recent,
                           ai_configured=gemini_ai.is_configured(),
                           cleaning=cr)


@app.route("/leads")
def leads():
    conn = get_conn()
    status_filter = request.args.get("status", "all")
    sql = ("SELECT l.*, COALESCE(s.stage,'NEW') AS stage FROM leads l "
           "LEFT JOIN lead_stage s USING(lead_id)")
    if status_filter != "all":
        sql += f" WHERE l.status='{status_filter}'"
    sql += " ORDER BY CAST(SUBSTR(l.lead_id,2) AS INT)"
    rows = conn.execute(sql).fetchall()
    counts = {r["status"]: r["c"] for r in
              conn.execute("SELECT status, COUNT(*) c FROM leads GROUP BY status")}
    counts["all"] = sum(counts.values())
    return render_template("leads.html", leads=rows, counts=counts,
                           current_filter=status_filter)


@app.route("/leads/<lead_id>")
def lead_detail(lead_id):
    conn = get_conn()
    lead = conn.execute("SELECT * FROM leads WHERE lead_id=?", (lead_id,)).fetchone()
    if not lead:
        flash(f"Lead {lead_id} not found.", "error")
        return redirect(url_for("leads"))

    lead_data = lead_dict(lead)
    raw_lead = conn.execute("SELECT * FROM raw_leads WHERE lead_id=?", (lead_id,)).fetchone()
    raw_data = lead_dict(raw_lead) if raw_lead else None
    stage = conn.execute("SELECT * FROM lead_stage WHERE lead_id=?", (lead_id,)).fetchone()
    members = conn.execute(
        "SELECT * FROM lead_members WHERE master_lead_id=? ORDER BY is_master DESC",
        (lead_id,)
    ).fetchall()
    draft = conn.execute("SELECT * FROM drafts WHERE lead_id=?", (lead_id,)).fetchone()
    events = conn.execute(
        "SELECT * FROM events WHERE master_lead_id=? ORDER BY event_id", (lead_id,)
    ).fetchall()
    tasks = conn.execute(
        "SELECT * FROM tasks WHERE lead_id=? ORDER BY task_id", (lead_id,)
    ).fetchall()

    ai_draft = None
    if lead["status"] == "ELIGIBLE_FOR_DRAFT":
        ai_draft = gemini_ai.generate_outreach(lead_data, conn)

    scenarios = {}
    for row in conn.execute("SELECT * FROM ai_content WHERE lead_id=? AND content_type LIKE 'reply_%'",
                            (lead_id,)).fetchall():
        cat = row["content_type"].replace("reply_", "")
        scenarios[cat] = {
            "subject": row["subject"], "body": row["body"],
            "sample_incoming": row["sample_incoming"], "cached": True
        }

    return render_template("lead_detail.html", lead=lead_data, raw=raw_data,
                           stage=stage,
                           members=members, draft=draft, events=events, tasks=tasks,
                           ai_draft=ai_draft, scenarios=scenarios,
                           all_categories=[c[0] for c in gemini_ai.REPLY_CATEGORIES],
                           ai_configured=gemini_ai.is_configured())


@app.route("/api/generate-draft/<lead_id>", methods=["POST"])
def api_generate_draft(lead_id):
    conn = get_conn()
    lead = conn.execute("SELECT * FROM leads WHERE lead_id=?", (lead_id,)).fetchone()
    if not lead:
        return jsonify({"error": "Lead not found"}), 404
    conn.execute("DELETE FROM ai_content WHERE id=?", (f"{lead_id}:outreach",))
    conn.commit()
    result = gemini_ai.generate_outreach(lead_dict(lead), conn)
    result["ai_configured"] = gemini_ai.is_configured()
    result["last_error"] = gemini_ai.get_last_error()
    return jsonify(result)


@app.route("/api/test-gemini")
def api_test_gemini():
    conn = get_conn()
    api_key = conn.execute("SELECT value FROM settings WHERE key='gemini_api_key'").fetchone()
    info = {
        "configured": gemini_ai.is_configured(),
        "has_key": bool(api_key and api_key["value"]),
        "last_error": gemini_ai.get_last_error(),
        "model": gemini_ai._model_name,
    }
    if gemini_ai.is_configured():
        try:
            raw = gemini_ai._call_api(
                'Return ONLY this exact JSON: {"test": "ok", "message": "Gemini is working"}',
                max_tokens=100, temp=0
            )
            info["raw_response"] = (raw or "")[:500]
            info["parsed"] = gemini_ai._parse_json(raw)
        except Exception as e:
            info["error"] = str(e)
    return jsonify(info)


@app.route("/api/generate-scenarios/<lead_id>", methods=["POST"])
def api_generate_scenarios(lead_id):
    conn = get_conn()
    lead = conn.execute("SELECT * FROM leads WHERE lead_id=?", (lead_id,)).fetchone()
    if not lead:
        return jsonify({"error": "Lead not found"}), 404
    conn.execute("DELETE FROM ai_content WHERE lead_id=? AND content_type LIKE 'reply_%'", (lead_id,))
    conn.commit()
    results = gemini_ai.generate_all_scenarios(lead_dict(lead), conn)
    return jsonify(results)


@app.route("/api/generate-all", methods=["POST"])
def api_generate_all():
    conn = get_conn()
    leads = conn.execute("SELECT * FROM leads WHERE status='ELIGIBLE_FOR_DRAFT'").fetchall()
    count = 0
    for lead in leads:
        ld = lead_dict(lead)
        gemini_ai.generate_outreach(ld, conn)
        gemini_ai.generate_all_scenarios(ld, conn)
        count += 1
    return jsonify({"generated": count})


@app.route("/api/stats")
def api_stats():
    conn = get_conn()
    s = p.summary(conn)
    return jsonify(s)


@app.route("/drafts")
def drafts():
    conn = get_conn()
    rows = conn.execute(
        "SELECT d.*, l.first_name, l.last_name, l.company, l.role_norm, l.city_norm, l.segment "
        "FROM drafts d JOIN leads l USING(lead_id) ORDER BY d.draft_id"
    ).fetchall()
    return render_template("drafts.html", drafts=rows, ai_configured=gemini_ai.is_configured())


@app.route("/events")
def events_page():
    conn = get_conn()
    rows = conn.execute(
        "SELECT e.*, l.first_name, l.last_name, l.company "
        "FROM events e LEFT JOIN leads l ON e.master_lead_id=l.lead_id ORDER BY e.event_id"
    ).fetchall()
    tasks = conn.execute("SELECT * FROM tasks ORDER BY task_id").fetchall()
    return render_template("events.html", events=rows, tasks=tasks)


@app.route("/settings", methods=["GET", "POST"])
def settings():
    conn = get_conn()
    if request.method == "POST":
        api_key = request.form.get("gemini_api_key", "").strip()
        if api_key:
            result = gemini_ai.configure(api_key)
            if result is True:
                conn.execute("INSERT OR REPLACE INTO settings VALUES('gemini_api_key', ?)", (api_key,))
                conn.commit()
                flash("Gemini API key saved and verified.", "success")
            else:
                flash(f"API key test failed: {result}", "error")
        else:
            conn.execute("DELETE FROM settings WHERE key='gemini_api_key'")
            conn.commit()
            flash("API key removed.", "info")
        return redirect(url_for("settings"))

    current_key = conn.execute("SELECT value FROM settings WHERE key='gemini_api_key'").fetchone()
    masked = ""
    if current_key and current_key["value"]:
        k = current_key["value"]
        masked = k[:8] + "*" * (len(k) - 12) + k[-4:] if len(k) > 12 else "****"
    return render_template("settings.html", masked_key=masked,
                           ai_configured=gemini_ai.is_configured())


@app.route("/audit")
def audit_log():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 200").fetchall()
    return render_template("audit.html", logs=rows)


@app.route("/load-sample", methods=["POST"])
def load_sample():
    conn = get_conn()
    run_id = "S" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")[:-3]
    conn.execute("INSERT OR IGNORE INTO runs VALUES(?,?,?,?,?,?)",
                 (run_id, p.now(), None, "RUNNING", "sample-load", ""))
    conn.commit()
    try:
        total, new = p.import_leads(conn, run_id, str(p.LEADS_CSV))
        p.build_leads(conn, run_id)
        p.build_drafts(conn, run_id)
        stats = p.import_replies(conn, run_id, str(p.REPLIES_CSV), p.Faults([]))
        p.build_leads(conn, run_id)
        p.build_drafts(conn, run_id)
        with conn:
            conn.execute("UPDATE runs SET finished_at=?, status='OK' WHERE run_id=?", (p.now(), run_id))
            p.audit(conn, run_id, "RUN_FINISHED", "run", run_id, "sample data loaded")
        p.export_all(conn)
        flash(f"Sample data loaded: {total} leads, {stats['stored']} reply events.", "success")
    except Exception as e:
        flash(f"Error loading sample data: {e}", "error")
    return redirect(url_for("dashboard"))


@app.route("/reset", methods=["POST"])
def reset_data():
    conn = get_conn()
    for table in ("raw_leads", "leads", "lead_members", "lead_stage", "drafts",
                  "events", "tasks", "dead_letter", "audit_log", "runs", "ai_content"):
        conn.execute(f"DELETE FROM {table}")
    conn.commit()
    flash("All data cleared. Suppression list and settings preserved.", "info")
    return redirect(url_for("index"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    print(f"Cashback Property PoC - http://127.0.0.1:{port}")
    app.run(debug=debug, host="0.0.0.0", port=port)
