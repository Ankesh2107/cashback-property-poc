#!/usr/bin/env python3
"""
OPTIONAL STRETCH: mock webhook endpoint (localhost only, test data only).

POST /events   JSON body, header  X-Signature: sha256=<hex HMAC-SHA256 of the raw body>
  reply: {"type":"reply","event_id":"E100","lead_id":"L008","reply_text":"Yes, let's speak."}
  lead : {"type":"lead","lead_id":"L200","first_name":"..","last_name":"..","company":"..","email":"..",
          "role":"..","city":"..","source":"..","source_date":"DD-MM-YYYY","consent_status":"..","suppressed":"no","notes":""}

Responses: 200 accepted | 200 duplicate (idempotent) | 400 bad payload | 401 bad/missing signature.
Same gates and dedupe as the batch pipeline - it reuses pipeline.py functions.

    WEBHOOK_SECRET=test-secret python3 webhook_server.py        # listens on 127.0.0.1:8099
"""
import hashlib
import hmac
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

import pipeline as p

SECRET = os.environ.get("WEBHOOK_SECRET", "")
PORT = int(os.environ.get("WEBHOOK_PORT", "8099"))


def sign(body: bytes, secret: str = None) -> str:
    return "sha256=" + hmac.new((secret or SECRET).encode(), body, hashlib.sha256).hexdigest()


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        conn = p.connect()
        run_id = "WH" + p.now()
        got = self.headers.get("X-Signature", "")
        if not SECRET or not hmac.compare_digest(got, sign(body)):
            with conn:
                p.audit(conn, run_id, "WEBHOOK_REJECTED", "request", self.path, "bad or missing signature")
            return self._reply(401, {"error": "invalid signature"})
        try:
            msg = json.loads(body)
            kind = msg["type"]
        except (ValueError, KeyError):
            return self._reply(400, {"error": "body must be JSON with a 'type' field"})

        if kind == "reply":
            row = {k: (msg.get(k) or "") for k in p.REQUIRED_REPLY_COLS}
            if not all(v.strip() for v in row.values()):
                with conn:
                    p.dead_letter(conn, run_id, "webhook", 0, "missing field(s)", row)
                return self._reply(400, {"error": "event_id, lead_id and reply_text are required"})
            result = p.apply_event(conn, run_id, row, p.Faults([]))
            p.build_drafts(conn, run_id)
            return self._reply(200, {"status": "duplicate" if result == "duplicate" else "accepted",
                                     "event_id": row["event_id"]})
        if kind == "lead":
            row = {k: str(msg.get(k) or "") for k in p.REQUIRED_LEAD_COLS}
            if not row["lead_id"].strip():
                return self._reply(400, {"error": "lead_id required"})
            with conn:
                cur = conn.execute(
                    f"INSERT OR IGNORE INTO raw_leads({','.join(p.REQUIRED_LEAD_COLS)},imported_run) "
                    f"VALUES({','.join('?' * len(p.REQUIRED_LEAD_COLS))},?)",
                    [row[c] for c in p.REQUIRED_LEAD_COLS] + [run_id])
            if cur.rowcount:
                p.build_leads(conn, run_id)
                p.build_drafts(conn, run_id)
            m = conn.execute("SELECT master_lead_id FROM lead_members WHERE raw_lead_id=?", (row["lead_id"],)).fetchone()
            st = conn.execute("SELECT status FROM leads WHERE lead_id=?", (m["master_lead_id"],)).fetchone() if m else None
            return self._reply(200, {"status": "accepted" if cur.rowcount else "duplicate",
                                     "lead_id": row["lead_id"], "lead_status": st["status"] if st else None})
        return self._reply(400, {"error": f"unknown type {kind!r}"})

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    if not SECRET:
        sys.exit("Set WEBHOOK_SECRET (a test value) before starting.")
    print(f"mock webhook on http://127.0.0.1:{PORT}/events  (test data only)")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
