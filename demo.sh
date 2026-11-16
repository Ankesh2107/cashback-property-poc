#!/usr/bin/env bash
# Screen-recording helper: runs the whole story in ~2 minutes of terminal time.
# Usage: bash demo.sh        (press Enter between steps; set NOPAUSE=1 to skip pauses)
set -u
cd "$(dirname "$0")"
step() { echo; echo "=================== $1"; [ "${NOPAUSE:-0}" = 1 ] || read -r -p "(Enter to run) " _; }
show() { python3 - "$@" <<'EOF'
import csv, sys
f, ids = sys.argv[1], set(sys.argv[2:])
for r in csv.DictReader(open(f)):
    if not ids or r.get("lead_id") in ids or r.get("event_id") in ids:
        print({k: r[k][:70] for k in list(r)[:8]})
EOF
}

step "1. Fresh start + first run (import, clean, dedupe, gates, drafts, reply triage)"
python3 pipeline.py reset && python3 pipeline.py run

step "2. Acceptance cases: merge / suppressed / purchased / invalid"
python3 - <<'EOF'
import sqlite3
c = sqlite3.connect("output/tracker.db"); c.row_factory = sqlite3.Row
for lid in ("L001", "L003", "L005", "L007", "L011"):
    r = c.execute("select lead_id,status,merged_count,member_ids,status_reasons from leads where lead_id=?", (lid,)).fetchone()
    print(dict(r))
print("L002 as its own lead? ->", c.execute("select count(*) from leads where lead_id='L002'").fetchone()[0], "(merged into L001)")
EOF

step "3. Draft review queue - never sent (send_state is locked to NOT_SENT)"
head -c 1400 output/draft_queue.csv; echo; echo "..."; python3 -c "
import sqlite3; c=sqlite3.connect('output/tracker.db')
print(c.execute('select approval_state,send_state,count(*) from drafts group by 1,2').fetchall())"

step "4. Reply triage + tasks + suppression (E002 suppresses L004, E005 appears twice)"
show output/events.csv; echo; cat output/suppression_list.csv; echo; cat output/tasks.csv | cut -c1-110

step "5. Safe rerun: counts must be identical"
python3 pipeline.py run
python3 -c "
import sqlite3; c=sqlite3.connect('output/tracker.db')
print('events',c.execute('select count(*) from events').fetchone()[0],'| tasks',c.execute('select count(*) from tasks').fetchone()[0],
 '| E005 events/tasks',c.execute(\"select count(*) from events where event_id='E005'\").fetchone()[0], c.execute(\"select count(*) from tasks where event_id='E005'\").fetchone()[0],
 '| L004,L005 status', c.execute(\"select status from leads where lead_id in ('L004','L005')\").fetchall())"

step "6. Deliberate failure: crash mid-run after E012, then recover"
python3 pipeline.py reset >/dev/null; python3 pipeline.py run --inject crash_after:E012; echo "exit code: $?"
python3 -c "import sqlite3; c=sqlite3.connect('output/tracker.db'); print('events kept so far:', c.execute('select count(*) from events').fetchone()[0], '| run statuses:', c.execute('select status from runs').fetchall())"
python3 pipeline.py run
python3 -c "import sqlite3; c=sqlite3.connect('output/tracker.db'); print('after recovery -> events', c.execute('select count(*) from events').fetchone()[0], 'tasks', c.execute('select count(*) from tasks').fetchone()[0])"

step "7. Retry path: destination fails twice for E010, then succeeds"
python3 pipeline.py reset >/dev/null; python3 pipeline.py run --inject flaky_destination:E010:2
grep -E "RETRY|TASK_CREATED,task,T-E010" output/audit_log.csv | cut -c1-140

step "8. Summary report + dashboard"
cat output/summary_report.md; echo "dashboard: output/dashboard.html"

step "9. All acceptance + failure tests"
python3 tests/test_acceptance.py
python3 pipeline.py reset >/dev/null; python3 pipeline.py run | tail -1
