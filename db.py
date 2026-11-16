"""
Database abstraction: SQLite (local dev) or PostgreSQL (production via DATABASE_URL).

Every other module calls db.connect() and uses ? placeholders + INSERT OR IGNORE/REPLACE.
This module translates transparently when DATABASE_URL is set.
"""
import os
import re
import sqlite3
from pathlib import Path

DATABASE_URL = os.environ.get("DATABASE_URL")

ROOT = Path(__file__).resolve().parent
OUT = Path(os.environ.get("CASHBACK_OUT", ROOT / "output"))
DB_PATH = OUT / "tracker.db"

TABLE_PKS = {
    "runs": "run_id", "raw_leads": "lead_id", "leads": "lead_id",
    "lead_members": "raw_lead_id", "lead_stage": "lead_id",
    "suppression_list": "key", "drafts": "draft_id", "events": "event_id",
    "tasks": "task_id", "dead_letter": "dl_id", "audit_log": "id",
    "settings": "key", "ai_content": "id",
}

TABLE_COLS = {
    "lead_stage": ["lead_id", "stage", "last_event_id", "updated_at"],
    "settings": ["key", "value"],
    "ai_content": ["id", "lead_id", "content_type", "subject", "body",
                   "sample_incoming", "model", "created_at"],
}

SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, status TEXT, args TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS raw_leads (
  lead_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, company TEXT, email TEXT,
  role TEXT, city TEXT, source TEXT, source_date TEXT, consent_status TEXT,
  suppressed TEXT, notes TEXT, imported_run TEXT);
CREATE TABLE IF NOT EXISTS leads (
  lead_id TEXT PRIMARY KEY, email_norm TEXT, email_valid INTEGER, dedupe_key TEXT UNIQUE,
  first_name TEXT, last_name TEXT, company TEXT, role_norm TEXT, segment TEXT, city_norm TEXT,
  source TEXT, source_date TEXT, consent_status TEXT, status TEXT NOT NULL,
  status_reasons TEXT, member_ids TEXT, merged_count INTEGER, updated_at TEXT);
CREATE TABLE IF NOT EXISTS lead_members (
  raw_lead_id TEXT PRIMARY KEY, master_lead_id TEXT, dedupe_key TEXT, is_master INTEGER,
  source TEXT, source_date TEXT, differences TEXT);
CREATE TABLE IF NOT EXISTS lead_stage (
  lead_id TEXT PRIMARY KEY, stage TEXT, last_event_id TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS suppression_list (
  key TEXT PRIMARY KEY, key_type TEXT, reason TEXT, source_event TEXT, added_at TEXT);
CREATE TABLE IF NOT EXISTS drafts (
  draft_id TEXT PRIMARY KEY, lead_id TEXT, segment TEXT, to_email TEXT, subject TEXT, body TEXT,
  approval_state TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
  send_state TEXT NOT NULL DEFAULT 'NOT_SENT' CHECK (send_state = 'NOT_SENT'),
  state_reason TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY, lead_id TEXT, master_lead_id TEXT, reply_text TEXT,
  category TEXT, payload_hash TEXT, received_run TEXT, processed_at TEXT);
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, event_id TEXT, lead_id TEXT, task_type TEXT, title TEXT,
  status TEXT, due_date TEXT, created_at TEXT, UNIQUE(event_id, task_type));
CREATE TABLE IF NOT EXISTS dead_letter (
  dl_id TEXT PRIMARY KEY, source_file TEXT, row_num INTEGER, reason TEXT, raw_json TEXT,
  run_id TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS audit_log (
  id SERIAL PRIMARY KEY, ts TEXT, run_id TEXT, action TEXT,
  entity_type TEXT, entity_id TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS ai_content (
  id TEXT PRIMARY KEY, lead_id TEXT, content_type TEXT, subject TEXT, body TEXT,
  sample_incoming TEXT DEFAULT '', model TEXT, created_at TEXT);
"""


def _sqlite_to_pg(sql):
    """Translate SQLite-flavour SQL to PostgreSQL."""
    s = sql.strip().replace("%", "%%")

    # INSERT OR REPLACE → upsert
    m = re.match(r"INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)", s, re.I)
    if m:
        table = m.group(1).lower()
        pk = TABLE_PKS.get(table, "id")
        s = re.sub(r"INSERT\s+OR\s+REPLACE\s+INTO", "INSERT INTO", s, count=1, flags=re.I)
        cols = TABLE_COLS.get(table)
        if cols:
            update_cols = [c for c in cols if c != pk]
            set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
            s += f" ON CONFLICT ({pk}) DO UPDATE SET {set_clause}"
        else:
            s += f" ON CONFLICT ({pk}) DO UPDATE SET updated_at=EXCLUDED.updated_at"
        s = s.replace("?", "%s")
        return s

    # INSERT OR IGNORE → ON CONFLICT DO NOTHING
    if re.search(r"INSERT\s+OR\s+IGNORE", s, re.I):
        s = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", s, count=1, flags=re.I)
        # Find the end of VALUES(...) clause
        paren_depth = 0
        insert_end = len(s)
        in_values = False
        for i, ch in enumerate(s):
            if s[i:i+6].upper() == "VALUES":
                in_values = True
            if in_values and ch == '(':
                paren_depth += 1
            elif in_values and ch == ')':
                paren_depth -= 1
                if paren_depth == 0:
                    insert_end = i + 1
                    break
        s = s[:insert_end] + " ON CONFLICT DO NOTHING" + s[insert_end:]
        s = s.replace("?", "%s")
        return s

    # Regular statements: just swap placeholders
    s = s.replace("?", "%s")

    # AUTOINCREMENT → handled by SERIAL in schema
    s = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "SERIAL PRIMARY KEY", s, flags=re.I)

    return s


class DictRow(dict):
    """Dict that supports both key and index access, like sqlite3.Row."""
    def keys(self):
        return list(super().keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class PgCursor:
    """Wraps psycopg2 cursor to return DictRow objects."""
    def __init__(self, cur):
        self._cur = cur

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def description(self):
        return self._cur.description

    def __iter__(self):
        return iter(self.fetchall())

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None or self._cur.description is None:
            return None
        cols = [d[0] for d in self._cur.description]
        return DictRow(zip(cols, row))

    def fetchall(self):
        if self._cur.description is None:
            return []
        rows = self._cur.fetchall()
        cols = [d[0] for d in self._cur.description]
        return [DictRow(zip(cols, r)) for r in rows]


class PgConnection:
    """Wraps psycopg2 connection to match sqlite3.Connection interface."""

    def __init__(self, dsn):
        import psycopg2
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False

    def execute(self, sql, params=None):
        pg_sql = _sqlite_to_pg(sql)
        cur = self._conn.cursor()
        try:
            cur.execute(pg_sql, tuple(params) if params else ())
        except Exception as e:
            self._conn.rollback()
            raise
        return PgCursor(cur)

    def executescript(self, script):
        cur = self._conn.cursor()
        for stmt in script.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        self._conn.commit()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


def is_postgres():
    return bool(DATABASE_URL)


def connect():
    """Return a connection. Auto-detects PostgreSQL via DATABASE_URL env var."""
    if DATABASE_URL:
        conn = PgConnection(DATABASE_URL)
        conn.executescript(SCHEMA_PG)
        return conn
    OUT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    from pipeline import SCHEMA
    conn.executescript(SCHEMA)
    return conn
