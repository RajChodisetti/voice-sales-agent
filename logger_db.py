"""
================================================================================
  CALL LOGGER — Saves transcripts, outcomes, and opt-outs to local SQLite.
  Swap DATABASE_URL with PostgreSQL + Redis in production.
================================================================================
"""

import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.environ.get("CALL_LOG_DB", "calls.db")


def init_db():
    """Create tables if they don't exist."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS calls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            call_sid    TEXT,
            to_number   TEXT,
            started_at  TEXT,
            ended_at    TEXT,
            duration_s  REAL,
            outcome     TEXT,   -- 'booked' | 'not_interested' | 'opted_out' | 'transferred' | 'no-answer' | 'busy' | 'failed' | 'completed' | 'unknown'
            booking     TEXT,   -- JSON blob if appointment was booked
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS transcripts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id     INTEGER REFERENCES calls(id),
            role        TEXT,   -- 'user' | 'assistant' | 'system'
            content     TEXT,
            ts          TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS opt_outs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            phone        TEXT NOT NULL,
            call_id      INTEGER REFERENCES calls(id),
            opted_out_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_opt_outs_phone ON opt_outs (phone);
    """)
    con.commit()
    con.close()


def start_call(call_sid: str, to_number: str) -> int:
    """Insert a new call row and return its DB id."""
    init_db()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO calls (call_sid, to_number, started_at, outcome) VALUES (?, ?, ?, ?)",
        (call_sid, to_number, datetime.utcnow().isoformat(), "unknown"),
    )
    call_id = cur.lastrowid
    con.commit()
    con.close()
    return call_id


def log_turn(call_id: int, role: str, content: str):
    """Append a transcript line."""
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO transcripts (call_id, role, content) VALUES (?, ?, ?)",
        (call_id, role, content),
    )
    con.commit()
    con.close()


def end_call(
    call_id: int,
    outcome: str,
    booking: dict | None = None,
    duration_s: float | None = None,
):
    """Mark the call as ended. Safe to call with the same call_id more than once."""
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """UPDATE calls
           SET ended_at   = ?,
               outcome    = ?,
               booking    = ?,
               duration_s = ?
           WHERE id = ?""",
        (
            datetime.utcnow().isoformat(),
            outcome,
            json.dumps(booking) if booking else None,
            duration_s,
            call_id,
        ),
    )
    con.commit()
    con.close()


def get_call_summary(call_id: int) -> dict:
    """Return the full call record + transcript."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    call = con.execute("SELECT * FROM calls WHERE id = ?", (call_id,)).fetchone()
    turns = con.execute(
        "SELECT role, content, ts FROM transcripts WHERE call_id = ? ORDER BY id",
        (call_id,),
    ).fetchall()
    con.close()
    if not call:
        return {}
    return {
        **dict(call),
        "transcript": [dict(t) for t in turns],
    }


def list_calls(limit: int = 20) -> list[dict]:
    """Return recent calls without transcripts."""
    init_db()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM calls ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_call_number(call_id: int) -> str | None:
    """Return the to_number stored for a given call_id."""
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT to_number FROM calls WHERE id = ?", (call_id,)).fetchone()
    con.close()
    return row[0] if row else None


def is_opted_out(phone: str) -> bool:
    """Return True if the number is in the internal do-not-call list."""
    init_db()
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT 1 FROM opt_outs WHERE phone = ? LIMIT 1", (phone,)
    ).fetchone()
    con.close()
    return row is not None


def record_opt_out(phone: str, call_id: int):
    """
    Add phone to the internal do-not-call list (idempotent).
    Also marks the call outcome as 'opted_out'.
    """
    init_db()
    con = sqlite3.connect(DB_PATH)
    existing = con.execute(
        "SELECT 1 FROM opt_outs WHERE phone = ? LIMIT 1", (phone,)
    ).fetchone()
    if not existing:
        con.execute(
            "INSERT INTO opt_outs (phone, call_id) VALUES (?, ?)",
            (phone, call_id),
        )
    # Also mark the call outcome directly so it survives even if flush is late
    con.execute(
        "UPDATE calls SET outcome = 'opted_out' WHERE id = ? AND outcome NOT IN ('opted_out')",
        (call_id,),
    )
    con.commit()
    con.close()
