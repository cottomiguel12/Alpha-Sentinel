# /opt/alpha-sentinel/app/db.py
import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "/data/sentinel.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db():
    """
    Usage:
        with db() as conn:
            conn.execute(...)
    """
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:

        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT UNIQUE NOT NULL,
          password_hash TEXT NOT NULL,
          role TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          last_login_at TEXT
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          ticker TEXT NOT NULL,
          exp TEXT NOT NULL,
          strike REAL NOT NULL,
          opt_type TEXT NOT NULL,
          premium REAL,
          size INTEGER,
          volume INTEGER,
          oi INTEGER,
          bid REAL,
          ask REAL,
          spread_pct REAL,
          spot REAL,
          otm_pct REAL,
          dte INTEGER,
          score_total REAL NOT NULL,
          tags TEXT,
          reason_codes TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ticker ON alerts(ticker)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          agent_status TEXT NOT NULL,
          ws_status TEXT NOT NULL,
          last_event_ts TEXT,
          last_alert_ts TEXT,
          events_per_min INTEGER NOT NULL,
          alerts_per_min INTEGER NOT NULL,
          errors_15m INTEGER NOT NULL
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_health_ts ON health_snapshots(ts)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS monitor (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          contract_key TEXT UNIQUE NOT NULL,
          ticker TEXT NOT NULL,
          exp TEXT NOT NULL,
          strike REAL NOT NULL,
          opt_type TEXT NOT NULL,
          entry_score REAL NOT NULL,
          current_score REAL NOT NULL,
          peak_score REAL NOT NULL,
          score_history TEXT NOT NULL,
          status TEXT NOT NULL,
          last_update_ts TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
          contract_key TEXT PRIMARY KEY,
          added_by TEXT,
          created_at TEXT,
          is_active INTEGER DEFAULT 1,
          notes TEXT
        )
        """)

        # --- Unusual Whales integration tables (always created, only used when UW_ENABLED=1) ---

        conn.execute("""
        CREATE TABLE IF NOT EXISTS integrations_state (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS uw_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          ticker TEXT NOT NULL,
          exp TEXT,
          strike REAL,
          opt_type TEXT,
          premium REAL,
          size INTEGER,
          bid REAL,
          ask REAL,
          price REAL,
          flags_json TEXT,
          raw_json TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_uw_events_ts ON uw_events(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_uw_events_ticker_ts ON uw_events(ticker, ts)")

        # --- Non-destructive column migrations ---

        # Add alerts.source column if it doesn't exist yet (safe for existing DBs)
        existing_alert_cols = {r["name"] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
        if "source" not in existing_alert_cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN source TEXT")

        # Add alerts.contract_key column if it doesn't exist yet (legacy migration)
        if "contract_key" not in existing_alert_cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN contract_key TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_contract_key ON alerts(contract_key)")