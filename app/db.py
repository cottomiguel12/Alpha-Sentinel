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
          reason_codes TEXT,
          ingested_at TEXT,
          trade_time_raw TEXT,
          trade_tz TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ticker ON alerts(ticker)")

        # --- Simulation Tables ---
        conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_sim_alerts (
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
          reason_codes TEXT,
          source TEXT,
          contract_key TEXT,
          ingested_at TEXT,
          trade_time_raw TEXT,
          trade_tz TEXT,
          UNIQUE(contract_key, trade_time_raw, premium, size) ON CONFLICT IGNORE
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts_live (
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
          reason_codes TEXT,
          source TEXT,
          contract_key TEXT,
          ingested_at TEXT,
          trade_time_raw TEXT,
          trade_tz TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_live_ts ON alerts_live(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_live_ck_ts ON alerts_live(contract_key, ts)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_alerts (
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
          reason_codes TEXT,
          source TEXT,
          contract_key TEXT,
          ingested_at TEXT,
          trade_time_raw TEXT,
          trade_tz TEXT,
          trade_id TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_alerts_ts ON sim_alerts(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_alerts_ck_ts ON sim_alerts(contract_key, ts)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_state (
          id INTEGER PRIMARY KEY,
          is_running INTEGER NOT NULL DEFAULT 0,
          is_paused INTEGER NOT NULL DEFAULT 0,
          cursor_id INTEGER NOT NULL DEFAULT 1,
          speed_per_tick INTEGER NOT NULL DEFAULT 25,
          interval_sec REAL NOT NULL DEFAULT 1.0,
          last_tick_ts TEXT
        )
        """)
        # Initialize singleton row
        conn.execute("""
        INSERT OR IGNORE INTO sim_state (id, is_running, is_paused, cursor_id, speed_per_tick, interval_sec)
        VALUES (1, 0, 0, 1, 25, 1.0)
        """)

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
          errors_15m INTEGER NOT NULL,
          source TEXT DEFAULT 'live'
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

        # --- Market Tide tables ---
        conn.execute("""
        CREATE TABLE IF NOT EXISTS market_tide_ticks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          interval TEXT NOT NULL,
          date TEXT NOT NULL,
          net_call_premium REAL,
          net_put_premium REAL,
          net_volume INTEGER,
          raw TEXT,
          created_at TEXT
        )
        """)
        
        # Add non-destructive column migrations for market_tide_ticks
        existing_tide_cols = {r["name"] for r in conn.execute("PRAGMA table_info(market_tide_ticks)").fetchall()}
        for col, col_def in [
            ("interval", "TEXT NOT NULL DEFAULT '1m'"),
            ("date", "TEXT NOT NULL DEFAULT ''"),
            ("raw", "TEXT"),
            ("created_at", "TEXT")
        ]:
            if col not in existing_tide_cols:
                conn.execute(f"ALTER TABLE market_tide_ticks ADD COLUMN {col} {col_def}")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_tide_ticks_int_ts ON market_tide_ticks(interval, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_tide_ticks_date_int ON market_tide_ticks(date, interval)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS market_tide_state (
          key TEXT PRIMARY KEY,
          last_ts TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS market_regime_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          regime_label TEXT NOT NULL,
          confidence REAL NOT NULL,
          tide_index REAL NOT NULL,
          bias REAL NOT NULL,
          slope REAL NOT NULL,
          acceleration REAL NOT NULL,
          raw_json TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_regime_snapshots_ts ON market_regime_snapshots(ts)")

        for table in ["alerts", "alerts_live", "sim_alerts", "health_snapshots"]:
            existing_cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if "source" not in existing_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN source TEXT")

        existing_sim_cols = {r["name"] for r in conn.execute("PRAGMA table_info(sim_state)").fetchall()}
        if "dataset_hash" not in existing_sim_cols:
            conn.execute("ALTER TABLE sim_state ADD COLUMN dataset_hash TEXT")

        # Add non-destructive column migrations for timestamps
        for table in ["alerts", "alerts_live", "sim_alerts"]:
            existing_cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if "ingested_at" not in existing_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN ingested_at TEXT")
            if "trade_time_raw" not in existing_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN trade_time_raw TEXT")
            if "trade_tz" not in existing_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN trade_tz TEXT")
            if "trade_id" not in existing_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN trade_id TEXT")

        # Migrate raw_sim_alerts specifically if it exists
        existing_raw_cols = {r["name"] for r in conn.execute("PRAGMA table_info(raw_sim_alerts)").fetchall()}
        if "trade_id" not in existing_raw_cols and "id" in existing_raw_cols:
            conn.execute("ALTER TABLE raw_sim_alerts ADD COLUMN trade_id TEXT")