"""
Simulation Worker for Alpha Sentinel
Replays historical alerts-flow logically into the live feed dashboard.
"""
import time
import logging
import sqlite3
from datetime import datetime, timezone

from app.db import db

# Setup simple logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [SIM] %(message)s")
logger = logging.getLogger(__name__)

def normalize_contract_key(ticker: str, exp: str, strike: float, opt_type: str) -> str:
    """Normalize options details per standard API function logic."""
    t = (ticker or "").strip().upper()
    e = (exp or "").strip().split("T")[0].split(" ")[0]
    ot = (opt_type or "").strip().upper()
    ot = "C" if ot.startswith("C") else "P" if ot.startswith("P") else ot
    try:
        s = float(strike)
    except Exception:
        s = 0.0
    return f"{t}|{e}|{s}|{ot}"

def iter_sim():
    """Single execution step of the sim worker loop."""
    with db() as conn:
        state_row = conn.execute("SELECT * FROM sim_state WHERE id=1").fetchone()
        
        # Protect against non-init
        if not state_row:
            logger.warning("sim_state table not initialized. Missing ID 1.")
            return 5.0
            
        state = dict(state_row)
        
        # Global states
        if state["is_running"] == 0:
            return 1.0  # Stopped, fast poll
        if state["is_paused"] == 1:
            return 1.0  # Paused, fast poll
            
        cursor_id = int(state["cursor_id"])
        speed = int(state["speed_per_tick"])
        interval = float(state["interval_sec"])
        
        # Fetch chunk of historical alerts for replay
        alerts_chunk = conn.execute(
            """
            SELECT * FROM alerts 
            WHERE id >= ? 
            ORDER BY id ASC 
            LIMIT ?
            """,
            (cursor_id, speed)
        ).fetchall()
        
        # Reached the end: loop back 
        if not alerts_chunk:
            logger.info("Simulation reached stream end. Looping to beginning.")
            conn.execute("UPDATE sim_state SET cursor_id=1 WHERE id=1")
            return 0.1
            
        inserted_count = 0
        last_id = cursor_id
        now_ts = datetime.now(timezone.utc).isoformat()
        
        for row in alerts_chunk:
            d = dict(row)
            last_id = int(d["id"])
            
            # Normalize missing ContractKeys in history
            ck = d.get("contract_key")
            if not ck:
                ck = normalize_contract_key(d.get("ticker", ""), d.get("exp", ""), d.get("strike", 0.0), d.get("opt_type", ""))
            
            # Reinsert into active display pipe
            conn.execute(
                """
                INSERT INTO alerts_live
                (ts, ticker, exp, strike, opt_type, premium, size, volume, oi, bid, ask, spread_pct, spot, otm_pct, dte, score_total, tags, reason_codes, source, contract_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ts,  # Replace ancient timestamp with live feed 
                    d.get("ticker"),
                    d.get("exp"),
                    d.get("strike"),
                    d.get("opt_type"),
                    d.get("premium"),
                    d.get("size"),
                    d.get("volume"),
                    d.get("oi"),
                    d.get("bid"),
                    d.get("ask"),
                    d.get("spread_pct"),
                    d.get("spot"),
                    d.get("otm_pct"),
                    d.get("dte"),
                    d.get("score_total"),
                    d.get("tags"),
                    d.get("reason_codes"),
                    "sim",
                    ck
                )
            )
            inserted_count += 1
            
        # Update sim cursor and bounds
        next_cursor = last_id + 1
        conn.execute("UPDATE sim_state SET cursor_id=?, last_tick_ts=? WHERE id=1", (next_cursor, now_ts))
        
        logger.info(f"Stepped cursor to {next_cursor}, replayed {inserted_count} ticks.")
        return interval

def main():
    logger.info("Starting Simulation Replay Worker...")
    while True:
        try:
            sleep_time = iter_sim()
            time.sleep(sleep_time)
        except Exception as e:
            logger.exception(f"Exception inside sim worker loop: {e}")
            time.sleep(5.0)

if __name__ == "__main__":
    main()
