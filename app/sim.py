"""
Simulation Worker for Alpha Sentinel
Replays historical alerts-flow logically into the live feed dashboard.
Applies the same smart filter pipeline as the ingestion agent so only
high-conviction rows reach alerts_live.
"""
import json
import os
import time
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from app.db import db
from app.filters import filter_tick, FILTERS

# Setup simple logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [SIM] %(message)s")
logger = logging.getLogger(__name__)

FILTER_STATS_PATH = os.environ.get("FILTER_STATS_PATH_SIM", "/data/filter_stats_sim.json")


def _write_filter_stats(stats: dict) -> None:
    """Persist per-tick filter counters for the API and dashboard."""
    try:
        tmp = FILTER_STATS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(stats, f)
        os.replace(tmp, FILTER_STATS_PATH)
    except Exception as e:
        logger.warning(f"filter_stats write failed: {e}")


def _write_health(events_per_min: int, alerts_per_min: int) -> None:
    """Write simulation health snapshot to the DB."""
    try:
        with db() as conn:
            conn.execute(
                """
                INSERT INTO health_snapshots
                (ts, agent_status, ws_status, last_event_ts, last_alert_ts, events_per_min, alerts_per_min, errors_15m, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    "ok",
                    "sim",
                    None,
                    None,
                    events_per_min,
                    alerts_per_min,
                    0,
                    "sim"
                )
            )
    except Exception as e:
        logger.warning(f"Failed to write sim health: {e}")


def _row_to_ns(d: dict) -> SimpleNamespace:
    """
    Convert a sqlite Row dict to a SimpleNamespace so filter functions
    (which use attribute access) work transparently.
    """
    return SimpleNamespace(
        ticker      = d.get("ticker", "") or "",
        exp         = d.get("exp", "") or "",
        strike      = d.get("strike", 0.0) or 0.0,
        opt_type    = d.get("opt_type", "") or "",
        premium     = float(d.get("premium", 0) or 0),
        size        = int(d.get("size", 0) or 0),
        volume      = int(d.get("volume", 0) or 0),
        oi          = int(d.get("oi", 0) or 0),
        bid         = float(d.get("bid", 0) or 0),
        ask         = float(d.get("ask", 0) or 0),
        spread_pct  = float(d.get("spread_pct", 0) or 0),
        spot        = float(d.get("spot", 0) or 0),
        otm_pct     = float(d.get("otm_pct", 0) or 0),
        dte         = int(d.get("dte", 0) or 0),
        score_total = float(d.get("score_total", 0) or 0),
        source      = d.get("source", "SIM") or "SIM",
        tags        = d.get("tags", "") or "",
    )


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

        if not state_row:
            logger.warning("sim_state table not initialized. Missing ID 1.")
            return 5.0

        state = dict(state_row)

        if state["is_running"] == 0:
            return 1.0  # Stopped
        if state["is_paused"] == 1:
            return 1.0  # Paused

        cursor_id = int(state["cursor_id"])
        speed     = int(state["speed_per_tick"])
        interval  = float(state["interval_sec"])

        # Fetch a chunk of historical alerts for replay
        alerts_chunk = conn.execute(
            "SELECT * FROM raw_sim_alerts WHERE id >= ? ORDER BY id ASC LIMIT ?",
            (cursor_id, speed)
        ).fetchall()

        # Loop back when we reach the end
        if not alerts_chunk:
            logger.info("Simulation reached stream end. Looping to beginning.")
            conn.execute("UPDATE sim_state SET cursor_id=1 WHERE id=1")
            return 0.1

        # ── Apply smart filter pipeline ──────────────────────────────────
        raw_ns = [_row_to_ns(dict(row)) for row in alerts_chunk]
        candidates, fstats = filter_tick(raw_ns)

        # Top-N limiter
        max_n = FILTERS["MAX_INSERT_PER_TICK"]
        to_insert_ns = candidates[:max_n]

        # Map back to original dicts keyed by position — we need the raw
        # dict data for the INSERT (contract_key etc.)
        ns_set = set(id(ns) for ns in to_insert_ns)
        raw_dicts = [dict(row) for row in alerts_chunk]

        # Build a lookup: row index → namespace (same order as raw_ns)
        kept_dicts = []
        for idx, ns in enumerate(raw_ns):
            if id(ns) in ns_set:
                kept_dicts.append(raw_dicts[idx])

        # Write filter stats
        fstats["inserted"] = len(kept_dicts)
        fstats["efficiency_pct"] = (
            round(len(kept_dicts) / fstats["parsed"] * 100, 1)
            if fstats["parsed"] > 0 else 0.0
        )
        _write_filter_stats(fstats)
        _write_health(events_per_min=fstats["parsed"], alerts_per_min=fstats["inserted"])

        logger.info(
            f"[FILTER_SIM] parsed={fstats['parsed']} "
            f"s0_drop={fstats['dropped_stage0']} "
            f"s1_drop={fstats['dropped_stage1']} "
            f"s2_drop={fstats['dropped_stage2']} "
            f"s3_drop={fstats['pre_insert'] - fstats['inserted']} "
            f"inserted={fstats['inserted']}"
        )
        # ────────────────────────────────────────────────────────────────

        inserted_count = 0
        last_id = cursor_id
        now_ts = datetime.now(timezone.utc).isoformat()

        for d in kept_dicts:
            last_id = int(d["id"])

            ticker = d.get("ticker", "")
            trade_time_raw = d.get("trade_time_raw", "")
            trade_ts = d.get("ts", now_ts)
            
            ck = d.get("contract_key")
            if not ck:
                ck = normalize_contract_key(
                    ticker, d.get("exp", ""),
                    d.get("strike", 0.0), d.get("opt_type", "")
                )

            # Generate a unique trade_id for every alert to prevent any grouping
            import hashlib
            # Include contract details and price/size to ensure uniqueness even in the same second
            raw_id_str = f"{ticker}_{trade_time_raw}_{trade_ts}_{ck}_{d.get('size')}_{d.get('premium')}"
            trade_id = hashlib.md5(raw_id_str.encode()).hexdigest()

            ingested_at = now_ts
            trade_tz = d.get("trade_tz", "UTC")

            conn.execute(
                """
                INSERT INTO sim_alerts
                (ts, ticker, exp, strike, opt_type, premium, size, volume, oi,
                 bid, ask, spread_pct, spot, otm_pct, dte, score_total,
                 tags, reason_codes, source, contract_key, ingested_at, trade_time_raw, trade_tz, trade_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_ts,
                    ticker,
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
                    ck,
                    ingested_at,
                    trade_time_raw,
                    trade_tz,
                    trade_id
                )
            )
            inserted_count += 1

        # Advance cursor past ALL rows we fetched (not just those inserted)
        # so we don't replay filtered-out rows on the next tick
        last_fetched_id = int(dict(alerts_chunk[-1])["id"])
        next_cursor = last_fetched_id + 1
        conn.execute(
            "UPDATE sim_state SET cursor_id=?, last_tick_ts=? WHERE id=1",
            (next_cursor, now_ts)
        )

        logger.info(f"Stepped cursor to {next_cursor}, replayed {inserted_count}/{len(alerts_chunk)} rows.")
        return interval


def main():
    logger.info("Starting Simulation Replay Worker (with smart filter pipeline)...")
    while True:
        try:
            sleep_time = iter_sim()
            time.sleep(sleep_time)
        except Exception as e:
            logger.exception(f"Exception inside sim worker loop: {e}")
            time.sleep(5.0)


if __name__ == "__main__":
    main()
