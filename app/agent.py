# /opt/alpha-sentinel/app/agent.py
"""
Alpha Sentinel Agent
- Tails an options-flow CSV (or runs in MOCK mode if none configured)
- Inserts rows into `alerts`
- Maintains `monitor` scores for contracts in `watchlist` (is_active=1)
- Writes `health_snapshots`

Env:
  DB_PATH=/data/sentinel.db
  LOG_PATH=/logs/agent.log
  OPTIONS_CSV=/data/options-flow-02-15-2026.csv   (optional)
  AGENT_INTERVAL_SEC=1.5                         (optional)
  MAX_ALERTS_PER_TICK=25                         (optional)
  STATE_PATH=/data/agent_state.json              (optional)
"""

from __future__ import annotations

import csv
import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from pathlib import Path
from app.db import db, init_db

def _load_yaml_agent_config() -> dict:
    """
    Loads /app/config/agent.yml if present.
    Returns {} if missing/invalid so the agent always keeps running.
    """
    cfg_path = Path("/app/config/agent.yml")
    if not cfg_path.exists():
        return {}

    try:
        import yaml  # requires PyYAML installed in container
        data = yaml.safe_load(cfg_path.read_text()) or {}
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[agent] config load failed: {e}")
        return {}

# ---------- helpers ----------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, int):
            return x
        s = str(x).strip().replace(",", "")
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _norm_opt_type(x: Any) -> str:
    s = (str(x) if x is not None else "").strip().upper()
    if s in ("C", "CALL", "CALLS"):
        return "C"
    if s in ("P", "PUT", "PUTS"):
        return "P"
    if s.startswith("C"):
        return "C"
    if s.startswith("P"):
        return "P"
    return "C"


def _pick(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _contract_key(ticker: str, exp: str, strike: float, opt_type: str) -> str:
    return f"{ticker}|{exp}|{float(strike)}|{opt_type}"

def _normalize_contract_key(ck: str) -> str:
    if not ck:
        return ""

    parts = [p.strip() for p in str(ck).split("|")]
    if len(parts) != 4:
        return str(ck).strip()

    t, e, s, ot = parts
    t = t.upper()
    e = e.split("T")[0].split(" ")[0]
    ot = ot.upper()
    ot = "C" if ot.startswith("C") else "P" if ot.startswith("P") else ot

    try:
        s_norm = str(float(s))
    except Exception:
        s_norm = s.strip()

    return f"{t}|{e}|{s_norm}|{ot}"



def _dte_from_exp(exp: str) -> int:
    try:
        dt = datetime.strptime(exp, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dte = int((dt - datetime.now(timezone.utc)).total_seconds() // 86400)
        return max(dte, 0)
    except Exception:
        return 0


def _score_row(premium: float, volume: int, oi: int, spread_pct: float, otm_pct: float, dte: int, code: str) -> float:
    """
    Advanced Conviction Scoring (0-100+):
    - Massive premium gets exponential bonus
    - Vol > OI indicates new position openings
    - Short DTE with large premium indicates urgency
    - Sweeps & Blocks add to conviction
    """
    score = 0.0
    
    # 1. Premium Weighting (Exponential for whales)
    if premium >= 1_000_000:
        score += 45.0 + _clamp(math.log10(premium / 1_000_000) * 15.0, 0, 15.0)
    elif premium >= 100_000:
        score += 25.0 + _clamp(math.log10(premium / 100_000) * 15.0, 0, 20.0)
    else:
        score += _clamp(math.log10(max(premium, 1.0)) * 5.0, 0, 25.0)
        
    # 2. Volume vs Open Interest (Conviction)
    # If volume is higher than OI, it means new contracts are being opened aggressively
    if oi > 0:
        vol_oi_ratio = volume / oi
        if vol_oi_ratio > 2.0:
            score += 20.0
        elif vol_oi_ratio > 1.0:
            score += 10.0
        else:
            score += _clamp(vol_oi_ratio * 10.0, 0, 10.0)
    elif volume > 500: # No OI but high volume (new strike/expiry)
        score += 15.0
        
    # 3. Execution Type (Sweeps & Blocks)
    code_upper = code.upper()
    is_sweep = "SWEEP" in code_upper
    is_block = "BLOCK" in code_upper
    if is_sweep:
        score += 15.0
    elif is_block:
        score += 10.0
        
    # 4. Urgency (DTE) combined with conviction
    # High premium on short DTE is very urgent
    if dte <= 14 and premium >= 100_000:
        score += 10.0
    elif dte > 60:
        score -= 5.0 # LEAPS are less urgent
        
    # 5. Penalties (Sloppy execution / lottery tickets)
    spread_penalty = _clamp(spread_pct * 1.5, 0, 15.0)
    otm_penalty = _clamp(max(0.0, otm_pct) * 0.2, 0, 10.0)
    
    score = score - spread_penalty - otm_penalty
    
    return round(_clamp(score, 0, 100.0), 1)


def _load_state(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            st = json.load(f)
        if not isinstance(st, dict):
            return {"csv_offset": 0, "csv_header": None}
        st.setdefault("csv_offset", 0)
        st.setdefault("csv_header", None)
        return st
    except Exception:
        return {"csv_offset": 0, "csv_header": None}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def _alerts_has_contract_key(conn) -> bool:
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()]
    return "contract_key" in cols


def _normalize_exp(expires_val: str) -> str:
    """
    CSV shows: 2026-02-13T16:30:00-06:00
    We store:  2026-02-13
    """
    if not expires_val:
        return ""
    s = str(expires_val).strip()
    s = s.split(" ")[0]
    s = s.split("T")[0]
    return s


@dataclass
class AlertRow:
    ts: str
    ticker: str
    exp: str
    strike: float
    opt_type: str
    premium: float
    size: int
    volume: int
    oi: int
    bid: float
    ask: float
    spread_pct: float
    spot: float
    otm_pct: float
    dte: int
    score_total: float
    tags: str
    reason_codes: str
    contract_key: str


def _row_from_csv(rec: Dict[str, Any]) -> Optional[AlertRow]:
    """
    Supports your CSV header:
      Symbol,Price~,Type,Strike,Expires,DTE,"Bid x Size","Ask x Size",Trade,Size,Side,Premium,Volume,"Open Int",IV,Delta,Code,*,Time
    Plus other common variants.
    """
    ticker = str(_pick(rec, ["ticker", "symbol", "Symbol", "Symbol "], "")).strip().upper()
    if not ticker:
        return None

    exp_raw = _pick(rec, ["exp", "expiration", "expiry", "exp_date", "Expires", "expires"], "")
    exp = _normalize_exp(str(exp_raw).strip())
    if not exp:
        return None

    strike = _safe_float(_pick(rec, ["strike", "Strike", "strike_price", "k"], None), default=float("nan"))
    if not math.isfinite(strike):
        return None

    opt_type = _norm_opt_type(_pick(rec, ["opt_type", "type", "Type", "right", "call_put"], "C"))

    premium = _safe_float(_pick(rec, ["premium", "Premium", "notional", "trade_value"], 0.0), 0.0)
    size = _safe_int(_pick(rec, ["size", "Size", "qty", "contracts"], 0), 0)
    volume = _safe_int(_pick(rec, ["volume", "Volume", "vol"], 0), 0)
    oi = _safe_int(_pick(rec, ["oi", "Open Int", "open_interest"], 0), 0)

    # Your CSV provides "Price~" as underlying/spot-ish. Use it as spot for now.
    spot = _safe_float(_pick(rec, ["spot", "Price~", "Price", "underlying_price", "last"], 0.0), 0.0)

    # Your CSV gives Bid x Size / Ask x Size strings; parse bid/ask as first token
    bid_x = str(_pick(rec, ["bid", "Bid x Size", "Bid"], "")).strip()
    ask_x = str(_pick(rec, ["ask", "Ask x Size", "Ask"], "")).strip()

    bid = 0.0
    ask = 0.0
    if bid_x:
        try:
            bid = _safe_float(bid_x.split("x")[0].strip(), 0.0)
        except Exception:
            bid = _safe_float(bid_x, 0.0)
    if ask_x:
        try:
            ask = _safe_float(ask_x.split("x")[0].strip(), 0.0)
        except Exception:
            ask = _safe_float(ask_x, 0.0)

    spread_pct = 0.0
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
        if mid > 0:
            spread_pct = abs(ask - bid) / mid * 100.0

    otm_pct = 0.0
    if spot > 0:
        if opt_type == "C":
            otm_pct = (strike - spot) / spot * 100.0
        else:
            otm_pct = (spot - strike) / spot * 100.0

    # If CSV includes DTE, use it; else compute from exp date
    dte = _safe_int(_pick(rec, ["DTE", "dte"], None), default=-1)
    if dte < 0:
        dte = _dte_from_exp(exp)
        
    code_str = str(_pick(rec, ["Code", "code"], "")).strip()

    score = _score_row(
        premium=premium,
        volume=volume,
        oi=oi,
        spread_pct=spread_pct,
        otm_pct=otm_pct,
        dte=dte,
        code=code_str,
    )

    ck = _contract_key(ticker, exp, strike, opt_type)
    return AlertRow(
        ts=now_iso(),
        ticker=ticker,
        exp=exp,
        strike=float(strike),
        opt_type=opt_type,
        premium=float(premium),
        size=int(size),
        volume=int(volume),
        oi=int(oi),
        bid=float(bid),
        ask=float(ask),
        spread_pct=round(float(spread_pct), 2),
        spot=float(spot),
        otm_pct=round(float(otm_pct), 2),
        dte=int(dte),
        score_total=float(score),
        tags=f"CSV_{code_str}" if code_str else "CSV",
        reason_codes=json.dumps(["CSV_IMPORT", code_str]) if code_str else json.dumps(["CSV_IMPORT"]),
        contract_key=ck,
    )


def _mock_alert() -> AlertRow:
    tickers = ["SPY", "QQQ", "IWM", "NVDA", "AAPL", "MSFT", "AMD", "TSLA", "META", "VIX"]
    ticker = random.choice(tickers)
    opt_type = random.choice(["C", "P"])
    exp_dt = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30))
    exp = exp_dt.strftime("%Y-%m-%d")

    spot = random.uniform(100, 600)
    strike = round(spot * random.uniform(0.85, 1.15), 1)
    bid = round(random.uniform(0.4, 12.0), 2)
    ask = round(bid + random.uniform(0.01, 0.8), 2)

    premium = float(random.randint(50, 700)) * 1000.0
    size = random.randint(10, 1200)
    volume = random.randint(50, 9000)
    oi = random.randint(100, 30000)

    mid = (bid + ask) / 2.0
    spread_pct = abs(ask - bid) / mid * 100.0 if mid > 0 else 0.0
    otm_pct = (strike - spot) / spot * 100.0 if opt_type == "C" else (spot - strike) / spot * 100.0
    dte = _dte_from_exp(exp)
    score = _score_row(premium, volume, oi, spread_pct, otm_pct, dte)
    ck = _contract_key(ticker, exp, strike, opt_type)

    return AlertRow(
        ts=now_iso(),
        ticker=ticker,
        exp=exp,
        strike=float(strike),
        opt_type=opt_type,
        premium=premium,
        size=size,
        volume=volume,
        oi=oi,
        bid=float(bid),
        ask=float(ask),
        spread_pct=round(float(spread_pct), 2),
        spot=float(spot),
        otm_pct=round(float(otm_pct), 2),
        dte=int(dte),
        score_total=float(score),
        tags="MOCK",
        reason_codes=json.dumps(["MOCK_DATA"]),
        contract_key=ck,
    )


# ---------- agent ----------

class SentinelAgent:
    def __init__(self) -> None:
        self.state_path = os.environ.get("STATE_PATH", "/data/agent_state.json")
        self.state = _load_state(self.state_path)
        self.db_path = os.environ.get("DB_PATH", "/data/sentinel.db")

        cfg = _load_yaml_agent_config()

        def _cfg(key: str, env_key: str, default):
            v = cfg.get(key, None)
            if v is None:
                v = os.environ.get(env_key, default)
            return v

        # YAML keys supported:
        # options_csv, interval_sec, max_alerts_per_tick, replay_from_start, watchlist_refresh_sec
        csv_raw = str(_cfg("options_csv", "OPTIONS_CSV", "")).strip()
        self.csv_path = csv_raw or None

        self.interval = float(_cfg("interval_sec", "AGENT_INTERVAL_SEC", "2.5"))
        self.max_alerts_per_tick = int(_cfg("max_alerts_per_tick", "MAX_ALERTS_PER_TICK", "25"))

        replay_v = str(_cfg("replay_from_start", "REPLAY_FROM_START", "0")).strip().lower()
        replay_from_start = replay_v in ("1", "true", "yes", "y", "on")

        # Optional: log loaded config once (helpful to confirm it works)
        try:
            self._log(f"loaded_config: {cfg}")
        except Exception:
            print(f"[agent] loaded_config: {cfg}")

        # one-time: force full replay from start (for static CSV imports)
        if replay_from_start:
            ...

    def _log(self, msg: str) -> None:
        print(f"[AGENT] {msg}", flush=True)

    def _read_csv_new(self) -> List[AlertRow]:
        """
        Byte-offset tailer that:
        - reads header once (offset==0), stores it in state
        - reads up to max_alerts_per_tick new lines each tick
        - advances offset ONLY by bytes actually consumed for processed lines
        """
        if not self.csv_path:
            return []

        if not os.path.exists(self.csv_path):
            self._log(f"OPTIONS_CSV not found: {self.csv_path}")
            return []

        file_size = os.path.getsize(self.csv_path)
        offset = int(self.state.get("csv_offset", 0) or 0)
        header = self.state.get("csv_header")

        if offset >= file_size:
            # at EOF (no new lines) — caller will still heartbeat
            return []

        rows: List[AlertRow] = []

        try:
            with open(self.csv_path, "rb") as f:
                # If first run, read header line and store it.
                if offset == 0 or not header:
                    header_line = f.readline()
                    offset = f.tell()
                    try:
                        header_text = header_line.decode("utf-8", errors="ignore").strip("\r\n")
                        header = next(csv.reader([header_text]), None)
                    except Exception:
                        header = None

                    if not header:
                        self._log("CSV header missing/unreadable")
                        return []

                    self.state["csv_header"] = header
                    self.state["csv_offset"] = offset
                    _save_state(self.state_path, self.state)

                # Seek to current offset, read line-by-line
                f.seek(offset)
                consumed_offset = offset

                while len(rows) < self.max_alerts_per_tick:
                    line = f.readline()
                    if not line:
                        break  # EOF

                    consumed_offset = f.tell()
                    text = line.decode("utf-8", errors="ignore").strip("\r\n")
                    if not text:
                        continue

                    try:
                        values = next(csv.reader([text]))
                        if header and len(values) < len(header):
                            # partial / malformed line; skip but still advance offset (avoid infinite loop)
                            continue
                        rec = dict(zip(header, values)) if header else {}
                        ar = _row_from_csv(rec)
                        if ar:
                            rows.append(ar)
                    except Exception:
                        continue

                # Save the offset where we stopped reading (NOT necessarily EOF)
                self.state["csv_offset"] = consumed_offset
                _save_state(self.state_path, self.state)

            return rows
        except Exception as e:
            self._log(f"CSV read error: {e}")
            return []

    def _insert_alerts(self, alerts: List[AlertRow]) -> int:
        if not alerts:
            return 0

        inserted = 0
        with db() as conn:
            has_ck = _alerts_has_contract_key(conn)

            for a in alerts:
                try:
                    if has_ck:
                        conn.execute(
                            """
                            INSERT INTO alerts
                            (ts,contract_key,ticker,exp,strike,opt_type,premium,size,volume,oi,bid,ask,spread_pct,spot,otm_pct,dte,score_total,tags,reason_codes)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                a.ts, a.contract_key, a.ticker, a.exp, a.strike, a.opt_type,
                                a.premium, a.size, a.volume, a.oi, a.bid, a.ask, a.spread_pct,
                                a.spot, a.otm_pct, a.dte, a.score_total, a.tags, a.reason_codes
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO alerts
                            (ts,ticker,exp,strike,opt_type,premium,size,volume,oi,bid,ask,spread_pct,spot,otm_pct,dte,score_total,tags,reason_codes)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                a.ts, a.ticker, a.exp, a.strike, a.opt_type,
                                a.premium, a.size, a.volume, a.oi, a.bid, a.ask, a.spread_pct,
                                a.spot, a.otm_pct, a.dte, a.score_total, a.tags, a.reason_codes
                            ),
                        )
                    inserted += 1
                except Exception as e:
                    self._log(f"insert alert failed: {e}")

        return inserted

    def _active_watchlist_keys(self) -> List[str]:
        with db() as conn:
            rows = conn.execute(
                "SELECT contract_key FROM watchlist WHERE is_active=1 ORDER BY created_at DESC"
            ).fetchall()
        return [_normalize_contract_key(r["contract_key"]) for r in rows]


    def _latest_alert_score_for_key(self, contract_key: str) -> Optional[float]:
        contract_key = _normalize_contract_key(contract_key)
        t, exp, strike_s, opt_type = contract_key.split("|")
        strike = _safe_float(strike_s, default=float("nan"))

        with db() as conn:
            has_ck = _alerts_has_contract_key(conn)
            if has_ck:
                row = conn.execute(
                    "SELECT score_total FROM alerts WHERE contract_key=? ORDER BY id DESC LIMIT 1",
                    (contract_key,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT score_total FROM alerts
                    WHERE ticker=? AND exp=? AND strike=? AND opt_type=?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (t, exp, float(strike), opt_type),
                ).fetchone()

        if not row:
            return None
        return float(row["score_total"])

    def _upsert_monitor_for_watchlist(self) -> int:
        keys = self._active_watchlist_keys()
        if not keys:
            return 0

        updated = 0
        with db() as conn:
            for ck in keys:
                try:
                    t, exp, strike_s, opt_type = ck.split("|")
                    strike = float(_safe_float(strike_s, default=0.0))

                    latest = self._latest_alert_score_for_key(ck)
                    if latest is None:
                        latest = 0.0

                    row = conn.execute(
                        "SELECT * FROM monitor WHERE contract_key=?",
                        (ck,),
                    ).fetchone()

                    if not row:
                        conn.execute(
                            """
                            INSERT INTO monitor
                            (contract_key,ticker,exp,strike,opt_type,entry_score,current_score,peak_score,score_history,status,last_update_ts)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                ck, t, exp, strike, opt_type,
                                float(latest), float(latest), float(latest),
                                json.dumps([float(latest)]),
                                "Monitor",
                                now_iso(),
                            ),
                        )
                        updated += 1
                        continue

                    try:
                        hist = json.loads(row["score_history"]) if row["score_history"] else []
                        if not isinstance(hist, list):
                            hist = []
                    except Exception:
                        hist = []

                    hist.append(float(latest))
                    hist = hist[-40:]

                    peak = max(float(row["peak_score"]), float(latest))
                    current = float(latest)

                    if current >= 80:
                        status = "Strong"
                    elif current >= 70:
                        status = "Monitor"
                    else:
                        status = "Weakening"

                    conn.execute(
                        """
                        UPDATE monitor
                        SET current_score=?, peak_score=?, score_history=?, status=?, last_update_ts=?
                        WHERE contract_key=?
                        """,
                        (current, peak, json.dumps(hist), status, now_iso(), ck),
                    )
                    updated += 1
                except Exception as e:
                    self._log(f"monitor update failed for {ck}: {e}")

        return updated

    def _write_health(
        self,
        agent_status: str,
        last_event_ts: Optional[str],
        last_alert_ts: Optional[str],
        events_per_min: int,
        alerts_per_min: int,
        errors_15m: int,
    ) -> None:
        with db() as conn:
            conn.execute(
                """
                INSERT INTO health_snapshots
                (ts,agent_status,ws_status,last_event_ts,last_alert_ts,events_per_min,alerts_per_min,errors_15m)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    now_iso(),
                    agent_status,
                    "n/a",
                    last_event_ts,
                    last_alert_ts,
                    int(events_per_min),
                    int(alerts_per_min),
                    int(errors_15m),
                ),
            )

    def tick(self) -> None:
        # Read new CSV lines (if configured)
        alerts = self._read_csv_new()

        # If no CSV configured -> generate some mock traffic
        if not self.csv_path:
            alerts = [_mock_alert() for _ in range(min(3, self.max_alerts_per_tick))]

        inserted = self._insert_alerts(alerts)
        mon_updates = self._upsert_monitor_for_watchlist()

        last_alert_ts = alerts[-1].ts if alerts else None

        # Write health snapshot every tick
        self._write_health(
            agent_status="ok",
            last_event_ts=last_alert_ts,
            last_alert_ts=last_alert_ts,
            events_per_min=int(inserted),
            alerts_per_min=int(inserted),
            errors_15m=0,
        )

        # Verbose log only when we actually inserted
        if inserted and alerts:
            a0 = alerts[0]
            self._log(
                f"inserted={inserted} monitor_updated={mon_updates} "
                f"sample={a0.ticker} {a0.opt_type} {a0.strike} score={a0.score_total}"
            )

        # Heartbeat log (ALWAYS prints, even at EOF)
        self._log(
            f"tick: parsed={len(alerts)} inserted={inserted} "
            f"monitor_updated={mon_updates} offset={self.state.get('csv_offset')}"
        )

    def run_forever(self) -> None:
        init_db()
        self._log(
            f"starting. DB_PATH={self.db_path} OPTIONS_CSV={self.csv_path or '(none)'} "
            f"interval={self.interval}s max_alerts_per_tick={self.max_alerts_per_tick}"
        )

        while True:
            try:
                self.tick()
            except Exception as e:
                self._log(f"tick error: {e}")
                try:
                    self._write_health(
                        agent_status="error",
                        last_event_ts=None,
                        last_alert_ts=None,
                        events_per_min=0,
                        alerts_per_min=0,
                        errors_15m=1,
                    )
                except Exception:
                    pass
            time.sleep(self.interval)


def main() -> None:
    SentinelAgent().run_forever()


if __name__ == "__main__":
    main()
