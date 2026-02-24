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

  # Unusual Whales (feature-flagged, default OFF)
  UW_ENABLED=0                                   (set to 1 to enable)
  UW_API_KEY=                                    (required when UW_ENABLED=1)
  UW_BASE_URL=https://api.unusualwhales.com      (optional)
  UW_MODE=poll                                   (optional: poll|stream)
  UW_POLL_SEC=2                                  (optional)
  UW_RATE_LIMIT_SLEEP=0.25                       (optional)
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
import sqlite3
from app.db import db, init_db
from app.filters import filter_tick, FILTERS

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
        score += 45.0 + _clamp(math.log10(max(premium / 1_000_000, 1.0)) * 15.0, 0, 15.0)
    elif premium >= 100_000:
        score += 25.0 + _clamp(math.log10(max(premium / 100_000, 1.0)) * 15.0, 0, 20.0)
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
    ingested_at: str
    trade_time_raw: str
    trade_tz: str
    source: str = "CSV"  # CSV_ETF | CSV_STOCK | CSV | MOCK


def _source_tag(path: str) -> str:
    """
    Derive source tag from file path.
      etfs.csv   -> CSV_ETF
      stocks.csv -> CSV_STOCK
      else       -> CSV
    """
    name = os.path.basename(path).lower()
    if "etf" in name:
        return "CSV_ETF"
    if "stock" in name:
        return "CSV_STOCK"
    return "CSV"


def _row_from_csv(rec: Dict[str, Any], source: str = "CSV") -> Optional[AlertRow]:
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

    spot = _safe_float(_pick(rec, ["spot", "Price~", "Price", "underlying_price", "last"], 0.0), 0.0)

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

    # Extract Trade Time
    time_raw = str(_pick(rec, ["time", "Time"], "")).strip()
    if not time_raw:
        print(f"DEBUG: time_raw is empty. rec keys: {list(rec.keys())}")
        
    date_raw = str(_pick(rec, ["date", "Date"], "")).strip()
    
    # Simple ET parsing. CSV times are usually Eastern Time (NY).
    # NY is UTC-5 (or UTC-4 in daylight). We'll assume a fixed offset of 5 hours for simplicity
    trade_dt_utc = now_iso()
    if time_raw:
        try:
            # Strip trailing timezone info from string like "09:56:50 ET"
            clean_time_raw = time_raw.replace(" ET", "").replace(" EST", "").replace(" EDT", "").strip()

            # Fallback to today if date missing
            if not date_raw:
                date_raw = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            # Extract just the date part
            date_part = date_raw.split("T")[0]
            
            # Ensure time has seconds
            if len(clean_time_raw.split(":")) == 2:
                clean_time_raw += ":00"

            # Parse naive datetime: "2024-05-10 09:35:00"
            dt_str = f"{date_part} {clean_time_raw}"
            parsed_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            
            # Eastern Time is UTC-5; to convert TO UTC, we add 5 hours.
            parsed_dt = parsed_dt + timedelta(hours=5)
            
            # Attach UTC timezone and format to ISO
            trade_dt_utc = parsed_dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception as err:
            print(f"Time parse error on {time_raw} / {date_raw}: {err}")
            trade_dt_utc = now_iso()

    ck = _normalize_contract_key(_contract_key(ticker, exp, strike, opt_type))
    return AlertRow(
        ts=trade_dt_utc,
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
        tags=f"{source}_{code_str}" if code_str else source,
        reason_codes=json.dumps(["CSV_IMPORT", code_str]) if code_str else json.dumps(["CSV_IMPORT"]),
        contract_key=ck,
        ingested_at=now_iso(),
        trade_time_raw=time_raw,
        trade_tz="America/New_York",
        source=source,
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
        ingested_at=now_iso(),
        trade_time_raw=now_iso().split("T")[1][:8],
        trade_tz="UTC",
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
        # options_csvs, options_csv, interval_sec, max_alerts_per_tick, replay_from_start, watchlist_refresh_sec

        # Multi-file: OPTIONS_CSVS takes priority; fall back to OPTIONS_CSV for backward compat
        csvs_raw = str(_cfg("options_csvs", "OPTIONS_CSVS", "")).strip()
        if csvs_raw:
            self.csv_paths = [p.strip() for p in csvs_raw.split(",") if p.strip() and not os.path.basename(p.strip()).startswith("._")]
        else:
            single = str(_cfg("options_csv", "OPTIONS_CSV", "")).strip()
            self.csv_paths = [single] if single and not os.path.basename(single).startswith("._") else []

        # Keep backward-compat attribute for providers / old code that checks self.csv_path
        self.csv_path = self.csv_paths[0] if self.csv_paths else None

        self.interval = float(_cfg("interval_sec", "AGENT_INTERVAL_SEC", "2.5"))
        self.max_alerts_per_tick = int(_cfg("max_alerts_per_tick", "MAX_ALERTS_PER_TICK", "25"))

        replay_v = str(_cfg("replay_from_start", "REPLAY_FROM_START", "0")).strip().lower()
        replay_from_start = replay_v in ("1", "true", "yes", "y", "on")

        # Optional: log loaded config once (helpful to confirm it works)
        try:
            self._log(f"loaded_config: {cfg}")
        except Exception:
            print(f"[agent] loaded_config: {cfg}")

        # Initialise per-file state dict (backward compat: keep top-level csv_offset/csv_header too)
        if "csv_files" not in self.state:
            self.state["csv_files"] = {}

        # Reset all per-file offsets if REPLAY_FROM_START=1
        if replay_from_start:
            self.state["csv_files"] = {}
            self.state["csv_offset"] = 0
            self.state["csv_header"] = None
            _save_state(self.state_path, self.state)
            self._log("REPLAY_FROM_START=1: reset all CSV offsets")

        # Provider selection (UW > CSV > Mock)
        # Import here to avoid circular imports at module load time
        try:
            from app.providers import select_provider
            self.provider = select_provider(self)
        except Exception as exc:
            self._log(f"provider init failed ({exc}), defaulting to CSVProvider/MockProvider")
            from app.providers import CSVProvider, MockProvider
            self.provider = CSVProvider(self) if self.csv_path else MockProvider(self)

    def _log(self, msg: str) -> None:
        print(f"[AGENT] {msg}", flush=True)

    def _read_csv_file(self, path: str, file_state: Dict[str, Any], source: str = "CSV") -> List[AlertRow]:
        """
        Byte-offset tailer for a single CSV file.
        file_state is state['csv_files'][path] — a dict with 'offset' and 'header'.
        Advances file_state['offset'] in place and saves state.
        """
        if not os.path.exists(path):
            self._log(f"CSV not found: {path}")
            return []

        file_size = os.path.getsize(path)
        offset = int(file_state.get("offset", 0) or 0)
        header = file_state.get("header")

        if offset >= file_size:
            return []  # EOF

        rows: List[AlertRow] = []

        try:
            with open(path, "rb") as f:
                if offset == 0 or not header:
                    header_line = f.readline()
                    offset = f.tell()
                    try:
                        header_text = header_line.decode("utf-8", errors="ignore").strip("\r\n")
                        header = next(csv.reader([header_text]), None)
                    except Exception:
                        header = None

                    if not header:
                        self._log(f"CSV header missing/unreadable: {path}")
                        return []

                    file_state["header"] = header
                    file_state["offset"] = offset
                    _save_state(self.state_path, self.state)

                f.seek(offset)
                consumed_offset = offset

                while len(rows) < self.max_alerts_per_tick:
                    line = f.readline()
                    if not line:
                        break

                    consumed_offset = f.tell()
                    text = line.decode("utf-8", errors="ignore").strip("\r\n")
                    if not text:
                        continue

                    try:
                        values = next(csv.reader([text]))
                        if header and len(values) < len(header):
                            continue
                        rec = dict(zip(header, values)) if header else {}
                        ar = _row_from_csv(rec, source=source)
                        if ar:
                            rows.append(ar)
                    except Exception:
                        continue

                file_state["offset"] = consumed_offset
                _save_state(self.state_path, self.state)

            return rows
        except Exception as e:
            self._log(f"CSV read error ({path}): {e}")
            return []

    def _read_all_csvs(self) -> List[AlertRow]:
        """
        Iterates all configured CSV paths, reads up to max_alerts_per_tick total rows
        across all files, tagged by _source_tag.
        Per-file state is stored under state['csv_files'][path].
        """
        all_rows: List[AlertRow] = []
        remaining = self.max_alerts_per_tick

        for path in self.csv_paths:
            if remaining <= 0:
                break
            source = _source_tag(path)
            if path not in self.state["csv_files"]:
                self.state["csv_files"][path] = {"offset": 0, "header": None}
            file_state = self.state["csv_files"][path]

            rows = self._read_csv_file(path, file_state, source=source)
            all_rows.extend(rows[:remaining])
            remaining -= len(rows)

        return all_rows

    # Keep _read_csv_new for backward compat (CSVProvider can still call it for single-file mode)
    def _read_csv_new(self) -> List[AlertRow]:
        if not self.csv_path:
            return []
        return self._read_all_csvs()

    def _insert_alerts(self, alerts: List[AlertRow]) -> int:
        if not alerts:
            return 0

        inserted = 0
        with db() as conn:
            has_ck = _alerts_has_contract_key(conn)
            col_names = {r["name"] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
            has_source = "source" in col_names
            has_ingest = "ingested_at" in col_names

            for a in alerts:
                try:
                    target_table = "raw_sim_alerts" if str(a.source).upper().startswith("CSV") else "alerts"
                    ck = _normalize_contract_key(a.contract_key)
                    
                    # Generate a stable trade_id for multi-leg grouping
                    import hashlib
                    raw_id_str = f"{a.ticker}_{a.trade_time_raw}_{a.ts}"
                    trade_id = hashlib.md5(raw_id_str.encode()).hexdigest()

                    if has_ck and has_source and has_ingest:
                        conn.execute(
                            f"""
                            INSERT INTO {target_table}
                            (ts,contract_key,ticker,exp,strike,opt_type,premium,size,volume,oi,bid,ask,spread_pct,spot,otm_pct,dte,score_total,tags,reason_codes,source,ingested_at,trade_time_raw,trade_tz,trade_id)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                a.ts, ck, a.ticker, a.exp, a.strike, a.opt_type,
                                a.premium, a.size, a.volume, a.oi, a.bid, a.ask, a.spread_pct,
                                a.spot, a.otm_pct, a.dte, a.score_total, a.tags, a.reason_codes,
                                a.source, a.ingested_at, a.trade_time_raw, a.trade_tz, trade_id
                            ),
                        )
                    elif has_ck and has_source:
                        conn.execute(
                            f"""
                            INSERT INTO {target_table}
                            (ts,contract_key,ticker,exp,strike,opt_type,premium,size,volume,oi,bid,ask,spread_pct,spot,otm_pct,dte,score_total,tags,reason_codes,source)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                a.ts, ck, a.ticker, a.exp, a.strike, a.opt_type,
                                a.premium, a.size, a.volume, a.oi, a.bid, a.ask, a.spread_pct,
                                a.spot, a.otm_pct, a.dte, a.score_total, a.tags, a.reason_codes,
                                a.source,
                            ),
                        )
                    elif has_ck:
                        conn.execute(
                            f"""
                            INSERT INTO {target_table}
                            (ts,contract_key,ticker,exp,strike,opt_type,premium,size,volume,oi,bid,ask,spread_pct,spot,otm_pct,dte,score_total,tags,reason_codes)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                a.ts, ck, a.ticker, a.exp, a.strike, a.opt_type,
                                a.premium, a.size, a.volume, a.oi, a.bid, a.ask, a.spread_pct,
                                a.spot, a.otm_pct, a.dte, a.score_total, a.tags, a.reason_codes
                            ),
                        )
                    else:
                        conn.execute(
                            f"""
                            INSERT INTO {target_table}
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
                except sqlite3.Error as e:
                    self._log(f"Insert error: {e}")

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
                (ts,agent_status,ws_status,last_event_ts,last_alert_ts,events_per_min,alerts_per_min,errors_15m,source)
                VALUES (?,?,?,?,?,?,?,?,?)
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
                    "live"
                ),
            )

    def _write_filter_stats(self, stats: dict) -> None:
        """Persist per-tick filter counters to /data/filter_stats_live.json for the API."""
        import json as _json
        path = os.environ.get("FILTER_STATS_PATH_LIVE", "/data/filter_stats_live.json")
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump(stats, f)
            os.replace(tmp, path)
        except Exception as e:
            self._log(f"filter_stats write failed: {e}")

    def tick(self) -> None:
        # Fetch new alerts from the active provider (CSV / Mock / UnusualWhales)
        try:
            raw = self.provider.fetch()
        except Exception as exc:
            self._log(f"provider.fetch() error ({exc}), skipping tick alerts")
            raw = []

        # ── Smart filter pipeline (Stage 0 → 1 → 2 → Top-N) ────────────────
        candidates, fstats = filter_tick(raw)
        max_n = FILTERS["MAX_INSERT_PER_TICK"]
        to_insert = candidates[:max_n]
        fstats["inserted"] = len(to_insert)
        efficiency = (
            round(len(to_insert) / fstats["parsed"] * 100, 1)
            if fstats["parsed"] > 0 else 0.0
        )
        fstats["efficiency_pct"] = efficiency

        self._log(
            f"[FILTER] parsed={fstats['parsed']} "
            f"stage0_drop={fstats['dropped_stage0']} "
            f"stage1_drop={fstats['dropped_stage1']} "
            f"stage2_drop={fstats['dropped_stage2']} "
            f"stage3_drop={fstats['pre_insert'] - fstats['inserted']} "
            f"inserted={fstats['inserted']}"
        )
        self._write_filter_stats(fstats)
        # ─────────────────────────────────────────────────────────────────────

        inserted = self._insert_alerts(to_insert)
        mon_updates = self._upsert_monitor_for_watchlist()

        last_alert_ts = to_insert[-1].ts if to_insert else None

        # Write health snapshot every tick
        self._write_health(
            agent_status="ok",
            last_event_ts=last_alert_ts,
            last_alert_ts=last_alert_ts,
            events_per_min=int(fstats["parsed"]),
            alerts_per_min=int(inserted),
            errors_15m=0,
        )

        # Verbose log only when we actually inserted
        if inserted and to_insert:
            a0 = to_insert[0]
            self._log(
                f"inserted={inserted} monitor_updated={mon_updates} "
                f"sample={a0.ticker} {a0.opt_type} {a0.strike} score={a0.score_total}"
            )

        # Heartbeat log (ALWAYS prints, even at EOF)
        self._log(
            f"tick: parsed={fstats['parsed']} filtered_to={len(to_insert)} inserted={inserted} "
            f"monitor_updated={mon_updates} efficiency={efficiency}%"
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
