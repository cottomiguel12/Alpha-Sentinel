"""
app/filters.py — Smart Signal Filtering Pipeline
=================================================
4-stage pre-insertion filter for options flow contracts.

Stages:
  0  Schema sanity   – drop malformed / impossible rows
  1  Liquidity       – drop thin / wide-spread contracts
  2  Signal quality  – drop low-conviction orderflow
  3  Top-N limiter   – enforced in agent.py (sort + slice)

Usage (agent.py):
    from app.filters import FILTERS, _passes_stage0, _passes_stage1, _passes_stage2, filter_tick

All thresholds live in FILTERS; override per environment as needed.
"""

from __future__ import annotations
import math
import os
from typing import List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from app.agent import AlertRow

# ---------------------------------------------------------------------------
# Configuration — all thresholds in one place
# ---------------------------------------------------------------------------

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except Exception:
        return default

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except Exception:
        return default

def _env_bool(key: str, default: bool) -> bool:
    v = os.environ.get(key, "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


FILTERS: dict = {
    # ── Liquidity ────────────────────────────────────────────────────────────
    "MIN_PREMIUM_STOCK":           _env_float("FILTER_MIN_PREMIUM_STOCK",    7_500),
    "MIN_PREMIUM_ETF":             _env_float("FILTER_MIN_PREMIUM_ETF",      5_000),
    "MIN_SIZE_STOCK":              _env_int  ("FILTER_MIN_SIZE_STOCK",          150),
    "MIN_SIZE_ETF":                _env_int  ("FILTER_MIN_SIZE_ETF",            100),
    "MAX_SPREAD_PCT":              _env_float("FILTER_MAX_SPREAD_PCT",          18.0),
    "MAX_SPREAD_PCT_HIGH_PREMIUM": _env_float("FILTER_MAX_SPREAD_PCT_HIGH",     30.0),
    "HIGH_PREMIUM_OVERRIDE":       _env_float("FILTER_HIGH_PREMIUM_OVERRIDE", 50_000),

    # ── Signal ───────────────────────────────────────────────────────────────
    "VOL_OI_MIN":                  _env_float("FILTER_VOL_OI_MIN",              0.35),
    "MAX_DTE":                     _env_int  ("FILTER_MAX_DTE",                   45),
    "MAX_DTE_BIG":                 _env_int  ("FILTER_MAX_DTE_BIG",              120),
    "BIG_PREMIUM_FOR_LONG_DTE":    _env_float("FILTER_BIG_PREMIUM_LONG_DTE",  75_000),
    "REQUIRE_AGGRESSIVE_SIDE":     _env_bool ("FILTER_REQUIRE_AGGRESSIVE",       True),
    "ALLOW_MID_IF_PREMIUM_OVER":   _env_float("FILTER_ALLOW_MID_OVER",       100_000),

    # ── Tick limiter ─────────────────────────────────────────────────────────
    "MAX_INSERT_PER_TICK":         _env_int  ("FILTER_MAX_INSERT_PER_TICK",       15),
}


# ---------------------------------------------------------------------------
# ETF universe — used for liquidity-tier selection
# ---------------------------------------------------------------------------

ETF_TICKERS: frozenset = frozenset({
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "VEA", "VWO",
    # Sectors
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE",
    # Bonds / rates
    "TLT", "IEF", "SHY", "AGG", "LQD", "HYG", "JNK",
    # Volatility
    "VXX", "UVXY", "SVXY", "VIXY",
    # Leveraged
    "TQQQ", "SQQQ", "SPXL", "SPXS", "UPRO", "SPXU", "UDOW", "SDOW",
    "NUGT", "DUST", "LABU", "LABD",
    # Commodities / Gold / Oil
    "GLD", "SLV", "GDX", "GDXJ", "OIH", "USO", "UNG", "DBO",
    # International
    "EEM", "EFA", "FXI", "EWJ", "EWZ", "KWEB",
    # Misc high-volume ETFs
    "ARKK", "ARKG", "ARKW", "ARKF", "ARKQ",
    "SMH", "SOXX", "IBB", "XBI", "HACK",
    "HYG", "JETS", "MSOS",
})


def _is_etf(row: "AlertRow") -> bool:
    """True if the row should be treated as an ETF for liquidity thresholds."""
    if row.source in ("CSV_ETF",):
        return True
    return row.ticker in ETF_TICKERS


# ---------------------------------------------------------------------------
# Stage 0 — Schema Sanity
# ---------------------------------------------------------------------------

def _passes_stage0(row: "AlertRow") -> bool:
    """
    Drop rows that are structurally invalid or physically impossible.
    Runs before any financial logic.
    """
    f = FILTERS

    # Required string fields
    if not row.ticker:
        return False
    if not row.exp:
        return False

    # Strike must be a positive finite number
    try:
        strike = float(row.strike)
    except Exception:
        return False
    if not math.isfinite(strike) or strike <= 0:
        return False

    # Option type
    if row.opt_type not in ("C", "P"):
        return False

    # Premium must be positive
    if not (row.premium > 0):
        return False

    # Size (contracts) must be positive
    if not (row.size > 0):
        return False

    # Bid and ask must be positive and sensible
    if not (row.bid > 0):
        return False
    if not (row.ask > 0):
        return False
    if row.ask < row.bid:
        return False

    # Spread sanity cap — anything > 60% is a data error or untradeably wide
    if row.spread_pct > 60.0:
        return False

    return True


# ---------------------------------------------------------------------------
# Stage 1 — Liquidity
# ---------------------------------------------------------------------------

def _passes_stage1(row: "AlertRow") -> bool:
    """
    Filter out contracts that are too thin to act on:
    - Too small a premium (not institutional)
    - Too few contracts (order size too small)
    - Bid/ask spread too wide (execution risk)
    """
    f = FILTERS
    etf = _is_etf(row)

    # Premium floor
    min_prem = f["MIN_PREMIUM_ETF"] if etf else f["MIN_PREMIUM_STOCK"]
    if row.premium < min_prem:
        return False

    # Size floor
    min_size = f["MIN_SIZE_ETF"] if etf else f["MIN_SIZE_STOCK"]
    if row.size < min_size:
        return False

    # Spread cap — high-premium trades get a wider allowance
    if row.premium >= f["HIGH_PREMIUM_OVERRIDE"]:
        max_spread = f["MAX_SPREAD_PCT_HIGH_PREMIUM"]
    else:
        max_spread = f["MAX_SPREAD_PCT"]

    if row.spread_pct > max_spread:
        return False

    return True


# ---------------------------------------------------------------------------
# Stage 2 — Signal Quality
# ---------------------------------------------------------------------------

def _detect_aggression_side(row: "AlertRow") -> str:
    """
    Classify the trade as hitting ASK, BID, or MID.
    Requires bid and ask in the row; falls back to 'MID' if unavailable.
    """
    if row.bid <= 0 or row.ask <= 0:
        return "MID"

    # Use premium/size to infer trade price per contract (×100)
    # If premium is in dollars (not per-contract), convert
    trade_price = 0.0
    if row.size > 0 and row.premium > 0:
        trade_price = row.premium / (row.size * 100.0)  # per-share price

    if trade_price <= 0:
        return "MID"

    # Within 1 % of ask → aggressive buyer
    if trade_price >= row.ask * 0.99:
        return "ASK"
    # Within 1 % of bid → aggressive seller
    if trade_price <= row.bid * 1.01:
        return "BID"
    return "MID"


def _passes_stage2(row: "AlertRow") -> bool:
    """
    Filter out low-conviction orderflow:
      Must satisfy AT LEAST ONE of: high vol/OI, short DTE, big-premium long-dated, aggressive side
    + MID trades under the big-premium threshold are dropped when REQUIRE_AGGRESSIVE_SIDE=True
    """
    f = FILTERS

    vol_oi = row.volume / max(row.oi, 1)
    dte = row.dte if row.dte is not None else 9999
    side = _detect_aggression_side(row)

    # --- At least one conviction signal ---
    has_vol_oi   = vol_oi >= f["VOL_OI_MIN"]
    has_short_dte = dte <= f["MAX_DTE"]
    has_big_long  = row.premium >= f["BIG_PREMIUM_FOR_LONG_DTE"] and dte <= f["MAX_DTE_BIG"]
    is_aggressive = side in ("ASK", "BID")

    if not (has_vol_oi or has_short_dte or has_big_long or is_aggressive):
        return False

    # --- Aggression requirement ---
    if f["REQUIRE_AGGRESSIVE_SIDE"]:
        if side == "MID" and row.premium < f["ALLOW_MID_IF_PREMIUM_OVER"]:
            return False

    return True


# ---------------------------------------------------------------------------
# Convenience: run all stages and return counters
# ---------------------------------------------------------------------------

def filter_tick(rows: "List[AlertRow]") -> "Tuple[List[AlertRow], dict]":
    """
    Run stages 0→1→2 over a list of AlertRows.
    Returns (candidates, stats) where stats = {
        parsed, dropped_stage0, dropped_stage1, dropped_stage2, pre_insert
    }.
    The caller is responsible for the Top-N slice (MAX_INSERT_PER_TICK).
    """
    s0_drop = s1_drop = s2_drop = 0
    candidates = []

    for row in rows:
        if not _passes_stage0(row):
            s0_drop += 1
            continue
        if not _passes_stage1(row):
            s1_drop += 1
            continue
        if not _passes_stage2(row):
            s2_drop += 1
            continue
        candidates.append(row)

    # Sort best first
    candidates.sort(key=lambda r: r.score_total, reverse=True)

    stats = {
        "parsed":         len(rows),
        "dropped_stage0": s0_drop,
        "dropped_stage1": s1_drop,
        "dropped_stage2": s2_drop,
        "pre_insert":     len(candidates),
    }
    return candidates, stats
