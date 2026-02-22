# app/replay.py
from __future__ import annotations

import csv
import glob
import os
import threading
import time
from typing import Any, Dict, List, Optional

# -----------------------------
# Replay state (module-global)
# -----------------------------
_REPLAY_LOCK = threading.Lock()
_REPLAY_DATA: List[Dict[str, Any]] = []
_REPLAY_INDEX: int = 0
_REPLAY_RUNNING: bool = False
_REPLAY_THREAD: Optional[threading.Thread] = None
_REPLAY_SOURCE: Optional[str] = None


# -----------------------------
# Helpers
# -----------------------------
def _pick(row: Dict[str, Any], candidates: List[str], default: str = "") -> str:
    """Return first non-empty value for any of the candidate keys (case-insensitive)."""
    if not row:
        return default

    # Build case-insensitive lookup once per call
    lower_map = {str(k).strip().lower(): k for k in row.keys()}

    for c in candidates:
        key = lower_map.get(c.strip().lower())
        if key is None:
            continue
        v = row.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def _autodetect_latest_csv(pattern: str = "/data/options-flow-*.csv") -> str:
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No CSV found matching: {pattern}")
    # choose newest by mtime
    return max(matches, key=lambda p: os.path.getmtime(p))


def _reset_state() -> None:
    global _REPLAY_DATA, _REPLAY_INDEX, _REPLAY_SOURCE
    _REPLAY_DATA = []
    _REPLAY_INDEX = 0
    _REPLAY_SOURCE = None


# -----------------------------
# Public API
# -----------------------------
def load_csv(path: Optional[str] = None, *, autodetect_pattern: str = "/data/options-flow-*.csv") -> Dict[str, Any]:
    """
    Load an options flow CSV into memory.
    If path is None, autodetect newest /data/options-flow-*.csv
    """
    global _REPLAY_DATA, _REPLAY_INDEX, _REPLAY_SOURCE

    if path is None:
        path = _autodetect_latest_csv(autodetect_pattern)

    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]

    with _REPLAY_LOCK:
        _reset_state()
        _REPLAY_DATA = rows
        _REPLAY_INDEX = 0
        _REPLAY_SOURCE = path

    return {"ok": True, "path": path, "rows": len(rows), "columns": list(rows[0].keys()) if rows else []}


def next_tick() -> Optional[Dict[str, Any]]:
    """Return next row in replay stream (loops back to start)."""
    global _REPLAY_INDEX

    with _REPLAY_LOCK:
        if not _REPLAY_DATA:
            return None

        if _REPLAY_INDEX >= len(_REPLAY_DATA):
            _REPLAY_INDEX = 0

        row = _REPLAY_DATA[_REPLAY_INDEX]
        _REPLAY_INDEX += 1
        return row


def start_replay(*, interval_sec: float = 1.0, interval: Optional[float] = None, max_lines: Optional[int] = None) -> Dict[str, Any]:
    """
    Start background replay logging.
    Accepts interval_sec (preferred) OR interval (backward-compatible).
    """
    # If api.py passes interval=..., honor it.
    if interval is not None:
        interval_sec = float(interval)

    global _REPLAY_RUNNING, _REPLAY_THREAD

    with _REPLAY_LOCK:
        if _REPLAY_RUNNING:
            return {"ok": True, "running": True, "note": "already running"}

        if not _REPLAY_DATA:
            load_csv(None)

        _REPLAY_RUNNING = True

    def _loop() -> None:
        global _REPLAY_RUNNING
        printed = 0
        while True:
            with _REPLAY_LOCK:
                if not _REPLAY_RUNNING:
                    break

            tick = next_tick()
            if tick:
                sym = _pick(tick, ["Symbol", "Underlying", "UnderlyingSymbol", "Ticker"])
                cp = _pick(tick, ["Call/Put", "C/P", "Type", "OptionType"])
                strike = _pick(tick, ["Strike", "StrikePrice"])
                exp = _pick(tick, ["Expiration", "ExpirationDate", "Expiry", "Exp Date"])
                qty = _pick(tick, ["Volume", "Qty", "Size"])
                prem = _pick(tick, ["Premium", "Notional", "Value", "Trade Value"])
                print(f"[REPLAY] {sym} {cp} {strike} {exp} vol={qty} prem={prem}")

                printed += 1
                if max_lines is not None and printed >= max_lines:
                    with _REPLAY_LOCK:
                        _REPLAY_RUNNING = False
                    break

            time.sleep(max(0.05, float(interval_sec)))

    _REPLAY_THREAD = threading.Thread(target=_loop, name="sentinel-replay", daemon=True)
    _REPLAY_THREAD.start()
    return {"ok": True, "running": True, "interval_sec": interval_sec, "max_lines": max_lines}

def stop_replay() -> Dict[str, Any]:
    global _REPLAY_RUNNING
    with _REPLAY_LOCK:
        _REPLAY_RUNNING = False
    return {"ok": True, "running": False}


def status() -> Dict[str, Any]:
    with _REPLAY_LOCK:
        return {
            "ok": True,
            "running": _REPLAY_RUNNING,
            "rows": len(_REPLAY_DATA),
            "index": _REPLAY_INDEX,
            "source": _REPLAY_SOURCE,
        }