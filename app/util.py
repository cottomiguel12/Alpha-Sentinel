import os, json, time, random
from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def status_from_score(s: float) -> str:
    if s >= 80: return "Strong"
    if s >= 70: return "Monitor"
    if s >= 60: return "Weakening"
    if s >= 50: return "High Risk"
    return "Exit Zone"

def bump_score(prev: float) -> float:
    # small random walk with mild mean reversion to ~72
    drift = (72 - prev) * 0.02
    step = random.uniform(-2.2, 2.2) + drift
    return clamp(prev + step, 0, 100)

def update_history(hist_json: str, new_val: float, max_len: int = 20) -> str:
    try:
        arr = json.loads(hist_json) if hist_json else []
    except Exception:
        arr = []
    arr.append(round(float(new_val), 1))
    arr = arr[-max_len:]
    return json.dumps(arr)
