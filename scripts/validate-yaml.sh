#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import yaml, pathlib
p = pathlib.Path.home() / "alpha-sentinel" / "config" / "agent.yml"
data = yaml.safe_load(p.read_text())
assert isinstance(data, dict), "YAML must be key: value"
required = ["interval_sec","max_alerts_per_tick","replay_from_start","watchlist_refresh_sec","options_csv"]
missing = [k for k in required if k not in data]
if missing:
    raise SystemExit(f"Missing keys: {missing}")
print("YAML OK ✅")
print(data)
PY
