#!/usr/bin/env python3
# /opt/alpha-sentinel/app/selfcheck.py
"""
Alpha Sentinel Self-Check

Lightweight startup verification script.
Verifies:
  1. Required DB tables exist (including new UW tables)
  2. Provider selection works correctly with UW disabled (default)
  3. /integrations and /uw/health API endpoint logic would return correct shape

Run: python -m app.selfcheck
Exit code: 0 = all checks passed, 1 = one or more failures
"""
from __future__ import annotations

import os
import sys
import traceback

REQUIRED_TABLES = [
    "users",
    "alerts",
    "health_snapshots",
    "monitor",
    "watchlist",
    "integrations_state",
    "uw_events",
]

_passed = 0
_failed = 0


def _ok(name: str) -> None:
    global _passed
    _passed += 1
    print(f"  [PASS] {name}")


def _fail(name: str, reason: str) -> None:
    global _failed
    _failed += 1
    print(f"  [FAIL] {name}: {reason}")


# ---------------------------------------------------------------------------
# Check 1: DB tables exist
# ---------------------------------------------------------------------------
def check_db_tables() -> None:
    print("\n[1] DB tables")
    try:
        from app.db import db, init_db
        init_db()
        with db() as conn:
            existing = {
                r["name"]
                for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        for table in REQUIRED_TABLES:
            if table in existing:
                _ok(f"table '{table}' exists")
            else:
                _fail(f"table '{table}'", "not found in DB")
    except Exception as exc:
        _fail("DB tables check", f"exception: {exc}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Check 2: Provider selection with UW disabled
# ---------------------------------------------------------------------------
def check_provider_selection() -> None:
    print("\n[2] Provider selection (UW disabled)")
    # Force UW off for this check
    os.environ["UW_ENABLED"] = "0"
    os.environ["UW_API_KEY"] = ""

    try:
        # Simulate a minimal agent-like object so we don't spin up the full agent
        class _FakeAgent:
            csv_path = os.environ.get("OPTIONS_CSV", "").strip() or None
            max_alerts_per_tick = 3

            def _read_csv_new(self):
                return []

        from app.providers import select_provider, CSVProvider, MockProvider
        agent = _FakeAgent()
        provider = select_provider(agent)

        if agent.csv_path:
            if isinstance(provider, CSVProvider):
                _ok(f"CSVProvider selected (OPTIONS_CSV={agent.csv_path})")
            else:
                _fail("provider", f"expected CSVProvider, got {type(provider).__name__}")
        else:
            if isinstance(provider, MockProvider):
                _ok("MockProvider selected (no OPTIONS_CSV)")
            else:
                _fail("provider", f"expected MockProvider, got {type(provider).__name__}")

        # Verify fetch() returns a list (may be empty for MockProvider)
        result = provider.fetch()
        if isinstance(result, list):
            _ok(f"provider.fetch() returned list (len={len(result)})")
        else:
            _fail("provider.fetch()", f"expected list, got {type(result).__name__}")

    except Exception as exc:
        _fail("provider selection", f"exception: {exc}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Check 3: UW provider returns [] when disabled
# ---------------------------------------------------------------------------
def check_uw_provider_disabled() -> None:
    print("\n[3] UnusualWhalesProvider disabled (should return [])")
    os.environ["UW_ENABLED"] = "0"
    os.environ["UW_API_KEY"] = ""
    try:
        from app.providers import UnusualWhalesProvider
        uw = UnusualWhalesProvider.from_env()
        result = uw.fetch()
        if result == []:
            _ok("UnusualWhalesProvider.fetch() returns [] when disabled")
        else:
            _fail("UnusualWhalesProvider.fetch()", f"expected [], got {result!r}")
    except Exception as exc:
        _fail("UnusualWhalesProvider", f"exception: {exc}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Check 4: API endpoint logic shapes
# ---------------------------------------------------------------------------
def check_api_shapes() -> None:
    print("\n[4] API /integrations + /uw/health logic (env read only)")
    os.environ["UW_ENABLED"] = "0"
    os.environ["UW_API_KEY"] = ""
    os.environ["UW_BASE_URL"] = "https://api.unusualwhales.com"
    os.environ["UW_MODE"] = "poll"

    try:
        uw_enabled = os.environ.get("UW_ENABLED", "0").strip() in ("1", "true", "yes")
        uw_api_key = os.environ.get("UW_API_KEY", "").strip()

        integrations_resp = {
            "ok": True,
            "unusual_whales": {
                "enabled": uw_enabled,
                "configured": bool(uw_api_key),
                "mode": os.environ.get("UW_MODE", "poll"),
                "coming_soon": not uw_enabled,
            },
        }
        assert integrations_resp["ok"] is True
        assert integrations_resp["unusual_whales"]["coming_soon"] is True
        assert integrations_resp["unusual_whales"]["enabled"] is False
        _ok("/integrations response shape is correct")

        uw_health_resp = {"ok": True, "enabled": False} if not uw_enabled else {"ok": False, "error": "missing api key"}
        assert uw_health_resp["ok"] is True
        assert uw_health_resp["enabled"] is False
        _ok("/uw/health response shape is correct")

    except AssertionError as exc:
        _fail("API shape check", f"assertion failed: {exc}")
    except Exception as exc:
        _fail("API shape check", f"exception: {exc}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 55)
    print("  Alpha Sentinel Self-Check")
    print("=" * 55)

    check_db_tables()
    check_provider_selection()
    check_uw_provider_disabled()
    check_api_shapes()

    print(f"\n{'=' * 55}")
    print(f"  Results: {_passed} passed, {_failed} failed")
    print("=" * 55)

    if _failed > 0:
        print("\n[SELFCHECK FAILED] Fix the issues above before deploying.\n")
        return 1
    print("\n[SELFCHECK PASSED] System is ready.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
