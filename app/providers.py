# /opt/alpha-sentinel/app/providers.py
"""
Alpha Sentinel – Data Provider Architecture

Provider selection is controlled by environment variables:
  UW_ENABLED=1 AND UW_API_KEY set  -> UnusualWhalesProvider  (NOT YET LIVE)
  OPTIONS_CSV set                   -> CSVProvider
  (neither)                         -> MockProvider

All providers implement BaseProvider.fetch() -> List[AlertRow].

UnusualWhalesProvider returns [] unless UW is fully enabled+configured.
The agent wraps provider.fetch() in try/except and falls back to [] on error.
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    # Avoid circular import; agent imports providers, providers type-hints agent
    from app.agent import SentinelAgent, AlertRow


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------

class BaseProvider:
    """Abstract provider interface."""

    def fetch(self) -> "List[AlertRow]":
        """Return up to max_alerts_per_tick AlertRow objects, or []."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# CSVProvider — wraps the existing _read_csv_new logic on SentinelAgent
# ---------------------------------------------------------------------------

class CSVProvider(BaseProvider):
    """Tails the configured OPTIONS_CSV / OPTIONS_CSVS files, yielding new rows each tick."""

    def __init__(self, agent: "SentinelAgent") -> None:
        self._agent = agent

    def fetch(self) -> "List[AlertRow]":
        return self._agent._read_all_csvs()


# ---------------------------------------------------------------------------
# MockProvider — generates synthetic alerts for development / demo
# ---------------------------------------------------------------------------

class MockProvider(BaseProvider):
    """Generates random mock alerts. Used when no CSV or UW is configured."""

    def __init__(self, agent: "SentinelAgent") -> None:
        self._agent = agent

    def fetch(self) -> "List[AlertRow]":
        from app.agent import _mock_alert
        n = min(3, self._agent.max_alerts_per_tick)
        return [_mock_alert() for _ in range(n)]


# ---------------------------------------------------------------------------
# UnusualWhalesProvider — COMING SOON (feature-flagged, stubbed)
# ---------------------------------------------------------------------------

class UnusualWhalesProvider(BaseProvider):
    """
    Unusual Whales live data provider.

    Status: STUBBED — returns [] until the live integration is implemented.

    Behaviour:
      - Returns [] immediately if UW_ENABLED != '1' OR UW_API_KEY is empty.
      - Designed for two future modes (UW_MODE):
          'poll'   – periodic REST polling of /api/option-trades/activity
          'stream' – websocket streaming (not yet implemented)
      - No actual HTTP calls are made in this release.
    """

    def __init__(self, api_key: str, base_url: str, mode: str, poll_sec: float, rate_limit_sleep: float) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._mode = mode
        self._poll_sec = poll_sec
        self._rate_limit_sleep = rate_limit_sleep
        self._enabled = bool(api_key)

    @classmethod
    def from_env(cls) -> "UnusualWhalesProvider":
        return cls(
            api_key=os.environ.get("UW_API_KEY", "").strip(),
            base_url=os.environ.get("UW_BASE_URL", "https://api.unusualwhales.com"),
            mode=os.environ.get("UW_MODE", "poll"),
            poll_sec=float(os.environ.get("UW_POLL_SEC", "2")),
            rate_limit_sleep=float(os.environ.get("UW_RATE_LIMIT_SLEEP", "0.25")),
        )

    def fetch(self) -> "List[AlertRow]":
        """
        Returns [] — UW live ingestion is not yet implemented.
        When UW_ENABLED=1 and UW_API_KEY is set this will eventually call _poll() or _stream().
        """
        if not self._enabled:
            return []

        # Stub: real implementation will call self._poll() or self._stream()
        return []

    # ------------------------------------------------------------------
    # Future implementation stubs (do NOT remove — these will be filled in)
    # ------------------------------------------------------------------

    def _poll(self) -> "List[AlertRow]":
        """
        Stub for REST polling mode.
        Will call GET {base_url}/api/option-trades/activity with bearer auth.
        Not yet implemented.
        """
        raise NotImplementedError("UW polling not yet implemented")

    def _stream(self) -> "List[AlertRow]":
        """
        Stub for websocket streaming mode.
        Not yet implemented.
        """
        raise NotImplementedError("UW websocket streaming not yet implemented")

    def health(self) -> dict:
        """Returns a status dict suitable for the /uw/health API endpoint."""
        return {
            "ok": self._enabled,
            "enabled": self._enabled,
            "configured": bool(self._api_key),
            "mode": self._mode,
            "base_url": self._base_url,
            "coming_soon": True,
        }


# ---------------------------------------------------------------------------
# Factory: select the correct provider from environment
# ---------------------------------------------------------------------------

def select_provider(agent: "SentinelAgent") -> BaseProvider:
    """
    Returns the appropriate provider based on env vars.
    Priority: UnusualWhales > CSV > Mock
    Fails closed: any error in building the UW provider falls back to CSV/Mock.
    """
    uw_enabled = os.environ.get("UW_ENABLED", "0").strip() in ("1", "true", "yes")
    uw_api_key = os.environ.get("UW_API_KEY", "").strip()

    if uw_enabled and uw_api_key:
        try:
            provider = UnusualWhalesProvider.from_env()
            print(f"[providers] selected UnusualWhalesProvider (mode={provider._mode})", flush=True)
            return provider
        except Exception as exc:
            print(f"[providers] UW provider init failed ({exc}), falling back", flush=True)

    if agent.csv_path:
        print(f"[providers] selected CSVProvider (path={agent.csv_path})", flush=True)
        return CSVProvider(agent)

    print("[providers] selected MockProvider", flush=True)
    return MockProvider(agent)
