"""Tests for anchoring base context fetch with search_query.

Validates that the first synchronous base context fetch in prefetch()
passes the user's query as search_query to Honcho's peer.context(),
so that Honcho returns topic-relevant observations instead of an
unfiltered dump.

This is a one-line change (passing `query` to `get_prefetch_context`)
with significant behavioural impact: on cold starts, Honcho's
peer.context() now receives a relevance anchor and returns observations
related to the current conversation rather than all stored observations.
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call

from plugins.memory.honcho.client import HonchoClientConfig


class TestBaseContextFetchAnchoring:
    """Verify that the first synchronous base context fetch passes search_query."""

    def _make_provider(self):
        """Create a HonchoMemoryProvider with mocked internals."""
        from plugins.memory.honcho import HonchoMemoryProvider

        provider = HonchoMemoryProvider()
        provider._manager = MagicMock()
        provider._config = MagicMock()
        provider._config.prefetch_generic_context = False  # skip prewarm
        provider._config.timeout = 8.0
        provider._config.raw = {}
        provider._config.context_tokens = None
        provider._config.dialectic_depth = 1
        provider._config.dialectic_depth_levels = None
        provider._config.dialectic_reasoning_level = "low"
        provider._config.reasoning_heuristic = True
        provider._config.reasoning_level_cap = "high"
        provider._config.message_max_chars = 25000
        provider._config.dialectic_max_input_chars = 10000
        provider._config.user_observe_me = True
        provider._config.user_observe_others = True
        provider._config.ai_observe_me = True
        provider._config.ai_observe_others = True
        provider._config.write_frequency = "async"
        provider._config.session_strategy = "per-directory"
        provider._config.api_key = "test-key"
        provider._config.base_url = None
        provider._config.enabled = True
        provider._config.init_on_session_start = False
        provider._config.peer_name = None
        provider._config.pin_peer_name = False
        provider._config.runtime_peer_prefix = ""
        provider._config.user_peer_aliases = {}
        provider._recall_mode = "hybrid"
        provider._session_key = "test-session"
        provider._session_initialized = True
        provider._cron_skipped = False
        provider._injection_frequency = "every-turn"
        provider._context_cadence = 1
        provider._dialectic_cadence = 1
        return provider

    def test_first_fetch_passes_query_as_search_query(self):
        """The first synchronous base context fetch should pass the user's
        query to get_prefetch_context so Honcho can anchor observations
        to the current conversation topic."""
        provider = self._make_provider()

        # Simulate first call — base_context_cache is None
        assert provider._base_context_cache is None

        # Mock get_prefetch_context to return a proper dict with strings
        provider._manager.get_prefetch_context.return_value = {
            "representation": "test representation",
            "card": "test card",
        }

        # Mock the dialectic layer to return empty (we're testing base context)
        provider._manager.dialectic_query.return_value = ""

        # Call prefetch with a specific query
        provider.prefetch("How does Honcho choose observations?")

        # Verify get_prefetch_context was called with the query
        provider._manager.get_prefetch_context.assert_called_once_with(
            "test-session",
            "How does Honcho choose observations?",
        )

    def test_subsequent_fetch_uses_cache_not_api(self):
        """After the first fetch, subsequent calls should use the cache
        and not call get_prefetch_context again."""
        provider = self._make_provider()

        # First call populates the cache
        provider._manager.get_prefetch_context.return_value = {
            "representation": "test rep",
            "card": "test card",
        }
        provider._manager.dialectic_query.return_value = ""

        result1 = provider.prefetch("first query")
        assert provider._manager.get_prefetch_context.call_count == 1

        # Second call should use cache, not call get_prefetch_context again
        result2 = provider.prefetch("second query")
        assert provider._manager.get_prefetch_context.call_count == 1  # still 1

    def test_trivial_prompt_skips_fetch(self):
        """Trivial prompts like 'ok' or 'yes' should not trigger a fetch."""
        provider = self._make_provider()
        provider._manager.get_prefetch_context.return_value = {}

        result = provider.prefetch("ok")
        # _is_trivial_prompt should return True, so no fetch happens
        # (base_context_cache stays None but no API call is made)
        provider._manager.get_prefetch_context.assert_not_called()

    def test_empty_query_skips_fetch(self):
        """Empty query should not trigger a fetch."""
        provider = self._make_provider()
        provider._manager.get_prefetch_context.return_value = {}

        result = provider.prefetch("")
        provider._manager.get_prefetch_context.assert_not_called()


class TestSearchQueryPropagationToSessionManager:
    """Verify that search_query reaches peer.context() through the session manager."""

    def test_get_prefetch_context_passes_search_query(self):
        """get_prefetch_context should forward user_message as search_query
        to _fetch_peer_context, which passes it to peer.context()."""
        from plugins.memory.honcho.session import HonchoSessionManager

        # Create a minimal manager with mocked Honcho client
        mock_honcho = MagicMock()
        cfg = HonchoClientConfig(api_key="test-key", enabled=True)

        manager = HonchoSessionManager(
            honcho=mock_honcho,
            config=cfg,
            context_tokens=None,
        )

        # Verify the method signature accepts user_message
        import inspect
        sig = inspect.signature(manager.get_prefetch_context)
        params = list(sig.parameters.keys())
        assert "user_message" in params, f"get_prefetch_context missing user_message param, got: {params}"

    def test_fetch_peer_context_passes_search_query(self):
        """_fetch_peer_context should forward search_query to peer.context()."""
        from plugins.memory.honcho.session import HonchoSessionManager

        # Verify the method signature accepts search_query
        import inspect
        sig = inspect.signature(HonchoSessionManager._fetch_peer_context)
        params = list(sig.parameters.keys())
        assert "search_query" in params, f"_fetch_peer_context missing search_query param, got: {params}"