"""Gateway turn prepare phases; inherited by GatewayTurnMixin."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING
import asyncio
import dataclasses
import inspect
import json
import os
import queue
import threading
import time
from agent.i18n import t
from agent.session_activity import format_iteration_progress
from contextlib import nullcontext, suppress
from contextvars import copy_context
from gateway.config import Platform
from gateway.media_repair import repair_explicit_computer_use_media_paths
from gateway.platforms.base import BasePlatformAdapter
from gateway.platforms.event import MessageEvent
from gateway.session import (
    SessionSource, _session_key_namespace, build_channel_continuity_note,
    build_session_context,
)
from gateway.session_transcript import TranscriptReadError
from gateway.turn_context import TurnContext
from gateway.turn_lease import DEFAULT_LEASE_WAIT, TurnLeaseTimeoutError
from hermes_constants import get_hermes_home_override
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from utils import base_url_hostname

if TYPE_CHECKING:  # string annotations only; never imported at runtime (cycle)
    from gateway.run import GatewayRunner
    from gateway.run_turn_runner import TurnRunner

# Log-record parity with the origin module.
logger = logging.getLogger("gateway.run")
# ``_resolve_turn_agent_config``'s default tier: the runner's per-turn ``_service_tier``.
_TIER_FROM_RUNNER = object()


def _resolve_policy_agent_runtime(runner, policy, *, fallback=True, record_notice=True) -> tuple[str, dict]:
    """``_resolve_session_agent_runtime`` for a session bound to a launch policy: the route's FROZEN
    config and launch credential, walking its own fallback chain on an AuthError unless
    ``fallback=False`` (a session control asking which route the session itself runs). Module-level
    so tests that bind the method onto bare namespaces still reach it. ``record_notice=False``
    leaves the runner's one-shot fallback notice alone (see ``_resolve_session_agent_runtime``)."""
    from gateway.run import _runtime_agent_kwargs
    from gateway.session_policy import route_launch_key
    from gateway.session_worker_construct import route_runtime
    from gateway.session_authorities import active_authority
    authority = active_authority(runner)
    # A follow_profile_config session's route is the profile's current one, model and endpoint from one read.
    frozen, model = policy.effective(authority)
    provider = (frozen.get('model') or {}).get('provider')
    runtime, fallback_entry = route_runtime(policy, frozen, model, route_launch_key(authority, policy, frozen),
                                            fallback=fallback)
    if fallback_entry is not None:
        from hermes_cli.fallback_config import pre_agent_fallback_notice
        from agent.primary_auth_fallback import RUNTIME_KEY, set_pending_primary_auth_failure
        set_pending_primary_auth_failure(runtime.get(RUNTIME_KEY))
        notice = pre_agent_fallback_notice(
            provider or '', model or '',
            runtime.get('provider') or fallback_entry.get('provider') or 'unknown',
            fallback_entry.get('model') or 'default')
        if record_notice:
            runner._pre_agent_fallback_notice = notice
        return fallback_entry['model'], _runtime_agent_kwargs(runtime)
    return model, _runtime_agent_kwargs(runtime)


def _recover_or_remember_model(runner, skey, model):
    """Last step of ``_resolve_session_agent_runtime``: remember a good model, recover an empty one."""
    # Final safety net: an empty model (transient config-cache miss) makes every API call 400 and
    # the session goes silent — reuse the last model resolved for this session, else process-wide.
    if not model:
        _lr_state = runner._peek_session_state(skey) if skey else None
        _lr_star = runner._peek_session_state("*")
        _recovered = (
            (_lr_state.conversation.last_resolved_model if _lr_state else "")
            or (_lr_star.conversation.last_resolved_model if _lr_star else "")
        )
        if _recovered:
            logger.warning(
                "Empty model resolved for session=%s — recovering "
                "last-known-good model %s (config read likely returned "
                "empty; see #35314)", skey or "", _recovered,
            )
            model = _recovered
    else:
        # Cache the good resolution for future recovery turns.
        if skey:
            runner._session_state(skey).conversation.last_resolved_model = model
        runner._session_state("*").conversation.last_resolved_model = model
    return model


def _managed_oauth_env_host(provider: str) -> str:
    """The profile's documented host override for a managed OAuth provider, read through the
    secret scope like the resolver reads it: the process env holds the launch profile's value."""
    from agent.secret_scope import get_secret_str
    from hermes_cli.auth_xai import DEFAULT_XAI_OAUTH_BASE_URL, _xai_validate_inference_base_url

    if provider == "openai-codex":
        return get_secret_str("HERMES_CODEX_BASE_URL", "").strip()
    raw = get_secret_str("HERMES_XAI_BASE_URL", "").strip() or get_secret_str("XAI_BASE_URL", "").strip()
    return _xai_validate_inference_base_url(raw, fallback=DEFAULT_XAI_OAUTH_BASE_URL)


def _session_override_uses_managed_oauth(override: Optional[dict]) -> bool:
    """Only official Codex/xAI routes borrow the profile's current OAuth credential.

    A direct alias can retain the provider label but owns its endpoint/key. Never refresh a
    custom host's credential from the official provider's store (or the reverse).
    """
    from hermes_cli.auth import PROVIDER_REGISTRY

    if not override or override.get("owns_credentials"):
        return False
    provider = str(override.get("provider") or "")
    if provider not in ("openai-codex", "xai-oauth"):
        return False
    # The managed host is the registry default OR its documented override (HERMES_CODEX_BASE_URL /
    # HERMES_XAI_BASE_URL): the auth store still owns the bearer for either.
    managed_hosts = {PROVIDER_REGISTRY[provider].inference_base_url.rstrip("/"),
                     _managed_oauth_env_host(provider).rstrip("/")} - {""}
    return (override.get("base_url") or "").rstrip("/") in managed_hosts


def _resolve_session_override_runtime(override: dict, *, managed_oauth: bool) -> dict:
    from gateway.run import _resolve_runtime_agent_kwargs_for_provider

    runtime = _resolve_runtime_agent_kwargs_for_provider(
        override["provider"], target_model=override.get("model") or None)
    if managed_oauth:
        if (runtime.get("provider") != override["provider"]
                or (runtime.get("base_url") or "").rstrip("/") != override["base_url"].rstrip("/")
                or not runtime.get("api_key")):
            raise RuntimeError("Managed OAuth resolution no longer matches the session route")
        # _apply_session_model_override reads this dict again: replacing only the returned
        # runtime would re-overlay the switch-time key and its obsolete pool.
        override.update(api_key=runtime["api_key"], credential_pool=runtime.get("credential_pool"))
    return runtime


def _log_missing_session_override(runner, skey, model) -> None:
    # The override_keys scan walks every session; only pay for it when DEBUG is on.
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "No session model override: session=%s config_model=%s override_keys=%s",
            skey or "", model,
            [key for key, state in list(runner._sessions_map().items())
             if state.conversation.model_override is not None][:5] or "[]",
        )


def _session_override_snapshot(override: dict) -> dict:
    runtime = {key: override.get(key) for key in (
        "provider", "requested_provider", "api_key", "base_url", "api_mode",
        "max_tokens", "credential_pool", "request_overrides", "capabilities",
    )}
    runtime["capabilities"] = dict(runtime["capabilities"] or {})
    return runtime


def _resolve_channel_runtime(runner, source, model, runtime_kwargs) -> tuple[str, dict]:
    from gateway.run import _get_channel_override, _resolve_runtime_agent_kwargs_for_provider

    cfg = runner._serving_channel_config(source) if source is not None else None
    if cfg:
        ch = _get_channel_override(
            cfg, source.platform, str(source.chat_id) if source.chat_id else "",
            thread_id=str(source.thread_id) if getattr(source, "thread_id", None) else None,
            parent_id=str(source.parent_chat_id) if getattr(source, "parent_chat_id", None) else None,
        )
        if ch:
            if ch.model:
                model = ch.model
            if ch.provider:
                runtime_kwargs = _resolve_runtime_agent_kwargs_for_provider(ch.provider, target_model=model or None)
                ch_runtime_model = runtime_kwargs.pop("model", None)
                # Adopt the provider's bundled model only when the override named none.
                if ch_runtime_model and not ch.model:
                    model = ch_runtime_model
    return model, runtime_kwargs


class GatewayTurnPrepareMixin:
    def _resolve_source_channel_model_binding(
        self, source: Optional[SessionSource],
    ) -> Optional[dict[str, str]]:
        """Best-effort channel binding lookup for synthetic platform events.

        Normal adapter events carry the resolved binding on ``MessageEvent``.
        Synthetic events such as goal continuations may only have a
        ``SessionSource``; look up the same config here so durable channel
        bindings still apply.
        """
        if not source or not source.platform:
            return None
        adapter = getattr(self, "adapters", {}).get(source.platform)
        extra = getattr(getattr(adapter, "config", None), "extra", None)
        if not isinstance(extra, dict):
            return None
        channel_id = str(source.thread_id or source.chat_id or "")
        parent_id = str(source.parent_chat_id or "") or None
        if not parent_id and source.thread_id and source.chat_id:
            chat_id = str(source.chat_id)
            if chat_id and chat_id != channel_id:
                parent_id = chat_id
        if not channel_id:
            return None
        from gateway.platforms.base import resolve_channel_model_binding
        return resolve_channel_model_binding(extra, channel_id, parent_id)

    def _resolve_session_agent_runtime(
        self, *, source: Optional[SessionSource] = None, session_key: Optional[str] = None,
        channel_model_binding: Optional[dict[str, str]] = None,
