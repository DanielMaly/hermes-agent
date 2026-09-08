# health: allow FILE_LINES -- platform-tables refactor: core's PLATFORM_HINTS/_PLATFORM_DEFAULTS rows for this platform moved onto its register() call (hint text lives in adapter_hint.py); the growth is only those kwargs
"""Slack platform adapter: slack-bolt Socket Mode (messages, slash commands, threads)."""

import asyncio
import contextvars
import functools
import inspect
import json
import logging
import os
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Awaitable, Callable, ClassVar, Collection, Dict, Optional, Any, Tuple, List

aiohttp: Any = None
try:
    import aiohttp as _aiohttp
    from slack_bolt.async_app import AsyncApp
    from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
    from slack_sdk.web.async_client import AsyncWebClient

    aiohttp = _aiohttp
    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False
    AsyncApp = Any
    AsyncSocketModeHandler = Any
    AsyncWebClient = Any

import sys
from pathlib import Path as _Path

_checkout_root = _Path(__file__).resolve().parents[3]
# Append, never prepend: the checkout must not shadow same-named installed packages.
if str(_checkout_root) not in sys.path:
    sys.path.append(str(_checkout_root))

from agent.compression_marker import ELISION_MARKER_MAX_LEN, elide
from agent.i18n import t
from agent.retry_utils import parse_retry_after_seconds
from agent.secret_scope import get_secret
from gateway.config import Platform, PlatformConfig
from gateway.approval_actor import approval_refusal, clarify_refusal, refusal_text, slash_confirm_refusal
from gateway.platforms._shared import (
    apply_yaml_bridge as _apply_yaml_bridge, env_is_connected as _env_is_connected,
    extra_or_secret as _extra_or_secret, get_scoped_secret as _get_scoped_secret,
    platform_gate_env as _scoped_gate_env, request_never_left, send_error
)
from gateway.platforms.base_log_safety import safe_url_for_log
from gateway.platforms.helpers import MessageDeduplicator, clarify_text_with_choices, inline_document_text
from plugins.platforms.slack.adapter_config_readers import (
    _extra_or_env_channel_set_getter, _extra_or_env_flag_getter)
from gateway.platforms.base import (
    gateway_trust_env, BasePlatformAdapter, ExecApprovalPrompt,
    SendResult, SUPPORTED_DOCUMENT_TYPES, SUPPORTED_VIDEO_TYPES, _TEXT_INJECT_EXTENSIONS,
    resolve_channel_model_binding,
