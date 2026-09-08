# health: allow FILE_LINES -- platform-tables refactor: core's PLATFORM_HINTS/_PLATFORM_DEFAULTS rows for this platform moved onto its register() call (hint text lives in adapter_hint.py); the growth is only those kwargs
"""
Feishu/Lark platform adapter.

Supports WebSocket + Webhook transports, DM/@mention-gated group text, inbound
media caching, FEISHU_ALLOWED_USERS allowlisting, persistent dedup, per-chat
serial processing, processing-status reactions (Typing while working, CrossMark
on failure), reaction/card-button events as synthetic events, webhook anomaly
tracking and verification-token validation (all mirroring openclaw).

Feishu identity model (https://open.feishu.cn/document/home/user-identity-introduction/introduction):
  open_id  (ou_xxx) — app-scoped; differs per Feishu app; always in event payloads.
  user_id  (u_xxx)  — tenant-scoped; needs ``contact:user.employee_id:readonly``; may be absent.
  union_id (on_xxx) — developer-scoped; stable across one developer's apps.
Bot: ``app_id`` is the credential; the bot's own open_id (from ``/bot/v3/info``) is
what Feishu puts in ``mentions[].id.open_id`` — used for mention gating only.
Session keys prefer union_id (user_id_alt) over open_id (user_id) for stability.
"""

from __future__ import annotations

from pm import install_hint
import asyncio
import collections
import concurrent.futures
import contextvars
import hashlib
import itertools
import json
import logging
import mimetypes
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Literal, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# aiohttp/websockets are independent optional deps — import outside lark_oapi
# so they remain available for tests and webhook mode even if lark_oapi is missing.
try:
    import aiohttp
    from aiohttp import web
except ImportError:
    aiohttp = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore[assignment]

# lark_oapi is slow to import; the SDK names below stay None until Feishu actually connects
# (_load_lark_oapi binds every name in _LARK_SDK_IMPORTS plus ``lark`` and ``FeishuWSClient``).
_LARK_SDK_IMPORTS = (
    ("lark_oapi.api.application.v6", ("GetApplicationRequest",)),
    ("lark_oapi.api.im.v1", (
        "CreateFileRequest", "CreateFileRequestBody", "CreateImageRequest", "CreateImageRequestBody",
        "CreateMessageRequest", "CreateMessageRequestBody", "GetChatRequest", "GetMessageRequest",
        "DeleteMessageRequest", "GetMessageResourceRequest", "P2ImMessageMessageReadV1",
        "ReplyMessageRequest", "ReplyMessageRequestBody",
        "UpdateMessageRequest", "UpdateMessageRequestBody", "PatchMessageRequest", "PatchMessageRequestBody",
    )),
    ("lark_oapi.core", ("AccessTokenType", "HttpMethod")),
    ("lark_oapi.core.const", ("FEISHU_DOMAIN", "LARK_DOMAIN")),
    ("lark_oapi.core.model", ("BaseRequest",)),
    ("lark_oapi.event.callback.model.p2_card_action_trigger", ("CallBackCard", "CallBackToast", "P2CardActionTriggerResponse")),
    ("lark_oapi.event.dispatcher_handler", ("EventDispatcherHandler",)),
)
lark = FeishuWSClient = None  # type: ignore[assignment]
globals().update({name: None for _, names in _LARK_SDK_IMPORTS for name in names})
FEISHU_AVAILABLE = False
_lark_import_lock = threading.Lock()

FEISHU_WEBSOCKET_AVAILABLE = websockets is not None
FEISHU_WEBHOOK_AVAILABLE = aiohttp is not None

from gateway.approval_actor import approval_refusal, refusal_text, update_prompt_refusal
from gateway.config import Platform, PlatformConfig
from agent.i18n import t
from gateway.platforms.base import (
    BasePlatformAdapter, ExecApprovalPrompt, SendResult, describe_proxy_error, env_proxy_for_redaction,
    SUPPORTED_DOCUMENT_TYPES, cache_document_from_bytes_async, cache_image_from_url,
    cache_audio_from_bytes_async, cache_image_from_bytes_async,
)
from gateway.platforms.event import MessageEvent, MessageType, ProcessingOutcome
from gateway.platforms.helpers import caption_is_command, prompt_id_counter, send_chunks
from .cards import map_chat_type, pop_validated_prompt_state, resolve_source_chat_type, validate_card_action
from .adapter_choice_cards import FeishuChoiceCardsMixin
from .adapter_synthetic_events import FeishuSyntheticEventsMixin
from gateway.status import acquire_scoped_lock
from gateway.status_scoped_locks import release_scoped_lock
from hermes_constants import get_hermes_home
from utils import atomic_json_write, env_float, env_int

from gateway.platforms._shared import (
    apply_yaml_bridge as _apply_yaml_bridge, extra_or_secret as _shared_extra_or_secret,
    get_scoped_secret as _get_scoped_secret, secrets_match, send_error, timestamp_fresh
)


logger = logging.getLogger(__name__)

# --- Regex patterns ---
_MARKDOWN_HINT_RE = re.compile(
    # Pipe table: any header line + separator line both starting with '|'.
    r"(^\|.*\|\s*\n\|[-:|\s]+\|)"
    # Headings, lists, code, bold/italic/strike/underline, links, blockquotes.
    r"|(^#{1,6}\s)"
    r"|(^\s*[-*]\s)"
    r"|(^\s*\d+\.\s)"
    r"|(^\s*---+\s*$)"
    r"|(```)"
    r"|(`[^`\n]+`)"
    r"|(\*\*[^*\n].+?\*\*)"
    r"|(~~[^~\n].+?~~)"
    r"|(<u>.+?</u>)"
    r"|(\*[^*\n]+\*)"
    r"|(\[[^\]]+\]\([^)]+\))"
    r"|(^>\s)",
    re.MULTILINE,
)
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_MARKDOWN_FENCE_OPEN_RE = re.compile(r"^```([^\n`]*)\s*$")
_MARKDOWN_FENCE_CLOSE_RE = re.compile(r"^```\s*$")
_MULTISPACE_RE = re.compile(r"[ \t]{2,}")
_POST_CONTENT_INVALID_RE = re.compile(r"content format of the post type is incorrect", re.IGNORECASE)
# --- Media type sets and upload constants ---
_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
_AUDIO_EXTENSIONS = {".ogg", ".mp3", ".wav", ".m4a", ".aac", ".flac", ".opus", ".webm"}
_VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".3gp"}
_DOCUMENT_MIME_TO_EXT = {mime: ext for ext, mime in SUPPORTED_DOCUMENT_TYPES.items()}
_FEISHU_IMAGE_UPLOAD_TYPE = "message"
_FEISHU_FILE_UPLOAD_TYPE = "stream"
_FEISHU_OPUS_UPLOAD_EXTENSIONS = {".ogg", ".opus"}
_FEISHU_MEDIA_UPLOAD_EXTENSIONS = {".mp4", ".mov", ".avi", ".m4v"}
_FEISHU_DOC_UPLOAD_TYPES = {
    ".pdf": "pdf", ".doc": "doc", ".docx": "doc", ".xls": "xls", ".xlsx": "xls", ".ppt": "ppt", ".pptx": "ppt",
}
# --- Connection, retry and batching tuning ---
_MAX_TEXT_INJECT_BYTES = 100 * 1024
_FEISHU_CONNECT_ATTEMPTS = 3
_FEISHU_SEND_ATTEMPTS = 3
_FEISHU_APP_LOCK_SCOPE = "feishu-app-id"
_DEFAULT_TEXT_BATCH_DELAY_SECONDS = 0.6
_DEFAULT_TEXT_BATCH_MAX_MESSAGES = 8
_DEFAULT_TEXT_BATCH_MAX_CHARS = 4000
_DEFAULT_MEDIA_BATCH_DELAY_SECONDS = 0.8
_DEFAULT_DEDUP_CACHE_SIZE = 2048
_DEFAULT_WEBHOOK_HOST = "127.0.0.1"
_DEFAULT_WEBHOOK_PORT = 8765
_DEFAULT_WEBHOOK_PATH = "/feishu/webhook"
# --- TTL, rate-limit and webhook security constants ---
_FEISHU_DEDUP_TTL_SECONDS = 24 * 60 * 60          # 24 hours — matches openclaw
_FEISHU_SENDER_NAME_TTL_SECONDS = 10 * 60          # 10 minutes sender-name cache
_FEISHU_WEBHOOK_MAX_BODY_BYTES = 1 * 1024 * 1024   # 1 MB body limit
_FEISHU_WEBHOOK_RATE_WINDOW_SECONDS = 60            # sliding window for rate limiter
_FEISHU_WEBHOOK_RATE_LIMIT_MAX = 120               # max requests per window per IP — matches openclaw
_FEISHU_WEBHOOK_RATE_MAX_KEYS = 4096               # max tracked keys (prevents unbounded growth)
_FEISHU_WEBHOOK_BODY_TIMEOUT_SECONDS = 30          # max seconds to read request body
_FEISHU_WEBHOOK_ANOMALY_THRESHOLD = 25             # consecutive error responses before WARNING log
_FEISHU_WEBHOOK_ANOMALY_TTL_SECONDS = 6 * 60 * 60  # anomaly tracker TTL (6 hours) — matches openclaw
_FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS = 15 * 60    # card action token dedup window (15 min)

_APPROVAL_CHOICE_MAP: dict[str, str] = {
    "approve_once": "once", "approve_session": "session", "approve_always": "always", "deny": "deny",
}
# choice → catalog key of the resolved-card title; looked up through ``t()`` at resolve time.
_APPROVAL_LABEL_KEYS: dict[str, str] = {
    "once": "platform.feishu.approval.resolved_once", "session": "platform.feishu.approval.resolved_session",
    "always": "platform.feishu.approval.resolved_always", "deny": "platform.feishu.approval.resolved_deny",
}


async def _read_limited_feishu_webhook_body(request: Any, max_bytes: int) -> bytes:
    """Read at most ``max_bytes`` from an aiohttp request body."""
    try:
        body = await request.content.readexactly(max_bytes + 1)
    except asyncio.IncompleteReadError as exc:
        body = exc.partial
    if len(body) > max_bytes:
        raise ValueError("payload too large")
    return body


_FEISHU_REPLY_FALLBACK_CODES = frozenset({230011, 231003})  # reply target withdrawn/missing → create fallback

# Feishu reactions render as prominent badges, unlike Discord/Telegram's
# small footer emoji — a success badge on every message would add noise, so
# we only mark start (Typing) and failure (CrossMark); the reply itself is
# the success signal.
_FEISHU_REACTION_IN_PROGRESS = "Typing"
_FEISHU_REACTION_FAILURE = "CrossMark"
# Bound on the (message_id → reaction_id) handle cache. Happy-path entries
# drain on completion; the cap is a safeguard against unbounded growth from
# delete-failures, not a capacity plan.
_FEISHU_PROCESSING_REACTION_CACHE_SIZE = 1024
_FEISHU_MESSAGE_TEXT_CACHE_SIZE = 512       # LRU cap for reply-context message text lookups

# QR onboarding constants
_ONBOARD_ACCOUNTS_URLS = {
    "feishu": "https://accounts.feishu.cn",
    "lark": "https://accounts.larksuite.com",
}
_ONBOARD_OPEN_URLS = {"feishu": "https://open.feishu.cn", "lark": "https://open.larksuite.com"}
_REGISTRATION_PATH = "/oauth/v1/app/registration"
_ONBOARD_REQUEST_TIMEOUT_S = 10

# --- Fallback display strings ---
FALLBACK_POST_TEXT = "[Rich text message]"
FALLBACK_FORWARD_TEXT = "[Merged forward message]"
FALLBACK_SHARE_CHAT_TEXT = "[Shared chat]"
FALLBACK_INTERACTIVE_TEXT = "[Interactive message]"
FALLBACK_IMAGE_TEXT = "[Image]"
FALLBACK_ATTACHMENT_TEXT = "[Attachment]"
# --- Post/card parsing helpers ---
_PREFERRED_LOCALES = ("zh_cn", "en_us")
_MARKDOWN_SPECIAL_CHARS_RE = re.compile(r"([\\`*_{}\[\]()#+\-!|>~])")
_MENTION_PLACEHOLDER_RE = re.compile(r"@_user_\d+")
_MENTION_BOUNDARY_CHARS = frozenset(" \t\n\r.,;:!?、，。；：！？()[]{}<>\"'`")
_TRAILING_TERMINAL_PUNCT = frozenset(" \t\n\r.!?。！？")
_WHITESPACE_RE = re.compile(r"\s+")
_SUPPORTED_CARD_TEXT_KEYS = (
    "title", "text", "content", "label", "value", "name", "summary", "subtitle", "description", "placeholder", "hint",
)
_RICH_BLOCK_TAGS = {
    "plain_text", "lark_md", "markdown", "note", "div", "column_set", "column", "action", "button", "select_static",
    "date_picker",
}
_SKIP_TEXT_KEYS = {
    "tag", "type", "msg_type", "message_type", "chat_id", "open_chat_id", "share_chat_id", "file_key", "image_key",
    "user_id", "open_id", "union_id", "url", "href", "link", "token", "template", "locale",
}


@dataclass(frozen=True)
class FeishuPostMediaRef:
    file_key: str
    file_name: str = ""
    resource_type: str = "file"


@dataclass(frozen=True)
class FeishuMentionRef:
    name: str = ""
    open_id: str = ""
    is_all: bool = False
    is_self: bool = False


@dataclass(frozen=True)
class _FeishuBotIdentity:
    open_id: str = ""
    user_id: str = ""
    name: str = ""

    def matches(self, *, open_id: str, user_id: str, name: str) -> bool:
        # Precedence: open_id > user_id > name. IDs are authoritative when both
        # sides have them; the next tier is only considered when either side
        # lacks the current one.
        if open_id and self.open_id:
            return open_id == self.open_id
        if user_id and self.user_id:
            return user_id == self.user_id
        return bool(self.name) and name == self.name


@dataclass(frozen=True)
class FeishuPostParseResult:
    text_content: str
    image_keys: list[str] = field(default_factory=list)
    media_refs: list[FeishuPostMediaRef] = field(default_factory=list)


@dataclass(frozen=True)
class FeishuNormalizedMessage:
    raw_type: str
    text_content: str
    preferred_message_type: str = "text"
    image_keys: list[str] = field(default_factory=list)
    media_refs: list[FeishuPostMediaRef] = field(default_factory=list)
    mentions: list[FeishuMentionRef] = field(default_factory=list)
    relation_kind: str = "plain"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FeishuAdapterSettings:
    """Every field is mirrored onto the adapter as ``self._<field>`` by ``_apply_settings``."""

    app_id: str  # credential identifier, never taken from event payloads
    app_secret: str
    domain_name: str
    connection_mode: str
    encrypt_key: str
    verification_token: str
    group_policy: str
    allowed_group_users: frozenset[str]
    bot_open_id: str  # app-scoped id from /bot/v3/info — what mentions[].id.open_id carries
    bot_user_id: str  # tenant-scoped fallback for mention matching
    bot_name: str
    dedup_cache_size: int
    text_batch_delay_seconds: float
    text_batch_split_delay_seconds: float
    text_batch_max_messages: int
    text_batch_max_chars: int
    media_batch_delay_seconds: float
    webhook_host: str
    webhook_port: int
    webhook_path: str
    ws_reconnect_nonce: int = 30
    ws_reconnect_interval: int = 120
    ws_ping_interval: Optional[int] = None
    ws_ping_timeout: Optional[int] = None
    admins: frozenset[str] = frozenset()
    default_group_policy: str = ""
    group_rules: dict[str, FeishuGroupRule] = field(default_factory=dict)
    allow_bots: str = "none"  # "none" | "mentions" | "all"
    require_mention: bool = True
    allow_all_dm: bool = False  # resolved per-profile so multiplexed adapters honor their own .env


@dataclass
class FeishuGroupRule:
    """Per-group policy rule for controlling which users may interact with the bot."""

    policy: str  # "open" | "allowlist" | "blacklist" | "admin_only" | "disabled"
    allowlist: set[str] = field(default_factory=set)
    blacklist: set[str] = field(default_factory=set)
    require_mention: Optional[bool] = None  # None = inherit global


@dataclass
class FeishuBatchState:
    events: dict[str, MessageEvent] = field(default_factory=dict)
    tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)


# --- Admission: policy types ---

RejectReason = Literal["self_echo", "self_ids_unknown", "bots_disabled", "bot_not_mentioned", "group_policy_rejected"]


def _is_bot_sender(sender: Any) -> bool:
    # receive_v1 docs say {user, bot}; accept "app" defensively.
    return getattr(sender, "sender_type", "") in {"bot", "app"}


def _sender_identity(sender: Any) -> frozenset:
    # Take any non-empty id variant — tenant sender_id_type decides which are populated.
    sid = getattr(sender, "sender_id", None)
    if sid is None:
        return frozenset()
    return frozenset(v for v in (getattr(sid, k, None) for k in ("open_id", "user_id", "union_id")) if v)


# --- Markdown rendering helpers ---

def _escape_markdown_text(text: str) -> str:
    return _MARKDOWN_SPECIAL_CHARS_RE.sub(r"\\\1", text)


def _to_boolean(value: Any) -> bool:
    return value is True or value == 1 or value == "true"


def _is_style_enabled(style: dict[str, Any] | None, key: str) -> bool:
    if not style:
        return False
    return _to_boolean(style.get(key))


def _wrap_inline_code(text: str) -> str:
    max_run = max([0, *[len(run) for run in re.findall(r"`+", text)]])
    fence = "`" * (max_run + 1)
    body = f" {text} " if text.startswith("`") or text.endswith("`") else text
    return f"{fence}{body}{fence}"


def _sanitize_fence_language(language: str) -> str:
    return language.strip().replace("\n", " ").replace("\r", " ")


_TEXT_STYLE_WRAPPERS = (("bold", "**", "**"), ("italic", "*", "*"), ("underline", "<u>", "</u>"), ("strikethrough", "~~", "~~"))


def _render_text_element(element: dict[str, Any]) -> str:
    text = str(element.get("text", "") or "")
    style = element.get("style")
    style_dict = style if isinstance(style, dict) else None
    if _is_style_enabled(style_dict, "code"):
        return _wrap_inline_code(text)
    # Post text elements carry raw text plus separate style flags; the style wrappers below
    # re-create the markdown. Escaping the text here put `\*\*bold\*\*` / `\`code\`` into the
    # model's context and those backslashes came straight back out in replies (#9816).
    rendered = text
    if not rendered:
        return ""
    for key, prefix, suffix in _TEXT_STYLE_WRAPPERS:  # order matters for nesting
        if _is_style_enabled(style_dict, key):
            rendered = f"{prefix}{rendered}{suffix}"
    return rendered


def _render_code_block_element(element: dict[str, Any]) -> str:
    language = _sanitize_fence_language(str(element.get("language", "") or "") or str(element.get("lang", "") or ""))
    code = (str(element.get("text", "") or "") or str(element.get("content", "") or "")).replace("\r\n", "\n")
    trailing_newline = "" if code.endswith("\n") else "\n"
    return f"```{language}\n{code}{trailing_newline}```"


def _strip_markdown_to_plain_text(text: str) -> str:
    """Plain-text fallback: shared strip_markdown plus Feishu extras (blockquote, ~~, <u>, hr, CRLF)."""
    from gateway.platforms.helpers import strip_markdown
    plain = text.replace("\r\n", "\n")
    plain = _MARKDOWN_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2).strip()})", plain)
    plain = re.sub(r"^>\s?", "", plain, flags=re.MULTILINE)
    plain = re.sub(r"^\s*---+\s*$", "---", plain, flags=re.MULTILINE)
    plain = re.sub(r"~~([^~\n]+)~~", r"\1", plain)
    plain = re.sub(r"<u>([\s\S]*?)</u>", r"\1", plain)
    return strip_markdown(plain)


def _coerce_int(value: Any, default: Optional[int] = None, min_value: int = 0) -> Optional[int]:
    """Coerce value to int with optional default and minimum constraint."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= min_value else default


def _coerce_required_int(value: Any, default: int, min_value: int = 0) -> int:
    parsed = _coerce_int(value, default=default, min_value=min_value)
    return default if parsed is None else parsed


# --- Post payload builders and parsers ---

def _build_markdown_post_payload(content: str) -> str:
    rows = _build_markdown_post_rows(content)
    return json.dumps({"zh_cn": {"content": rows}}, ensure_ascii=False)


def _build_markdown_post_rows(content: str) -> list[list[dict[str, str]]]:
    """Build Feishu post rows, giving each fenced code block its own row.

    Feishu's `md` renderer can swallow trailing content when a fence sits inside one
    large element; splitting at real fence lines keeps surrounding prose visible.
    """
    if not content:
        return [[{"tag": "md", "text": ""}]]
    if "```" not in content:
        return [[{"tag": "md", "text": content}]]

    rows: list[list[dict[str, str]]] = []
    current: list[str] = []
    in_code_block = False

    def _flush_current() -> None:
        nonlocal current
        segment = "\n".join(current)
        if segment.strip():
            rows.append([{"tag": "md", "text": segment}])
        current = []

    for raw_line in content.splitlines():
        fence_re = _MARKDOWN_FENCE_CLOSE_RE if in_code_block else _MARKDOWN_FENCE_OPEN_RE
        is_fence = bool(fence_re.match(raw_line.strip()))
        if is_fence and not in_code_block:  # opening fence: prose before it becomes its own row
            _flush_current()
        current.append(raw_line)
        if is_fence:
            in_code_block = not in_code_block
            if not in_code_block:  # closing fence: the code block becomes its own row
                _flush_current()
    _flush_current()
    return rows or [[{"tag": "md", "text": content}]]


def parse_feishu_post_payload(
    payload: Any, *, mentions_map: Optional[dict[str, FeishuMentionRef]] = None,
) -> FeishuPostParseResult:
    resolved = _resolve_post_payload(payload)
    if not resolved:
        return FeishuPostParseResult(text_content=FALLBACK_POST_TEXT)
    image_keys: list[str] = []
    media_refs: list[FeishuPostMediaRef] = []
    parts: list[str] = []
    title = _normalize_feishu_text(str(resolved.get("title", "")).strip())
    if title:
        parts.append(title)
    for row in resolved.get("content", []) or []:
        if not isinstance(row, list):
            continue
        row_text = _normalize_feishu_text(
            "".join(_render_post_element(item, image_keys, media_refs, mentions_map) for item in row)
        )
        if row_text:
            parts.append(row_text)
    for entry in resolved.get("files", []) or []:
        if (
            not isinstance(entry, dict)
            or _to_boolean(entry.get("is_folder"))
            or not str(entry.get("file_key", "")).strip()
        ):
            continue
        placeholder = _render_post_element(
            {**entry, "tag": "file"}, image_keys, media_refs, mentions_map,
        )
        if placeholder:
            parts.append(placeholder)
    return FeishuPostParseResult(
        text_content="\n".join(parts).strip() or FALLBACK_POST_TEXT, image_keys=image_keys, media_refs=media_refs,
    )


def _resolve_post_payload(payload: Any) -> dict[str, Any]:
    direct = _to_post_payload(payload)
    if direct:
        return direct
    if not isinstance(payload, dict):
        return {}
    return _resolve_locale_payload(payload.get("post")) or _resolve_locale_payload(payload)


def _resolve_locale_payload(payload: Any) -> dict[str, Any]:
    direct = _to_post_payload(payload)
    if direct:
        return direct
    if not isinstance(payload, dict):
        return {}
    # Preferred locales first, then any locale that carries a content list.
    preferred = (payload.get(key) for key in _PREFERRED_LOCALES)
    for candidate in map(_to_post_payload, itertools.chain(preferred, payload.values())):
        if candidate:
            return candidate
    return {}


def _to_post_payload(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    content = candidate.get("content")
    if not isinstance(content, list):
        return {}
    files = candidate.get("files")
    return {
        "title": str(candidate.get("title", "") or ""),
        "content": content,
        "files": files if isinstance(files, list) else [],
    }


_STATIC_POST_TAGS = {"br": "\n", "hr": "\n\n---\n\n", "divider": "\n\n---\n\n"}


def _render_post_element(
    element: Any, image_keys: list[str], media_refs: list[FeishuPostMediaRef],
    mentions_map: Optional[dict[str, FeishuMentionRef]] = None,
) -> str:
    if isinstance(element, str):
        return element
    if not isinstance(element, dict):
        return ""

    tag = str(element.get("tag", "")).strip().lower()
    if tag in _STATIC_POST_TAGS:
        return _STATIC_POST_TAGS[tag]
    if tag == "text":
        return _render_text_element(element)
    if tag in {"code_block", "pre"}:
        return _render_code_block_element(element)
    if tag == "a":
        href = str(element.get("href", "")).strip()
        label = str(element.get("text", href) or "").strip()
        if not label:
            return ""
        escaped_label = _escape_markdown_text(label)
        return f"[{escaped_label}]({href})" if href else escaped_label
    if tag == "at":
        # <at>.user_id is a placeholder ("@_user_N" / "@_all"); mentions_map has the real ref.
        placeholder = str(element.get("user_id", "")).strip()
        if placeholder == "@_all":
            # The SDK sometimes omits @_all from top-level mentions; record it so callers see it.
            if mentions_map is not None and "@_all" not in mentions_map:
                mentions_map["@_all"] = FeishuMentionRef(is_all=True)
            return "@all"
        ref = (mentions_map or {}).get(placeholder)
        display_name = (ref.name or ref.open_id or "user") if ref is not None else (
            str(element.get("user_name", "")).strip() or "user"
        )
        return f"@{_escape_markdown_text(display_name)}"
    if tag in {"img", "image"}:
        image_key = str(element.get("image_key", "")).strip()
        if image_key and image_key not in image_keys:
            image_keys.append(image_key)
        alt = str(element.get("text", "")).strip() or str(element.get("alt", "")).strip()
        return f"[Image: {alt}]" if alt else "[Image]"
    if tag in {"media", "file", "audio", "video"}:
        file_key = str(element.get("file_key", "")).strip()
        names = (str(element.get(k, "")).strip() for k in ("file_name", "title", "text"))
        file_name = next((n for n in names if n), "")
        placeholder = f"[Attachment: {file_name}]" if file_name else "[Attachment]"
        if not file_key:
            return placeholder
        if not any(ref.file_key == file_key for ref in media_refs):
            media_refs.append(FeishuPostMediaRef(
                file_key=file_key, file_name=file_name, resource_type=tag if tag in {"audio", "video"} else "file",
            ))
            return placeholder
        return ""
    if tag in {"emotion", "emoji"}:
        label = str(element.get("text", "")).strip() or str(element.get("emoji_type", "")).strip()
        return f":{_escape_markdown_text(label)}:" if label else "[Emoji]"
    if tag == "code":
        code = str(element.get("text", "") or "") or str(element.get("content", "") or "")
        return _wrap_inline_code(code) if code else ""
    nested = (element.get(key) for key in ("text", "title", "content", "children", "elements"))
    return _join_nested_posts(nested, image_keys, media_refs, mentions_map)


def _join_nested_posts(values: Any, image_keys: Any, media_refs: Any, mentions_map: Any) -> str:
    parts = (_render_nested_post(item, image_keys, media_refs, mentions_map) for item in values)
    return " ".join(part for part in parts if part)


def _render_nested_post(
    value: Any, image_keys: list[str], media_refs: list[FeishuPostMediaRef],
    mentions_map: Optional[dict[str, FeishuMentionRef]] = None,
) -> str:
    if isinstance(value, str):
        return _escape_markdown_text(value)
    if isinstance(value, list):
        return _join_nested_posts(value, image_keys, media_refs, mentions_map)
    if isinstance(value, dict):
        direct = _render_post_element(value, image_keys, media_refs, mentions_map)
        return direct or _join_nested_posts(value.values(), image_keys, media_refs, mentions_map)
    return ""


# --- Message normalization ---

def normalize_feishu_message(
    *, message_type: str, raw_content: str, mentions: Optional[Sequence[Any]] = None,
    bot: _FeishuBotIdentity = _FeishuBotIdentity(),
) -> FeishuNormalizedMessage:
    normalized_type = str(message_type or "").strip().lower()
    payload = _load_feishu_payload(raw_content)
    mentions_map = _build_mentions_map(mentions, bot)

    if normalized_type == "text":
        text = str(payload.get("text", "") or "")
        # Feishu SDK sometimes omits @_all from the mentions payload even when
        # the text literal contains it (confirmed via im.v1.message.get).
        if "@_all" in text and "@_all" not in mentions_map:
            mentions_map["@_all"] = FeishuMentionRef(is_all=True)
        return FeishuNormalizedMessage(
            raw_type=normalized_type, text_content=_normalize_feishu_text(text, mentions_map),
            mentions=list(mentions_map.values()),
        )
    if normalized_type == "post":
        # The walker writes back to mentions_map if it encounters
        # <at user_id="@_all">, so reading .values() after parsing is enough.
        parsed_post = parse_feishu_post_payload(payload, mentions_map=mentions_map)
        return FeishuNormalizedMessage(
            raw_type=normalized_type, text_content=parsed_post.text_content,
            image_keys=list(parsed_post.image_keys), media_refs=list(parsed_post.media_refs),
            mentions=list(mentions_map.values()), relation_kind="post",
        )
    mention_refs = list(mentions_map.values())
    if normalized_type == "image":
        image_key = str(payload.get("image_key", "") or "").strip()
        alt_text = _normalize_feishu_text(
            str(payload.get("text", "") or "")
            or str(payload.get("alt", "") or "")
            or FALLBACK_IMAGE_TEXT,
            mentions_map,
        )
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=alt_text if alt_text != FALLBACK_IMAGE_TEXT else "",
            preferred_message_type="photo", image_keys=[image_key] if image_key else [],
            relation_kind="image", mentions=mention_refs,
        )
    if normalized_type in {"file", "audio", "media"}:
        media_ref = _build_media_ref_from_payload(payload, resource_type=normalized_type)
        return FeishuNormalizedMessage(
            raw_type=normalized_type, text_content="",
            preferred_message_type="audio" if normalized_type == "audio" else "document",
            media_refs=[media_ref] if media_ref.file_key else [], relation_kind=normalized_type,
            metadata={"placeholder_text": _attachment_placeholder(media_ref.file_name)},
            mentions=mention_refs,
        )
    if normalized_type == "merge_forward":
        return _normalize_merge_forward_message(payload)
    if normalized_type == "share_chat":
        return _normalize_share_chat_message(payload)
    if normalized_type in {"interactive", "card"}:
        return _normalize_interactive_message(normalized_type, payload)
    return FeishuNormalizedMessage(raw_type=normalized_type, text_content="")


def _load_feishu_payload(raw_content: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_content) if raw_content else {}
    except json.JSONDecodeError:
        return {"text": raw_content}
    return parsed if isinstance(parsed, dict) else {"content": parsed}


def _normalize_merge_forward_message(payload: dict[str, Any]) -> FeishuNormalizedMessage:
    title = _first_text_field(payload, "title", "summary", "preview", deep=("title", "summary", "preview", "description"))
    entries = _collect_forward_entries(payload)
    lines = ([title] if title else []) + entries[:8]
    return FeishuNormalizedMessage(
        raw_type="merge_forward", text_content="\n".join(lines).strip() or FALLBACK_FORWARD_TEXT,
        relation_kind="merge_forward", metadata={"entry_count": len(entries), "title": title},
    )


def _normalize_share_chat_message(payload: dict[str, Any]) -> FeishuNormalizedMessage:
    chat_name = _first_text_field(payload, "chat_name", "name", "title", deep=("chat_name", "name", "title"))
    share_id = _first_text_field(payload, "chat_id", "open_chat_id", "share_chat_id")
    lines = [f"Shared chat: {chat_name}" if chat_name else FALLBACK_SHARE_CHAT_TEXT]
    if share_id:
        lines.append(f"Chat ID: {share_id}")
    return FeishuNormalizedMessage(
        raw_type="share_chat", text_content="\n".join(lines), relation_kind="share_chat",
        metadata={"chat_id": share_id, "chat_name": chat_name},
    )


def _normalize_interactive_message(message_type: str, payload: dict[str, Any]) -> FeishuNormalizedMessage:
    card_payload = payload.get("card") if isinstance(payload.get("card"), dict) else payload
    title = _first_non_empty_text(
        _find_header_title(card_payload), payload.get("title"),
        _find_first_text(card_payload, keys=("title", "summary", "subtitle")),
    )
    actions = _collect_action_labels(card_payload)
    lines = ([title] if title else []) + [line for line in _collect_card_lines(card_payload) if line != title]
    if actions:
        lines.append(f"Actions: {', '.join(actions)}")
    return FeishuNormalizedMessage(
        raw_type=message_type,
        text_content="\n".join(lines[:12]).strip() or FALLBACK_INTERACTIVE_TEXT,
        relation_kind="interactive", metadata={"title": title, "actions": actions},
    )


# --- Content extraction utilities (card / forward / text walking) ---

def _collect_forward_entries(payload: dict[str, Any]) -> list[str]:
    candidates: list[Any] = []
    for key in ("messages", "items", "message_list", "records", "content"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    entries: list[str] = []
    for item in candidates:
        if not isinstance(item, dict):
            text = _normalize_feishu_text(str(item or ""))
            if text:
                entries.append(f"- {text}")
            continue
        sender = _first_text_field(item, "sender_name", "user_name", "sender", "name")
        nested_type = str(item.get("message_type", "") or item.get("msg_type", "")).strip().lower()
        if nested_type == "post":
            body = parse_feishu_post_payload(item.get("content") or item).text_content
        else:
            body = _first_text_field(
                item, "text", "summary", "preview", "content", deep=("text", "content", "summary", "preview", "title"),
            )
        body = _normalize_feishu_text(body)
        if sender and body:
            entries.append(f"- {sender}: {body}")
        elif body:
            entries.append(f"- {body}")
    return _unique_lines(entries)


def _collect_card_lines(payload: Any) -> list[str]:
    lines = _collect_text_segments(payload, in_rich_block=False)
    normalized = [_normalize_feishu_text(line) for line in lines]
    return _unique_lines([line for line in normalized if line])


def _collect_action_labels(payload: Any) -> list[str]:
    labels: list[str] = []
    for item in _walk_nodes(payload):
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag", "") or item.get("type", "")).strip().lower()
        if tag not in {"button", "select_static", "overflow", "date_picker", "picker"}:
            continue
        label = _first_text_field(item, "text", "name", "value", deep=("text", "content", "name", "value"))
        if label:
            labels.append(label)
    return _unique_lines(labels)


def _collect_text_segments(value: Any, *, in_rich_block: bool) -> list[str]:
    if isinstance(value, str):
        return [_normalize_feishu_text(value)] if in_rich_block else []
    if isinstance(value, list):
        return [seg for item in value for seg in _collect_text_segments(item, in_rich_block=in_rich_block)]
    if not isinstance(value, dict):
        return []
    tag = str(value.get("tag", "") or value.get("type", "")).strip().lower()
    next_in_rich_block = in_rich_block or tag in _RICH_BLOCK_TAGS
    segments: list[str] = []
    if next_in_rich_block:
        for key in _SUPPORTED_CARD_TEXT_KEYS:
            item = value.get(key)
            if isinstance(item, str) and _normalize_feishu_text(item):
                segments.append(_normalize_feishu_text(item))
    for key, item in value.items():
        if key not in _SKIP_TEXT_KEYS:
            segments.extend(_collect_text_segments(item, in_rich_block=next_in_rich_block))
    return segments


def _build_media_ref_from_payload(payload: dict[str, Any], *, resource_type: str) -> FeishuPostMediaRef:
    file_key = str(payload.get("file_key", "") or "").strip()
    file_name = _first_text_field(payload, "file_name", "title", "text")
    effective_type = resource_type if resource_type in {"audio", "video"} else "file"
    return FeishuPostMediaRef(file_key=file_key, file_name=file_name, resource_type=effective_type)


def _attachment_placeholder(file_name: str) -> str:
    normalized_name = _normalize_feishu_text(file_name)
    return f"[Attachment: {normalized_name}]" if normalized_name else FALLBACK_ATTACHMENT_TEXT


def _find_header_title(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    header = payload.get("header")
    if not isinstance(header, dict):
        return ""
    title = header.get("title")
    if isinstance(title, dict):
        return _first_non_empty_text(title.get("content"), title.get("text"), title.get("name"))
    return _normalize_feishu_text(str(title or ""))


def _find_first_text(payload: Any, *, keys: tuple[str, ...]) -> str:
    for node in _walk_nodes(payload):
        if not isinstance(node, dict):
            continue
        for key in keys:
            value = node.get(key)
            if isinstance(value, str):
                normalized = _normalize_feishu_text(value)
                if normalized:
                    return normalized
    return ""


def _walk_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_nodes(item)


def _first_non_empty_text(*values: Any) -> str:
    """First scalar (non-dict/list, non-None) value that normalizes to non-empty text."""
    for value in values:
        if value is None or isinstance(value, (dict, list)):
            continue
        normalized = _normalize_feishu_text(value if isinstance(value, str) else str(value))
        if normalized:
            return normalized
    return ""


def _first_text_field(payload: dict[str, Any], *keys: str, deep: tuple[str, ...] = ()) -> str:
    """``_first_non_empty_text`` over ``payload[key]`` for each key, then a deep ``_find_first_text``."""
    values = [payload.get(key) for key in keys]
    if deep:
        values.append(_find_first_text(payload, keys=deep))
    return _first_non_empty_text(*values)


# --- General text utilities ---

def _normalize_feishu_text(text: str, mentions_map: Optional[dict[str, FeishuMentionRef]] = None) -> str:
    def _sub(match: re.Match[str]) -> str:
        ref = (mentions_map or {}).get(match.group(0))
        return " " if ref is None else f"@{ref.name or ref.open_id or 'user'}"

    cleaned = _MENTION_PLACEHOLDER_RE.sub(_sub, text or "")
    cleaned = cleaned.replace("@_all", "@all")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = "\n".join(_WHITESPACE_RE.sub(" ", line).strip() for line in cleaned.split("\n"))
    cleaned = "\n".join(line for line in cleaned.split("\n") if line)
    cleaned = _MULTISPACE_RE.sub(" ", cleaned)
    return cleaned.strip()


def _unique_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for line in lines:
        if not line or line in seen:
            continue
        seen.add(line)
        unique.append(line)
    return unique


# --- Mention helpers ---

def _extract_mention_ids(mention: Any) -> tuple[str, str]:
    """(open_id, user_id): message.get gives a string id + id_type; events give a nested UserId object."""
    mention_id = getattr(mention, "id", None)
    if isinstance(mention_id, str):
        id_type = str(getattr(mention, "id_type", "") or "").lower()
        return (mention_id, "") if id_type == "open_id" else ("", mention_id) if id_type == "user_id" else ("", "")
    if mention_id is None:
        return "", ""
    return str(getattr(mention_id, "open_id", "") or ""), str(getattr(mention_id, "user_id", "") or "")


def _build_mentions_map(mentions: Optional[Sequence[Any]], bot: _FeishuBotIdentity) -> dict[str, FeishuMentionRef]:
    result: dict[str, FeishuMentionRef] = {}
    for mention in mentions or []:
        key = str(getattr(mention, "key", "") or "")
        if not key:
            continue
        if key == "@_all":
            result[key] = FeishuMentionRef(is_all=True)
            continue
        open_id, user_id = _extract_mention_ids(mention)
        name = str(getattr(mention, "name", "") or "").strip()
        is_self = bot.matches(open_id=open_id, user_id=user_id, name=name)
        result[key] = FeishuMentionRef(name=name, open_id=open_id, is_self=is_self)
    return result


def _build_mention_hint(mentions: Sequence[FeishuMentionRef]) -> str:
    parts: list[str] = []
    seen: set = set()
    for ref in mentions:
        if ref.is_self:
            continue
        signature = (ref.is_all, ref.open_id, ref.name)
        if signature in seen:
            continue
        seen.add(signature)
        if ref.is_all:
            parts.append("@all")
        elif ref.open_id:
            parts.append(f"{ref.name or 'unknown'} (open_id={ref.open_id})")
        else:
            parts.append(ref.name or "unknown")
    return f"[Mentioned: {', '.join(parts)}]" if parts else ""


def _strip_edge_self_mentions(text: str, mentions: Sequence[FeishuMentionRef]) -> str:
    # Leading self-mentions are stripped unconditionally (word-boundary so @Al can't eat @Alice);
    # trailing ones only when followed by whitespace/terminal punct so "don't @Bot again" survives.
    if not text:
        return text
    self_names = [f"@{ref.name or ref.open_id or 'user'}" for ref in mentions if ref.is_self]
    if not self_names:
        return text
    remaining = text.lstrip()
    while True:
        for nm in self_names:
            if not remaining.startswith(nm):
                continue
            after = remaining[len(nm):]
            if after and after[0] not in _MENTION_BOUNDARY_CHARS:
                continue
            remaining = after.lstrip()
            break
        else:
            break
    while True:
        i = len(remaining)
        while i > 0 and remaining[i - 1] in _TRAILING_TERMINAL_PUNCT:
            i -= 1
        body = remaining[:i]
        tail = remaining[i:]
        for nm in self_names:
            if body.endswith(nm):
                remaining = body[: -len(nm)].rstrip() + tail
                break
        else:
            return remaining


# --- Multiplex isolation for the lark_oapi WebSocket client ---
#
# ``lark_oapi.ws.client`` keeps the asyncio loop in a *module-level global* (``loop``), and
# Hermes monkey-patches ``websockets.connect`` on the shared module to inject ping settings.
# In multiplex mode N profiles each run a WS client on their own thread, so they overwrite
# each other's globals (last-write-wins): tasks land on a sibling's loop ("Future attached
# to a different loop") or a client binds the wrong loop and goes deaf. Fix: install
# process-wide, thread-dispatching shims exactly once —
#   * ``ws_client_module.loop`` becomes a proxy forwarding to the loop registered by the
#     *current thread* (all SDK reads happen on the loop-owning thread); unregistered
#     threads fall back to the SDK's original loop.
#   * ``websockets.connect`` becomes one dispatcher that merges the calling thread's
#     registered ping overrides, so profiles stop racing over the global patch.

# --------------------------------------------------------------------------- Multiplex isolation for the
# lark_oapi WebSocket client (#73779)
# --------------------------------------------------------------------------- ``lark_oapi.ws.client`` keeps
# the asyncio loop used by ``Client.start()`` and every coroutine it spawns in a *module-level global*
# (``loop``), and Hermes also monkey-patches ``websockets.connect`` on the shared ``websockets`` module to
# inject per-adapter ping settings. In multiplex mode every profile runs its own WS client on a dedicated
# thread, so the N threads overwrite each other's module globals (last-write-wins): a client ends up
# scheduling tasks on a sibling profile's loop ("Future attached to a different loop" crashes) or binds to
# the wrong loop at construction time and goes deaf from the start. The fix installs process-wide,
# thread-dispatching shims exactly once: * ``ws_client_module.loop`` becomes a proxy that forwards every
# attribute access to the loop registered by the *current thread*. All SDK reads of the global happen on the
# thread that owns the loop (``start()`` blocks in ``run_until_complete`` and every ``create_task`` callback
# runs on the loop's own thread), so each profile transparently sees its own loop. Threads that never
# registered one (single-profile installs, CLI) fall back to the SDK's original module loop. *
# ``websockets.connect`` becomes a single dispatcher that merges the per-thread ping overrides registered by
# the calling profile, so profiles no longer race over the global patch or restore each other's hooks while
# a sibling is still connected.
_WS_ISOLATION_LOCK = threading.Lock()
_WS_ISOLATION_INSTALLED = False
_ws_isolation_state = threading.local()  # per WS thread: .loop and .connect_kwargs


class _ThreadLocalLoopProxy:
    """Forwards attribute access to the current thread's registered loop."""

    def __init__(self, fallback: Any) -> None:
        self._fallback = fallback

    def _target(self) -> Any:
        return getattr(_ws_isolation_state, "loop", None) or self._fallback

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target(), name)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<ThreadLocalLoopProxy target={self._target()!r}>"


def _install_lark_ws_isolation(ws_client_module: Any) -> None:
    """Install the thread-dispatching shims once per process (idempotent)."""
    global _WS_ISOLATION_INSTALLED
    with _WS_ISOLATION_LOCK:
        if _WS_ISOLATION_INSTALLED:
            return
        ws_client_module.loop = _ThreadLocalLoopProxy(ws_client_module.loop)
        real_connect = ws_client_module.websockets.connect

        def _dispatch_connect(*args: Any, **kwargs: Any) -> Any:
            overrides = getattr(_ws_isolation_state, "connect_kwargs", None) or {}
            for key, value in overrides.items():
                kwargs.setdefault(key, value)
            return real_connect(*args, **kwargs)

        # Keep inspect.signature(websockets.connect) honest — the SDK probes it for ``proxy`` support.
        _dispatch_connect.__wrapped__ = real_connect
        _dispatch_connect.__name__ = getattr(real_connect, "__name__", "connect")
        ws_client_module.websockets.connect = _dispatch_connect

        original_receive_loop = ws_client_module.Client._receive_message_loop

        async def _receive_message_loop_exit_notify(self: Any) -> None:
            # The SDK schedules this coroutine right after the websocket handshake succeeded, so its
            # entry is the only in-thread proof that a (re)built link is actually up.
            on_link_up = getattr(_ws_isolation_state, "on_link_up", None)
            if on_link_up is not None:
                on_link_up()
            try:
                await original_receive_loop(self)
            except Exception:
                # ``Client.start()`` parks in ``run_until_complete(_select())``, which only returns
                # when this worker loop stops, and the receive loop runs as a bare ``create_task``
                # whose exception nobody retrieves — so every unrecoverable exit (reconnect ladder
                # disabled, or its ``ClientException``/``ServerUnreachableException`` re-raise)
                # left a deaf-but-ESTABLISHED socket whose executor future never completed and the
                # supervisor never rebuilt (#113662). Log the root cause here and stop the loop so
                # ``start()`` raises, the future completes and ``_supervise_websocket_thread`` fires.
                # A *normal* return means the SDK's own ladder already reconnected (it scheduled a
                # fresh receive loop) and must NOT stop the loop. Deliberate disconnects nil
                # ``_ws_client`` first, so the supervisor exits without restarting.
                adapter = getattr(_ws_isolation_state, "adapter", None)
                if adapter is None or getattr(adapter, "_running", True):
                    logger.exception(
                        "[Feishu] lark WS receive loop died; stopping the worker "
                        "loop so the supervisor can rebuild"
                    )
                else:
                    # ``disconnect()`` sent the CLOSE frame itself: the loop ending here is expected.
                    logger.debug("[Feishu] lark WS receive loop ended during disconnect", exc_info=True)
                asyncio.get_running_loop().stop()

        ws_client_module.Client._receive_message_loop = _receive_message_loop_exit_notify
        _WS_ISOLATION_INSTALLED = True


def _run_official_feishu_ws_client(ws_client: Any, adapter: Any) -> None:
    """Run the official Lark WS client in its own thread-local loop (see isolation notes above)."""
    import lark_oapi.ws.client as ws_client_module

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    adapter._ws_thread_loop = loop
    original_configure = getattr(ws_client, "_configure", None)

    def _apply_runtime_ws_overrides() -> None:
        try:
            ws_client._reconnect_nonce = adapter._ws_reconnect_nonce
            ws_client._reconnect_interval = adapter._ws_reconnect_interval
            if adapter._ws_ping_interval is not None:
                ws_client._ping_interval = adapter._ws_ping_interval
            # SDK observer (lark-oapi ``Client.on_reconnecting``, fired first thing in ``_reconnect()``):
            # on the live link ``_auto_reconnect`` is on, so the ladder runs *inside* the receive loop
            # and the thread never dies — without this the supervisor's ``retrying`` is never published.
            ws_client.on_reconnecting = _on_reconnecting
        except Exception:
            logger.debug("[Feishu] Failed to apply websocket runtime overrides", exc_info=True)

    connect_overrides = {
        key: value
        for key, value in (("ping_interval", adapter._ws_ping_interval), ("ping_timeout", adapter._ws_ping_timeout))
        if value is not None
    }
    adapter_loop = adapter._loop

    def _on_reconnecting() -> None:
        if adapter_loop is not None and not adapter_loop.is_closed():
            adapter_loop.call_soon_threadsafe(adapter._ws_link_retrying, ws_client)

    def _on_link_up() -> None:
        # Fired on the WS thread when the SDK scheduled a receive loop (handshake done); hop to the
        # adapter loop so the ``connected`` re-stamp after a supervisor rebuild runs where the adapter's
        # state lives.
        if adapter_loop is not None and not adapter_loop.is_closed():
            adapter_loop.call_soon_threadsafe(adapter._ws_link_up, ws_client)

    _install_lark_ws_isolation(ws_client_module)
    _ws_isolation_state.loop = loop
    _ws_isolation_state.connect_kwargs = connect_overrides
    _ws_isolation_state.on_link_up = _on_link_up
    _ws_isolation_state.adapter = adapter

    def _configure_with_overrides(conf: Any) -> Any:
        if original_configure is None:
            raise RuntimeError("Feishu _configure_with_overrides called but original_configure is None")
        result = original_configure(conf)
        _apply_runtime_ws_overrides()
        return result

    if original_configure is not None:
        ws_client._configure = _configure_with_overrides
    _apply_runtime_ws_overrides()
    try:
        ws_client.start()
    except Exception:
        pass
    finally:
        _ws_isolation_state.loop = None
        _ws_isolation_state.connect_kwargs = None
        _ws_isolation_state.on_link_up = None
        _ws_isolation_state.adapter = None
        if original_configure is not None:
            ws_client._configure = original_configure
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        for closer in (loop.stop, loop.close):
            try:
                closer()
            except Exception:
                pass
        adapter._ws_thread_loop = None


def _load_lark_oapi() -> bool:
    """Import and bind the Feishu SDK after an explicit connection request."""
    if FEISHU_AVAILABLE:
        return True
    with _lark_import_lock:
        if FEISHU_AVAILABLE:
            return True
        import importlib
        try:
            bound: dict[str, Any] = {"lark": importlib.import_module("lark_oapi")}
            for module_name, names in _LARK_SDK_IMPORTS:
                module = importlib.import_module(module_name)
                bound.update({name: getattr(module, name) for name in names})
            bound["FeishuWSClient"] = importlib.import_module("lark_oapi.ws").Client
        except (ImportError, AttributeError):
            return False
        bound["FEISHU_AVAILABLE"] = True
        globals().update(bound)
        return True


def feishu_deps_present() -> bool:
    """PASSIVE registry ``check_fn``: is lark-oapi installed? Must never install or import the SDK.

    Uses cheap importlib.metadata lookups; the real import is deferred to ``_load_lark_oapi``
    and the ACTIVE installer is ``check_feishu_requirements`` (``ensure_deps_fn``).

    Registry ``check_fn`` — called from status displays and config loading, so it must never install
    anything. The ACTIVE lazy-installer (``check_feishu_requirements``) is registered as ``ensure_deps_fn``
    and runs from ``create_adapter()`` when this returns False (#79812).
    """
    if FEISHU_AVAILABLE:
        return True
    try:
        from pm.extras import available
        return available("feishu")
    except Exception:  # pragma: no cover — defensive
        return False


def check_feishu_requirements() -> bool:
    """Ensure Feishu dependencies are installed without importing the SDK."""
    if FEISHU_AVAILABLE:
        return True

    from pm import ensure_import

    try:
        ensure_import("feishu")
        return True
    except Exception:
        return False


def _tenant_get_request(uri: str, *, queries: Optional[list[tuple[str, str]]] = None) -> Any:
    """Raw ``BaseRequest`` GET with the tenant access token (bot/v3 endpoints have no typed SDK request)."""
    builder = BaseRequest.builder().http_method(HttpMethod.GET).uri(uri)
    if queries is not None:
        builder = builder.queries(queries)
    return builder.token_types({AccessTokenType.TENANT}).build()


def _sdk_domain(domain_name: str) -> Any:
    return LARK_DOMAIN if domain_name == "lark" else FEISHU_DOMAIN


def _build_lark_client(app_id: str, app_secret: str, sdk_domain: Any) -> Any:
    return lark.Client.builder().app_id(app_id).app_secret(app_secret).domain(sdk_domain).log_level(lark.LogLevel.WARNING).build()


def _card_button(label: str, btn_type: str, value: dict[str, Any]) -> dict[str, Any]:
    return {"tag": "button", "text": {"tag": "plain_text", "content": label}, "type": btn_type, "value": value}


def _card(title: str, template: str, markdown: str, *, actions: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    """Wide interactive card: colored header + one markdown block (+ an optional button row)."""
    elements: list[dict[str, Any]] = [{"tag": "markdown", "content": markdown}]
    if actions is not None:
        elements.append({"tag": "action", "actions": actions})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"content": title, "tag": "plain_text"}, "template": template},
        "elements": elements,
    }


def _sdk_build(request_cls: Any, **fields: Any) -> Any:
    """``request_cls.builder().<field>(value)...build()``; SimpleNamespace when the SDK is unbound."""
    if request_cls is None:
        return SimpleNamespace(**fields)
    builder = request_cls.builder()
    for name, value in fields.items():
        builder = getattr(builder, name)(value)
    return builder.build()


class FeishuAdapter(FeishuChoiceCardsMixin, FeishuSyntheticEventsMixin, BasePlatformAdapter):
    """Feishu/Lark bot adapter."""
    # Answers /p/<profile>/... on the default listener for a served secondary (shared_ingress).
    serves_profile_prefix: bool = True

    supports_code_blocks = True  # Feishu renders fenced code blocks
    splits_long_messages = True  # send() chunks via truncate_message(MAX_MESSAGE_LENGTH)
    serial_receive_loop = True  # a chat's next message waits on its lock (_handle_message_with_guards)
    MAX_MESSAGE_LENGTH = 8000
    CHAT_LOCK_MAX_SIZE: int = 1000  # distinct chat IDs kept in _chat_locks before LRU eviction
    _SPLIT_THRESHOLD = 4000  # chunk near Feishu's ~4096-char client split → continuation almost certain

    # --- Lifecycle — init / settings / connect / disconnect ---
    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.FEISHU)
        self._settings = self._load_settings(config.extra or {})
        self._apply_settings(self._settings)
        self._client: Optional[Any] = None
        # Adapter-owned pool for blocking SDK calls, recreated on demand: a torn-down default
        # executor can no longer wedge sends with "Executor shutdown has been called".
        # See issue #10849.
        self._sdk_executor_lock = threading.Lock()
        self._sdk_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._sdk_executor_closing = False  # set on disconnect so a real teardown isn't resurrected
        self._ws_client = self._ws_future = self._ws_supervisor = self._ws_thread_loop = None
        self._ws_restart_backoff = 5.0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._webhook_runner = self._webhook_site = self._event_handler = None
        self._seen_message_ids: dict[str, float] = {}  # message_id → seen_at (time.time())
        self._seen_message_order: list[str] = []
        self._dedup_state_path = get_hermes_home() / "feishu_seen_message_ids.json"
        self._dedup_lock = threading.Lock()
        # Serializes the offloaded dedup-state flushes so two concurrent
        # inbound messages cannot land their writes out of order.
        self._dedup_persist_lock = asyncio.Lock()
        self._sender_name_cache: dict[str, tuple[str, float]] = {}  # sender_id → (name, expire_at)
        self._webhook_rate_counts: dict[str, tuple[int, float]] = {}  # rate_key → (count, window_start)
        self._webhook_anomaly_counts: dict[str, tuple[int, str, float]] = {}  # ip → (count, last_status, first_seen)
        self._card_action_tokens: dict[str, float] = {}  # token → first_seen_time
        # Inbound events that arrived before the loop was ready; one drainer thread replays them.
        self._pending_inbound_events: list[Any] = []
        self._pending_inbound_lock = threading.Lock()
        self._pending_drain_scheduled = False
        self._pending_inbound_max_depth = 1000  # cap queue; drop oldest beyond
        self._chat_locks: collections.OrderedDict[str, asyncio.Lock] = collections.OrderedDict()  # chat_id → lock (per-chat serial processing, LRU-bounded)
        self._chat_info_cache: dict[str, dict[str, Any]] = {}
        self._message_text_cache: OrderedDict[str, Optional[str]] = OrderedDict()
        self._app_lock_identity: Optional[str] = None
        self._text_batch_state = FeishuBatchState()
        self._pending_text_batches = self._text_batch_state.events
        self._pending_text_batch_tasks = self._text_batch_state.tasks
        self._pending_text_batch_counts = self._text_batch_state.counts
        self._media_batch_state = FeishuBatchState()
        self._pending_media_batches = self._media_batch_state.events
        self._pending_media_batch_tasks = self._media_batch_state.tasks
        # Button-card state: id → {session_key, message_id, chat_id}
        self._approval_state: dict[int, dict[str, str]] = {}
        self._approval_counter = prompt_id_counter()
        self._update_prompt_state: dict[int, dict[str, str]] = {}
        self._update_prompt_counter = prompt_id_counter()
        self._init_choice_card_state()
        # Reaction deletion needs the opaque reaction_id from create, cached per message_id.
        self._pending_processing_reactions: OrderedDict[str, str] = OrderedDict()
        self._load_seen_message_ids()

    @staticmethod
    def _load_settings(extra: dict[str, Any]) -> FeishuAdapterSettings:
        def _id_set(values: Any) -> set[str]:
            return {str(u).strip() for u in values if str(u).strip()}

        def _secret(name: str, default: str = "") -> str:
            return _get_scoped_secret(name, default).strip()

        def _extra_or_secret(key: str, env: str, default: str = "") -> str:
            return str(_shared_extra_or_secret(extra, key, env, default)).strip()

        _extra_or_env = _extra_or_secret

        raw_group_rules = extra.get("group_rules", {})
        group_rules: dict[str, FeishuGroupRule] = {}
        if isinstance(raw_group_rules, dict):
            for chat_id, rule_cfg in raw_group_rules.items():
                if not isinstance(rule_cfg, dict):
                    continue
                group_rules[str(chat_id)] = FeishuGroupRule(
                    policy=str(rule_cfg.get("policy", "open")).strip().lower(),
                    allowlist=_id_set(rule_cfg.get("allowlist", [])),
                    blacklist=_id_set(rule_cfg.get("blacklist", [])),
                    # Only override when explicitly set — missing vs false must not collapse.
                    require_mention=_to_boolean(rule_cfg["require_mention"]) if "require_mention" in rule_cfg else None,
                )

        # Scoped read: under multiplex a secondary profile's .env must govern its own adapter; yaml
        # feishu.allow_bots reaches it via ``extra`` (the env bridge is skipped under its scope).
        # See #86905.
        allow_bots = str(_extra_or_secret("allow_bots", "FEISHU_ALLOW_BOTS", "none") or "none").strip().lower()
        if allow_bots not in {"none", "mentions", "all"}:
            logger.warning(
                "[Feishu] Unknown allow_bots=%r, falling back to 'none'. Valid: none, mentions, all.",
                allow_bots,
            )
            allow_bots = "none"

        allow_all_dm = any(
            _secret(var).lower() in {"true", "1", "yes"} for var in ("FEISHU_ALLOW_ALL_USERS", "GATEWAY_ALLOW_ALL_USERS")
        )
        return FeishuAdapterSettings(
            app_id=_extra_or_secret("app_id", "FEISHU_APP_ID"),
            app_secret=_extra_or_secret("app_secret", "FEISHU_APP_SECRET"),
            domain_name=_extra_or_env("domain", "FEISHU_DOMAIN", "feishu").lower(),
            connection_mode=_extra_or_env("connection_mode", "FEISHU_CONNECTION_MODE", "websocket").lower(),
            encrypt_key=_extra_or_secret("encrypt_key", "FEISHU_ENCRYPT_KEY"),
            verification_token=_extra_or_secret("verification_token", "FEISHU_VERIFICATION_TOKEN"),
            group_policy=_secret("FEISHU_GROUP_POLICY", "allowlist").lower(),
            allowed_group_users=frozenset(_id_set(_get_scoped_secret("FEISHU_ALLOWED_USERS", "").split(","))),
            bot_open_id=_secret("FEISHU_BOT_OPEN_ID"),
            bot_user_id=_secret("FEISHU_BOT_USER_ID"),
            bot_name=_secret("FEISHU_BOT_NAME"),
            dedup_cache_size=max(32, env_int("HERMES_FEISHU_DEDUP_CACHE_SIZE", _DEFAULT_DEDUP_CACHE_SIZE)),
            text_batch_delay_seconds=env_float("HERMES_FEISHU_TEXT_BATCH_DELAY_SECONDS", _DEFAULT_TEXT_BATCH_DELAY_SECONDS),
            text_batch_split_delay_seconds=env_float("HERMES_FEISHU_TEXT_BATCH_SPLIT_DELAY_SECONDS", 2.0),
            text_batch_max_messages=max(1, env_int("HERMES_FEISHU_TEXT_BATCH_MAX_MESSAGES", _DEFAULT_TEXT_BATCH_MAX_MESSAGES)),
            text_batch_max_chars=max(1, env_int("HERMES_FEISHU_TEXT_BATCH_MAX_CHARS", _DEFAULT_TEXT_BATCH_MAX_CHARS)),
            media_batch_delay_seconds=env_float("HERMES_FEISHU_MEDIA_BATCH_DELAY_SECONDS", _DEFAULT_MEDIA_BATCH_DELAY_SECONDS),
            webhook_host=_extra_or_env("webhook_host", "FEISHU_WEBHOOK_HOST", _DEFAULT_WEBHOOK_HOST),
            webhook_port=int(extra.get("webhook_port") or _get_scoped_secret("FEISHU_WEBHOOK_PORT", str(_DEFAULT_WEBHOOK_PORT))),
            webhook_path=_extra_or_env("webhook_path", "FEISHU_WEBHOOK_PATH", _DEFAULT_WEBHOOK_PATH) or _DEFAULT_WEBHOOK_PATH,
            ws_reconnect_nonce=_coerce_required_int(extra.get("ws_reconnect_nonce"), default=30, min_value=0),
            ws_reconnect_interval=_coerce_required_int(extra.get("ws_reconnect_interval"), default=120, min_value=1),
            ws_ping_interval=_coerce_int(extra.get("ws_ping_interval"), default=None, min_value=1),
            ws_ping_timeout=_coerce_int(extra.get("ws_ping_timeout"), default=None, min_value=1),
            admins=frozenset(_id_set(extra.get("admins", []))),
            default_group_policy=str(extra.get("default_group_policy", "")).strip().lower(),
            group_rules=group_rules, allow_bots=allow_bots, allow_all_dm=allow_all_dm,
            require_mention=_to_boolean(extra.get("require_mention", _get_scoped_secret("FEISHU_REQUIRE_MENTION", "true"))),
        )

    def _apply_settings(self, settings: FeishuAdapterSettings) -> None:
        # Every settings field is mirrored as ``self._<field>``; the three below need coercion.
        for name in FeishuAdapterSettings.__dataclass_fields__:
            setattr(self, f"_{name}", getattr(settings, name))
        self._allowed_group_users = set(settings.allowed_group_users)
        self._admins = set(settings.admins)
        self._default_group_policy = settings.default_group_policy or settings.group_policy

    def _build_event_handler(self) -> Any:
        if EventDispatcherHandler is None:
            return None
        return (
            EventDispatcherHandler.builder(self._encrypt_key, self._verification_token)
            .register_p2_im_message_message_read_v1(self._on_message_read_event)
            .register_p2_im_message_receive_v1(self._on_message_event)
            .register_p2_im_message_reaction_created_v1(lambda d: self._on_reaction_event("im.message.reaction.created_v1", d))
            .register_p2_im_message_reaction_deleted_v1(lambda d: self._on_reaction_event("im.message.reaction.deleted_v1", d))
            .register_p2_card_action_trigger(self._on_card_action_trigger)
            .register_p2_im_chat_member_bot_added_v1(self._on_bot_added_to_chat)
            .register_p2_im_chat_member_bot_deleted_v1(self._on_bot_removed_from_chat)
            .register_p2_im_chat_access_event_bot_p2p_chat_entered_v1(self._on_p2p_chat_entered)
            .register_p2_im_message_recalled_v1(self._on_message_recalled)
            .register_p2_customized_event("drive.notice.comment_add_v1", self._on_drive_comment_event)
            .register_p2_customized_event("vc.bot.meeting_invited_v1", self._on_meeting_invited_event)
            .build()
        )

    def _get_sdk_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Adapter-owned executor; recreated after an *external* shutdown, never after our own close.

        Recreates the pool if it was never built or was shut down by an *external* teardown of the loop's
        default executor, so that can no longer permanently wedge sends (#10849). Refuses to resurrect once
        the adapter itself is closing — a real disconnect/shutdown stays shut.
        """
        lock = getattr(self, "_sdk_executor_lock", None)  # bare adapters (tests) may lack __init__ state
        if lock is None:
            lock = self._sdk_executor_lock = threading.Lock()
        with lock:
            if getattr(self, "_sdk_executor_closing", False):
                raise RuntimeError("Feishu adapter is shutting down; SDK executor unavailable")
            executor = getattr(self, "_sdk_executor", None)
            if executor is None or getattr(executor, "_shutdown", False):
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix="hermes-feishu-sdk")
                self._sdk_executor = executor
            return executor

    async def _run_blocking(self, func, *args):
        """Run a blocking Feishu SDK call on the adapter-owned thread pool.

        ``copy_context().run`` mirrors ``asyncio.to_thread``: the worker sees the caller's
        profile HERMES_HOME override / secret scope (multiplexed dedup flush, thread lookup).
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._get_sdk_executor(), contextvars.copy_context().run, func, *args)

    def _shutdown_sdk_executor(self) -> None:
        """Stop the adapter-owned SDK executor without touching the loop default."""
        lock = getattr(self, "_sdk_executor_lock", None)
        if lock is None:
            return
        with lock:
            self._sdk_executor_closing = True
            executor = getattr(self, "_sdk_executor", None)
            self._sdk_executor = None
        if executor is None:
            return
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=False)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Feishu/Lark."""
        self._sdk_executor_closing = False  # re-arm the SDK executor after a prior disconnect
        if not self._app_id or not self._app_secret:
            logger.error("[Feishu] FEISHU_APP_ID or FEISHU_APP_SECRET not set")
            return False
        if self._connection_mode not in {"websocket", "webhook"}:
            logger.error(
                "[Feishu] Unsupported FEISHU_CONNECTION_MODE=%s. Supported modes: websocket, webhook.",
                self._connection_mode,
            )
            return False
        if self._connection_mode == "webhook" and not (self._verification_token or self._encrypt_key):
            logger.error("[Feishu] Webhook mode requires FEISHU_VERIFICATION_TOKEN or FEISHU_ENCRYPT_KEY.")
            return False
        if not await asyncio.to_thread(_load_lark_oapi):
            logger.error("[Feishu] lark-oapi not installed")
            return False

        try:
            self._app_lock_identity = self._app_id
            acquired, existing = acquire_scoped_lock(
                _FEISHU_APP_LOCK_SCOPE, self._app_lock_identity, metadata={"platform": self.platform.value},
            )
            if not acquired:
                owner_pid = existing.get("pid") if isinstance(existing, dict) else None
                message = (
                    "Another local Hermes gateway is already using this Feishu app_id"
                    + (f" (PID {owner_pid})." if owner_pid else ".")
                    + " Stop the other gateway before starting a second Feishu websocket client."
                )
                logger.error("[Feishu] %s", message)
                self._set_fatal_error("feishu_app_lock", message, retryable=False)
                return False

            self._loop = asyncio.get_running_loop()
            await self._connect_with_retry()
            if self._connection_mode == "websocket":
                # The WS thread can die without any external signal; keep a watcher alive.
                self._ws_supervisor = asyncio.ensure_future(self._supervise_websocket_thread())
            self._mark_connected()
            logger.info("[Feishu] Connected in %s mode (%s)", self._connection_mode, self._domain_name)
            # Plugin-registered native handlers (lark_oapi client).
            self._wire_plugin_handlers(self._client)
            return True
        except Exception as exc:
            await self._release_app_lock()
            message = f"Feishu startup failed: {exc}"
            self._set_fatal_error("feishu_connect_error", message, retryable=True)
            logger.error("[Feishu] Failed to connect: %s", exc, exc_info=True)
            return False

    async def disconnect(self) -> None:
        """Disconnect from Feishu/Lark."""
        self._running = False
        if self._ws_supervisor is not None:
            self._ws_supervisor.cancel()
            self._ws_supervisor = None
        await self._cancel_pending_tasks(self._pending_text_batch_tasks)
        await self._cancel_pending_tasks(self._pending_media_batch_tasks)
        self._reset_batch_buffers()
        # ``_disable_websocket_auto_reconnect()`` nils ``_ws_client`` — capture first.
        # Send a WebSocket CLOSE frame to Feishu BEFORE tearing down the thread loop. Without this, Feishu's
        # server never learns the connection is dead and continues routing messages to the stale endpoint —
        # the channel goes silent until the server-side CLOSE-WAIT expires (minutes to hours). See issue
        # #10202.
        ws_client = self._ws_client
        ws_thread_loop = self._ws_thread_loop
        self._disable_websocket_auto_reconnect()
        await self._stop_webhook_server()
        await self._teardown_ws_thread(ws_client, ws_thread_loop)
        self._ws_future = None
        self._ws_thread_loop = None
        self._loop = None
        self._event_handler = None
        self._shutdown_sdk_executor()
        self._persist_seen_message_ids()
        await self._release_app_lock()
        self._mark_disconnected()
        logger.info("[Feishu] Disconnected")

    async def _teardown_ws_thread(self, ws_client: Any, ws_thread_loop: Any) -> None:
        """CLOSE frame → cancel the WS thread's tasks → wait for the thread future."""
        # Send the CLOSE frame BEFORE tearing down the thread loop; otherwise Feishu keeps
        # routing to the stale endpoint until server-side CLOSE-WAIT expires (minutes to hours).
        loop_alive = ws_thread_loop is not None and not ws_thread_loop.is_closed()
        if ws_client is not None and loop_alive and hasattr(ws_client, "_disconnect"):
            try:
                future = asyncio.run_coroutine_threadsafe(ws_client._disconnect(), ws_thread_loop)
                # A CLOSE frame is one control frame; if 5s isn't enough the link is already wedged.
                await asyncio.wait_for(asyncio.wrap_future(future), timeout=5.0)
                logger.debug("[Feishu] Sent WebSocket CLOSE frame to Feishu")
            except TimeoutError:
                logger.warning(
                    "[Feishu] CLOSE frame not acknowledged within 5s — "
                    "Feishu may briefly route messages to the stale "
                    "connection until server-side timeout"
                )
            except Exception as exc:
                logger.debug("[Feishu] Could not send WebSocket CLOSE frame: %s", exc, exc_info=True)

        if loop_alive:
            logger.debug("[Feishu] Cancelling websocket thread tasks and stopping loop")

            def cancel_all_tasks() -> None:
                tasks = [t for t in asyncio.all_tasks(ws_thread_loop) if not t.done()]
                logger.debug("[Feishu] Found %d pending tasks in websocket thread", len(tasks))
                for task in tasks:
                    task.cancel()
                ws_thread_loop.call_later(0.1, ws_thread_loop.stop)

            ws_thread_loop.call_soon_threadsafe(cancel_all_tasks)

        ws_future = self._ws_future
        if ws_future is not None:
            try:
                logger.debug("[Feishu] Waiting for websocket thread to exit (timeout=10s)")
                await asyncio.wait_for(asyncio.shield(ws_future), timeout=10.0)
                logger.debug("[Feishu] Websocket thread exited cleanly")
            except TimeoutError:
                logger.warning("[Feishu] Websocket thread did not exit within 10s - may be stuck")
            except asyncio.CancelledError:
                logger.debug("[Feishu] Websocket thread cancelled during disconnect")
            except Exception as exc:
                logger.debug("[Feishu] Websocket thread exited with error: %s", exc, exc_info=True)

    async def _cancel_pending_tasks(self, tasks: dict[str, asyncio.Task]) -> None:
        pending = [task for task in tasks.values() if task and not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        tasks.clear()

    def _reset_batch_buffers(self) -> None:
        self._pending_text_batches.clear()
        self._pending_text_batch_counts.clear()
        self._pending_media_batches.clear()

    def _disable_websocket_auto_reconnect(self) -> None:
        if self._ws_client is None:
            return
        try:
            self._ws_client._auto_reconnect = False
        except Exception:
            pass
        finally:
            self._ws_client = None

    async def _stop_webhook_server(self) -> None:
        if self._webhook_runner is None:
            return
        try:
            await self._webhook_runner.cleanup()
        finally:
            self._webhook_runner = None
            self._webhook_site = None

    # --- Outbound — send / edit / send_image / send_voice / … ---
    async def send(
        self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a Feishu message."""
        if not self._client:
            return SendResult(success=False, error="Not connected")

        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, self.MAX_MESSAGE_LENGTH)
        # Decide markdown-vs-text once for the whole message: a chunk of a long
        # markdown reply may be plain prose that fails the per-chunk regex and would
        # otherwise render as literal ``**bold`` / fences while other chunks render.
        # Lock the markdown decision at the whole-message level so every chunk consistently uses ``post``.
        # See #26841.
        prefer_post = bool(_MARKDOWN_HINT_RE.search(formatted))

        async def _send_plain(chunk: str) -> Any:
            return await self._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="text",
                payload=json.dumps({"text": _strip_markdown_to_plain_text(chunk)}, ensure_ascii=False),
                reply_to=reply_to,
                metadata=metadata,
            )

        async def _send_one(chunk: str) -> SendResult:
            msg_type, payload = self._build_outbound_payload(chunk, prefer_post=prefer_post)
            try:
                response = await self._feishu_send_with_retry(
                    chat_id=chat_id, msg_type=msg_type, payload=payload, reply_to=reply_to, metadata=metadata,
                )
            except Exception as exc:
                if msg_type != "post" or not _POST_CONTENT_INVALID_RE.search(str(exc)):
                    raise
                logger.warning("[Feishu] Invalid post payload rejected by API; falling back to plain text")
                response = await _send_plain(chunk)
            if (
                msg_type == "post"
                and not self._response_succeeded(response)
                and _POST_CONTENT_INVALID_RE.search(str(getattr(response, "msg", "") or ""))
            ):
                logger.warning("[Feishu] Post payload rejected by API response; falling back to plain text")
                response = await _send_plain(chunk)
            return self._finalize_send_result(response, "send failed")

        try:  # stops at the first refused chunk and marks a partial delivery: a later success never masks it
            return await send_chunks(chunks, _send_one)
        except Exception as exc:
            logger.error("[Feishu] Send error: %s", (shown := describe_proxy_error(exc, env_proxy_for_redaction(_onboard_open_base_url(self._domain_name)))) or exc, exc_info=not shown)
            return SendResult(success=False, error=shown or str(exc))

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False) -> SendResult:
        """Edit a previously sent Feishu text/post message."""
        if not self._client:
            return SendResult(success=False, error="Not connected")

        content = self.format_message(content)

        async def _update(msg_type: str, payload: str) -> SendResult:
            body = self._build_update_message_body(msg_type=msg_type, content=payload)
            request = self._build_update_message_request(message_id=message_id, request_body=body)
            response = await self._run_blocking(self._client.im.v1.message.update, request)
            return self._finalize_send_result(response, "update failed")

        try:
            msg_type, payload = self._build_outbound_payload(content)
            result = await _update(msg_type, payload)
            if not result.success and msg_type == "post" and _POST_CONTENT_INVALID_RE.search(result.error or ""):
                logger.warning("[Feishu] Invalid post update payload rejected by API; falling back to plain text")
                result = await _update(
                    "text", json.dumps({"text": _strip_markdown_to_plain_text(content)}, ensure_ascii=False),
                )
            if result.success:
                result.message_id = message_id
            return result
        except Exception as exc:
            logger.error("[Feishu] Failed to edit message %s: %s", message_id, (shown := describe_proxy_error(exc, env_proxy_for_redaction(_onboard_open_base_url(self._domain_name)))) or exc, exc_info=not shown)
            return SendResult(success=False, error=shown or str(exc))

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a bot-posted message (used by stream-consumer preview cleanup)."""
        if not self._client or not message_id:
            return False
        try:
            request = self._build_delete_message_request(message_id)
            response = await self._run_blocking(self._client.im.v1.message.delete, request)
            if self._response_succeeded(response):
                return True
            logger.debug(
                "[Feishu] Delete of message %s rejected: code=%s msg=%s",
                message_id, getattr(response, "code", None), getattr(response, "msg", None),
            )
            return False
        except Exception:
            logger.debug("[Feishu] Failed to delete message %s", message_id, exc_info=True)
            return False

    # Template attrs for the shared _format_exec_approval core. The card
    # header carries the title, so the text core starts at the code fence.
    _EA_HEADER = ""
    _EA_CMD_BUDGET = 3000

    # Resolved per call (not at class-body time) so the active language applies.
    @property
    def _EA_REASON_LABEL(self) -> str:
        return f"**{t('gateway.exec_approval.reason_label')}:** "

    @property
    def _EA_SMART_DENY_LINE(self) -> str:
        line = t("gateway.exec_approval.smart_deny_line")
        label, sep, rest = line.partition(":")
        return "\n\n" + (f"**{label}{sep}**{rest}" if sep else line)

    @property
    def _EA_ACTION_LABELS(self) -> dict[str, str]:
        return {"once": "✅ " + t("gateway.exec_approval.action_once"),
                "session": "✅ " + t("platform.feishu.approval.action_session"),
                "always": "✅ " + t("platform.feishu.approval.action_always"),
                "deny": "❌ " + t("gateway.exec_approval.action_deny")}
    _EA_CARD_ACTIONS = {"once": "approve_once", "session": "approve_session", "always": "approve_always", "deny": "deny"}

    async def _send_exec_approval_prompt(self, prompt: ExecApprovalPrompt) -> SendResult:
        """Approval-button card; ``hermes_action`` in each button value lets the click callback
        route to ``resolve_gateway_approval()`` and unblock the waiting agent thread."""
        if not self._client:
            return SendResult(success=False, error="Not connected")
        try:
            approval_id = next(self._approval_counter)
            actions = [
                _card_button(label, style or "default",
                             {"hermes_action": self._EA_CARD_ACTIONS[choice], "approval_id": approval_id})
                for label, choice, style in prompt.actions]
            card = _card(f"⚠️ {t('gateway.exec_approval.header')}", "orange", prompt.text, actions=actions)
            return await self._send_interactive_card(
                prompt.chat_id, card, prompt.metadata, "send_exec_approval failed",
                state_map=self._approval_state, state_id=approval_id, session_key=prompt.session_key,
                extra_state={"body": prompt.text.replace(self._ea_deadline_line(), ""),  # deadline is moot once resolved
                             "request_id": prompt.request_id or ""},
            )
        except Exception as exc:
            logger.warning("[Feishu] send_exec_approval failed: %s", shown := describe_proxy_error(exc, env_proxy_for_redaction(_onboard_open_base_url(self._domain_name))) or str(exc))
            return SendResult(success=False, error=shown)

    async def _send_interactive_card(
        self, chat_id: str, card: dict[str, Any], metadata: Optional[dict[str, Any]], failure_message: str, *,
        state_map: dict[int, dict[str, str]], state_id: int, session_key: str, extra_state: Optional[dict[str, str]] = None,
    ) -> SendResult:
        """Send a button card and, on success, remember where it went (plus *extra_state*) so a click can be validated."""
        response = await self._feishu_send_with_retry(
            chat_id=chat_id, msg_type="interactive", payload=json.dumps(card, ensure_ascii=False),
            reply_to=None, metadata=metadata,
        )
        result = self._finalize_send_result(response, failure_message)
        if result.success:
            state_map[state_id] = {
                "session_key": session_key,
                "message_id": result.message_id or "",
                "chat_id": chat_id,
                **(extra_state or {}),
            }
        return result

    @staticmethod
    def _build_update_prompt_card(*, prompt: str, default: str, prompt_id: int) -> dict[str, Any]:
        default_hint = t("platform.feishu.update_prompt.default_hint", default=default) if default else ""

        def _btn(label: str, answer: str, btn_type: str) -> dict:
            return _card_button(label, btn_type, {"hermes_update_prompt_action": answer, "update_prompt_id": prompt_id})

        actions = [_btn(t("platform.feishu.update_prompt.answer_yes"), "y", "primary"),
                   _btn(t("platform.feishu.update_prompt.answer_no"), "n", "danger")]
        return _card(t("platform.feishu.update_prompt.title"), "orange", f"{prompt}{default_hint}", actions=actions)

    async def send_update_prompt(
        self, chat_id: str, prompt: str, default: str = "", session_key: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send an interactive update prompt with Yes/No buttons."""
        if not self._client:
            return SendResult(success=False, error="Not connected")
        try:
            prompt_id = next(self._update_prompt_counter)
            card = self._build_update_prompt_card(prompt=prompt, default=default, prompt_id=prompt_id)
            return await self._send_interactive_card(
                chat_id, card, metadata, "send_update_prompt failed",
                state_map=self._update_prompt_state, state_id=prompt_id, session_key=session_key,
            )
        except Exception as exc:
            logger.warning("[Feishu] send_update_prompt failed: %s", shown := describe_proxy_error(exc, env_proxy_for_redaction(_onboard_open_base_url(self._domain_name))) or str(exc))
            return SendResult(success=False, error=shown)

    @staticmethod
    def _build_resolved_approval_card(*, choice: str, user_name: str, body: str | None = None) -> dict[str, Any]:
        """Raw card JSON shown in place of the buttons once an approval is resolved; the request (*body*) stays."""
        icon = "❌" if choice == "deny" else "✅"
        label = t(_APPROVAL_LABEL_KEYS.get(choice, "platform.feishu.approval.resolved_fallback"))
        decided = t("platform.feishu.approval.resolved_by_user", icon=icon, label=label, user=user_name)
        return _card(f"{icon} {label}", "red" if choice == "deny" else "green", f"{body}\n\n{decided}" if body else decided)

    @staticmethod
    def _build_resolved_update_prompt_card(*, answer: str, user_name: str) -> dict[str, Any]:
        yes = answer == "y"
        title = t("platform.feishu.update_prompt.answered_title", icon="✅" if yes else "❌",
                  answer=t("platform.feishu.update_prompt.word_yes" if yes else "platform.feishu.update_prompt.word_no"))
        return _card(title, "green" if yes else "red", t("platform.feishu.update_prompt.answered_by", user=user_name))

    @staticmethod
    def _write_update_prompt_response(answer: str) -> None:
        response_path = get_hermes_home() / ".update_response"
        tmp_path = response_path.with_suffix(".tmp")
        tmp_path.write_text(answer, encoding="utf-8")
        tmp_path.replace(response_path)

    async def send_voice(
        self, chat_id: str, audio_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None, is_voice: Optional[bool] = None, **kwargs,
    ) -> SendResult:
        """Native voice message (Feishu only accepts Opus): non-opus audio is transcoded via ffmpeg
        first; without ffmpeg the original file goes out as a file attachment. ``is_voice=False``
        (an audio MEDIA file without [[audio_as_voice]]: music, a sound effect) is a file
        attachment under its real name, never transcoded into a voice note."""
        if is_voice is False:
            return await self.send_document(
                chat_id, audio_path, caption=caption, reply_to=reply_to, metadata=metadata)
        transcoded_path: Optional[str] = None
        ext = Path(audio_path).suffix.lower()
        if ext not in _FEISHU_OPUS_UPLOAD_EXTENSIONS:
            from gateway.platforms.base import transcode_to_ogg_opus
            transcoded_path = await asyncio.to_thread(transcode_to_ogg_opus, audio_path)
            if transcoded_path:
                audio_path = transcoded_path
        try:
            return await self._send_uploaded_file_message(
                chat_id=chat_id, file_path=audio_path, reply_to=reply_to, metadata=metadata,
                caption=caption, outbound_message_type="audio",
            )
        finally:
            if transcoded_path:
                try:
                    os.unlink(transcoded_path)
                except OSError:
                    pass

    async def send_document(
        self, chat_id: str, file_path: str, caption: Optional[str] = None, file_name: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        """Send a document/file attachment to Feishu."""
        return await self._send_uploaded_file_message(
            chat_id=chat_id, file_path=file_path, reply_to=reply_to, metadata=metadata,
            caption=caption, file_name=file_name,
        )

    async def send_video(
        self, chat_id: str, video_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        """Send a video file to Feishu."""
        return await self._send_uploaded_file_message(
            chat_id=chat_id, file_path=video_path, reply_to=reply_to, metadata=metadata,
            caption=caption, outbound_message_type="media",
        )

    async def send_image_file(
        self, chat_id: str, image_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        """Send a local image file to Feishu."""
        if not self._client:
            return SendResult(success=False, error="Not connected")
        if not os.path.exists(image_path):
            return SendResult(success=False, error=f"Image file not found: {image_path}")
        try:
            import io as _io
            image_file = _io.BytesIO(await asyncio.to_thread(Path(image_path).read_bytes))  # lark's MultipartEncoder needs .name and .tell()
            image_file.name = os.path.basename(image_path)
            body = self._build_image_upload_body(image_type=_FEISHU_IMAGE_UPLOAD_TYPE, image=image_file)
            request = self._build_image_upload_request(body)
            upload_response = await self._run_blocking(self._client.im.v1.image.create, request)
            image_key = self._extract_response_field(upload_response, "image_key")
            if not image_key:
                return self._response_error_result(
                    upload_response, default_message="image upload failed",
                    override_error="Feishu image upload missing image_key",
                )
            message_response = await self._send_uploaded_key(
                chat_id=chat_id, reply_to=reply_to, metadata=metadata, caption=caption,
                key_msg_type="image", key_payload={"image_key": image_key},
                media_tag={"tag": "img", "image_key": image_key},
            )
            return self._finalize_send_result(message_response, "image send failed")
        except Exception as exc:
            logger.error("[Feishu] Failed to send image %s: %s", image_path, (shown := describe_proxy_error(exc, env_proxy_for_redaction(_onboard_open_base_url(self._domain_name)))) or exc, exc_info=not shown)
            return SendResult(success=False, error=shown or str(exc))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Feishu bot API does not expose a typing indicator."""
        return

    async def send_image(
        self, chat_id: str, image_url: str, caption: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Download a remote image then send it through the native Feishu image flow."""
        try:
            image_path = await self._download_remote_image(image_url)
        except Exception as exc:
            logger.error("[Feishu] Failed to download image %s: %s", image_url, (shown := describe_proxy_error(exc, env_proxy_for_redaction(image_url))) or exc, exc_info=not shown)
            return await super().send_image(
                chat_id=chat_id, image_url=image_url, caption=caption, reply_to=reply_to, metadata=metadata,
            )
        return await self.send_image_file(
            chat_id=chat_id, image_path=image_path, caption=caption, reply_to=reply_to, metadata=metadata,
        )

    async def send_animation(
        self, chat_id: str, animation_url: str, caption: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Feishu has no native GIF bubble; degrade to a downloadable file."""
        try:
            file_path, file_name = await self._download_remote_document(
                animation_url, default_ext=".gif", preferred_name="animation.gif",
            )
        except Exception as exc:
            logger.error("[Feishu] Failed to download animation %s: %s", animation_url, (shown := describe_proxy_error(exc, env_proxy_for_redaction(animation_url))) or exc, exc_info=not shown)
            return await super().send_animation(
                chat_id=chat_id, animation_url=animation_url, caption=caption, reply_to=reply_to, metadata=metadata,
            )
        degraded_caption = self.warning_text(
            t("platform.feishu.media.gif_downgraded_caption", caption=caption) if caption
            else t("platform.feishu.media.gif_downgraded"), caption)
        return await self.send_document(
            chat_id=chat_id, file_path=file_path, file_name=file_name, caption=degraded_caption,
            reply_to=reply_to, metadata=metadata,
        )

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Return real chat metadata from Feishu when available."""
        fallback = {"chat_id": chat_id, "name": chat_id, "type": "dm"}
        if not self._client:
            return fallback
        cached = self._chat_info_cache.get(chat_id)
        if cached is not None:
            return dict(cached)
        try:
            request = self._build_get_chat_request(chat_id)
            response = await self._run_blocking(self._client.im.v1.chat.get, request)
            if not response or getattr(response, "success", lambda: False)() is False:
                code = getattr(response, "code", "unknown")
                msg = getattr(response, "msg", "chat lookup failed")
                logger.warning("[Feishu] Failed to get chat info for %s: [%s] %s", chat_id, code, msg)
                return fallback
            data = getattr(response, "data", None)
            raw_chat_type = str(getattr(data, "chat_type", "") or "").strip().lower()
            # chat.get's chat_type is private/public; the DM marker is chat_mode == "p2p".
            raw_chat_mode = str(getattr(data, "chat_mode", "") or "").strip().lower()
            info = {
                "chat_id": chat_id, "name": str(getattr(data, "name", None) or chat_id),
                "type": map_chat_type(raw_chat_mode), "raw_type": raw_chat_type or None,
                "raw_mode": raw_chat_mode or None,
            }
            self._chat_info_cache[chat_id] = info
            return dict(info)
        except Exception:
            logger.warning("[Feishu] Failed to get chat info for %s", chat_id, exc_info=True)
            return fallback

    def format_message(self, content: str) -> str:
        """Feishu text messages are plain text by default."""
        return content.strip()

    # --- Inbound event handlers ---
    def _on_message_event(self, data: Any) -> None:
        """SDK dispatcher callback (background thread); queues for replay while the loop isn't ready."""
        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            if self._enqueue_pending_inbound_event(data):
                # Replayed events hop onto the loop from THIS thread's context; keep the WS thread's
                # profile scope (see _connect_websocket) rather than starting from an empty one.
                threading.Thread(
                    target=contextvars.copy_context().run, args=(self._drain_pending_inbound_events,),
                    name="feishu-pending-inbound-drainer", daemon=True,
                ).start()
            return
        self._submit_on_loop(loop, self._handle_message_event_data(data))

    def _enqueue_pending_inbound_event(self, data: Any) -> bool:
        """Queue an event for replay; True when the caller should spawn the (single) drainer thread."""
        with self._pending_inbound_lock:
            if len(self._pending_inbound_events) >= self._pending_inbound_max_depth:
                # Full — drop the oldest (loop unavailable for long AND WS still firing).
                dropped = self._pending_inbound_events.pop(0)
                try:
                    message = getattr(getattr(dropped, "event", None), "message", None)
                    message_id = str(getattr(message, "message_id", "") or "unknown")
                except Exception:
                    message_id = "unknown"
                logger.error(
                    "[Feishu] Pending-inbound queue full (%d); dropped oldest event %s",
                    self._pending_inbound_max_depth, message_id,
                )
            self._pending_inbound_events.append(data)
            depth = len(self._pending_inbound_events)
            should_start = not self._pending_drain_scheduled
            self._pending_drain_scheduled = True
        logger.warning("[Feishu] Queued inbound event for replay (loop not ready, queue depth=%d)", depth)
        return should_start

    def _drain_pending_inbound_events(self) -> None:
        """Daemon-thread drainer: replay queued inbound events once the loop is ready (or drop on shutdown/timeout)."""
        poll_interval = 0.25
        max_wait_seconds = 120.0  # safety cap: drop queue after 2 minutes
        waited = 0.0

        def _take_all() -> list[Any]:
            with self._pending_inbound_lock:
                batch = self._pending_inbound_events[:]
                self._pending_inbound_events.clear()
            return batch

        def _queue_empty() -> bool:
            with self._pending_inbound_lock:
                return not self._pending_inbound_events

        try:
            while True:
                if not getattr(self, "_running", True):
                    # Shutting down — drop rather than hold events against a closed loop.
                    dropped = len(_take_all())
                    if dropped:
                        logger.warning("[Feishu] Dropped %d queued inbound event(s) during shutdown", dropped)
                    return
                loop = self._loop
                if self._loop_accepts_callbacks(loop):
                    batch = _take_all()
                    if not batch:
                        if _queue_empty():  # emptied between check and grab
                            return
                        continue
                    # Loop closed/unavailable mid-batch → requeue those and poll again.
                    requeue = [e for e in batch if not self._submit_on_loop(loop, self._handle_message_event_data(e))]
                    if requeue:
                        with self._pending_inbound_lock:
                            self._pending_inbound_events[:0] = requeue
                    if len(batch) - len(requeue):
                        logger.info("[Feishu] Replayed %d queued inbound event(s)", len(batch) - len(requeue))
                    if not requeue and _queue_empty():  # fully drained and nothing new arrived
                        return
                    continue
                if waited >= max_wait_seconds:
                    logger.error(
                        "[Feishu] Adapter loop unavailable for %.0fs; dropped %d queued inbound event(s)",
                        max_wait_seconds, len(_take_all()),
                    )
                    return
                time.sleep(poll_interval)
                waited += poll_interval
        finally:
            with self._pending_inbound_lock:
                self._pending_drain_scheduled = False

    async def _handle_message_event_data(self, data: Any) -> None:
        """Shared inbound message handling for websocket and webhook transports."""
        event = getattr(data, "event", None)
        message = getattr(event, "message", None)
        sender = getattr(event, "sender", None)
        if not message or not sender or not getattr(sender, "sender_id", None):
            logger.debug("[Feishu] Dropping malformed inbound event: missing message/sender")
            return
        message_id = getattr(message, "message_id", None)
        if not message_id or await self._is_duplicate(message_id):
            logger.debug("[Feishu] Dropping duplicate/missing message_id: %s", message_id)
            return
        reason = self._admit(sender, message)
        if reason is not None:
            logger.debug("[Feishu] dropping inbound event: %s", reason)
            if reason == "group_policy_rejected":
                self._warn_once_empty_allowlist_deny(getattr(message, "chat_id", "") or "")
            return
        await self._process_inbound_message(
            data=data, message=message, sender_id=getattr(sender, "sender_id", None),
            chat_type=getattr(message, "chat_type", "p2p"), message_id=message_id, is_bot=_is_bot_sender(sender),
        )

    def _on_message_read_event(self, data: P2ImMessageMessageReadV1) -> None:
        """Ignore read-receipt events that Hermes does not act on."""
        message = getattr(getattr(data, "event", None), "message", None)
        logger.debug("[Feishu] Ignoring message_read event: %s", getattr(message, "message_id", None) or "")

    def _on_bot_membership_change(self, data: Any, verb: str) -> None:
        chat_id = str(getattr(getattr(data, "event", None), "chat_id", "") or "")
        logger.info("[Feishu] Bot %s chat: %s", verb, chat_id)
        self._chat_info_cache.pop(chat_id, None)

    def _on_bot_added_to_chat(self, data: Any) -> None:
        self._on_bot_membership_change(data, "added to")

    def _on_bot_removed_from_chat(self, data: Any) -> None:
        self._on_bot_membership_change(data, "removed from")

    def _on_p2p_chat_entered(self, data: Any) -> None:
        logger.debug("[Feishu] User entered P2P chat with bot")

    def _on_message_recalled(self, data: Any) -> None:
        logger.debug("[Feishu] Message recalled by user")

    def _submit_if_ready(self, label: str, make_coro: Any) -> None:
        """Schedule ``make_coro()`` on the adapter loop, or log-and-drop when the loop isn't ready."""
        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            logger.warning("[Feishu] Dropping %s before adapter loop is ready", label)
            return
        self._submit_on_loop(loop, make_coro())

    def _on_drive_comment_event(self, data: Any) -> None:
        """drive.notice.comment_add_v1 → feishu_comment.handle_drive_comment_event on the adapter loop."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event
        self._submit_if_ready("drive comment event", lambda: handle_drive_comment_event(
            self._client, data, self_open_id=self._bot_open_id, secondary=bool(getattr(self, "_owner_profile", None))))

    def _on_meeting_invited_event(self, data: Any) -> None:
        """vc.bot.meeting_invited_v1 → feishu_meeting_invite.handle_meeting_invited_event."""
        from plugins.platforms.feishu.feishu_meeting_invite import handle_meeting_invited_event
        self._submit_if_ready("meeting invite event", lambda: handle_meeting_invited_event(self, data))

    def _on_reaction_event(self, event_type: str, data: Any) -> None:
        """Route user reactions on bot messages as synthetic text events."""
        event = getattr(data, "event", None)
        message_id = str(getattr(event, "message_id", "") or "")
        operator_type = str(getattr(event, "operator_type", "") or "")
        reaction_type_obj = getattr(event, "reaction_type", None)
        emoji_type = str(getattr(reaction_type_obj, "emoji_type", "") or "")
        action = "added" if "created" in event_type else "removed"
        logger.debug(
            "[Feishu] Reaction %s on message %s (operator_type=%s, emoji=%s)",
            action, message_id, operator_type, emoji_type,
        )
        # Drop bot/app-origin reactions to break the feedback loop from our own lifecycle
        # reactions; a human clicking the same emoji is still routed through.
        loop = self._loop
        if operator_type in {"bot", "app"} or not message_id or not self._loop_accepts_callbacks(loop):
            return
        self._submit_on_loop(loop, self._handle_reaction_event(event_type, data))

    def _on_card_action_trigger(self, data: Any) -> Any:
        """Synchronous SDK card-action callback.

        Approval/update-prompt/clarify/picker buttons return the settled card inline (the only
        reliable way to sync all clients) and schedule any async resolution; other clicks are routed
        as synthetic commands via ``_handle_card_action_event``.
        """
        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            logger.warning("[Feishu] Dropping card action before adapter loop is ready")
            return self._card_response()
        event = getattr(data, "event", None)
        action = getattr(event, "action", None)
        action_value = getattr(action, "value", {}) or {}
        for key, handler in self._CARD_ACTION_ROUTES if isinstance(action_value, dict) else ():
            if action_value.get(key):
                return getattr(self, handler)(event=event, action_value=action_value, loop=loop)
        self._submit_on_loop(loop, self._handle_card_action_event(data))
        return self._card_response()

    @staticmethod
    def _loop_accepts_callbacks(loop: Any) -> bool:
        """Return True when the adapter loop can accept thread-safe submissions."""
        return loop is not None and not bool(getattr(loop, "is_closed", lambda: False)())

    def _submit_on_loop(self, loop: Any, coro: Any) -> bool:
        """Schedule background work on the adapter loop with shared failure logging."""
        from agent.async_utils import safe_schedule_threadsafe
        future = safe_schedule_threadsafe(
            coro, loop, logger=logger, log_message="[Feishu] Failed to schedule background callback work",
            log_level=logging.WARNING,
        )
        if future is None:
            return False
        future.add_done_callback(self._log_background_failure)
        return True

    @staticmethod
    def _card_response(card_data: Optional[dict[str, Any]] = None) -> Any:
        """Synchronous card-callback response; ``card_data`` updates the card inline."""
        if P2CardActionTriggerResponse is None:
            return None
        response = P2CardActionTriggerResponse()
        if card_data is not None and CallBackCard is not None:
            card = CallBackCard()
            card.type = "raw"
            card.data = card_data
            response.card = card
        return response

    def _card_toast(self, text: str) -> Any:
        """Card-callback response that leaves the card as is and shows *text* to the clicker only."""
        if (response := self._card_response()) is not None and (toast := CallBackToast) is not None:  # health: allow F821 -- bound with the other SDK names by _load_lark_oapi
            response.toast, response.toast.type, response.toast.content = toast(), "warning", text
        return response

    def _handle_approval_card_action(self, *, event: Any, action_value: dict[str, Any], loop: Any) -> Any:
        """Schedule approval resolution and build the synchronous callback response."""
        approval_id = action_value.get("approval_id")
        if approval_id is None:
            logger.debug("[Feishu] Card action missing approval_id, ignoring")
            return self._card_response()
        state = self._approval_state.get(approval_id)
        if not state:
            logger.debug("[Feishu] Approval %s already resolved or unknown", approval_id)
            return self._card_response()
        choice = _APPROVAL_CHOICE_MAP.get(action_value.get("hermes_action"), "deny")
        checked = validate_card_action(self, event=event, state=state, label="approval", ident=approval_id)
        if checked is None:
            return self._card_response()
        open_id, chat_id, user_name, user_id, union_id = checked
        # SessionSource.user_id is the tenant user_id when the app can see it, else the open_id.
        if requester := approval_refusal(state["session_key"], user_id or open_id, extra=self.config.extra,
                                         request_id=state.get("request_id"), choice=choice):
            return self._card_toast(refusal_text(requester))  # only the clicker sees it; card unchanged
        if not self._submit_on_loop(loop, self._resolve_approval(
                approval_id=approval_id, choice=choice, user_name=user_name, open_id=open_id, chat_id=chat_id,
                user_id=user_id, union_id=union_id)):
            return self._card_response()
        return self._card_response(self._build_resolved_approval_card(choice=choice, user_name=user_name, body=state.get("body")))

    def _handle_update_prompt_card_action(self, *, event: Any, action_value: dict[str, Any], loop: Any) -> Any:
        """Schedule update prompt resolution and build the synchronous callback response."""
        prompt_id = action_value.get("update_prompt_id")
        if prompt_id is None:
            logger.debug("[Feishu] Card action missing update_prompt_id, ignoring")
            return self._card_response()
        state = self._update_prompt_state.get(prompt_id)
        if not state:
            logger.debug("[Feishu] Update prompt %s already resolved or unknown", prompt_id)
            return self._card_response()
        answer = str(action_value.get("hermes_update_prompt_action", "") or "").strip().lower()
        if answer not in {"y", "n"}:
            logger.debug("[Feishu] Card action has invalid update prompt answer=%r", answer)
            return self._card_response()
        checked = validate_card_action(self, event=event, state=state, label="update prompt", ident=prompt_id)
        if checked is None:
            return self._card_response()
        open_id, chat_id, user_name, user_id, union_id = checked
        if requester := update_prompt_refusal(user_id or open_id, extra=self.config.extra):
            return self._card_toast(refusal_text(requester))  # not the /update sender or an admin: card stays open
        coro = self._resolve_update_prompt(prompt_id, answer, user_name, open_id=open_id, chat_id=chat_id, user_id=user_id, union_id=union_id)
        if not self._submit_on_loop(loop, coro):
            return self._card_response()
        return self._card_response(self._build_resolved_update_prompt_card(answer=answer, user_name=user_name))

    async def _resolve_approval(
        self, approval_id: Any, choice: str, user_name: str, *, open_id: str = "", chat_id: str = "",
        user_id: str = "", union_id: str = "",
    ) -> None:
        """Pop approval state and unblock the waiting agent thread."""
        state = pop_validated_prompt_state(
            self, states=self._approval_state, ident=approval_id, label="Approval", open_id=open_id, chat_id=chat_id,
            unauthorized_fmt="[Feishu] Unauthorized approval click by %s for approval %s",
            operator_repr=open_id or "<unknown>", user_id=user_id, union_id=union_id,
        )
        if not state:
            return
        try:
            from tools.approval import resolve_gateway_approval_card
            count = resolve_gateway_approval_card(state["session_key"], choice, state.get("request_id"),
                                                  actor_id=user_id or open_id, extra=self.config.extra)
            logger.info(
                "Feishu button resolved %d approval(s) for session %s (choice=%s, user=%s)",
                count, state["session_key"], choice, user_id or open_id,
            )
            if not count and choice != "deny":
                # The card already reads "Approved" (synchronous callback), but nothing was
                # waiting — the wait timed out (fail-closed deny) or was resolved via /approve.
                # Correct the record so the user doesn't believe the command ran.
                _chat = str(state.get("chat_id", "") or chat_id or "")
                if _chat:
                    try:
                        await self.send(_chat, t("platform.shared.approval_expired"))
                    except Exception:
                        logger.debug("[Feishu] expired-approval notice failed", exc_info=True)
        except Exception as exc:
            logger.error("Failed to resolve gateway approval from Feishu button: %s", exc)

    async def _resolve_update_prompt(
        self, prompt_id: Any, answer: str, user_name: str, *, open_id: str = "", chat_id: str = "",
        user_id: str = "", union_id: str = "",
    ) -> None:
        """Persist an update prompt answer for the detached update process."""
        state = pop_validated_prompt_state(
            self, states=self._update_prompt_state, ident=prompt_id, label="Update prompt", open_id=open_id,
            chat_id=chat_id, unauthorized_fmt="[Feishu] Unauthorized update prompt click by %s for prompt %s",
            operator_repr=open_id, user_id=user_id, union_id=union_id,
        )
        if not state:
            return
        try:
            self._write_update_prompt_response(answer)
            logger.info(
                "Feishu update prompt resolved for session %s (answer=%s, user=%s)",
                state["session_key"], answer, user_id or open_id,
            )
        except Exception as exc:
            logger.error("Failed to resolve Feishu update prompt: %s", exc)

    def _is_card_action_duplicate(self, token: str) -> bool:
        """Return True if this card action token was already processed within the dedup window."""
        now = time.time()
        # Prune expired tokens lazily each call.
        expired = [t for t, ts in self._card_action_tokens.items() if now - ts > _FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS]
        for t in expired:
            del self._card_action_tokens[t]
        if token in self._card_action_tokens:
            return True
        self._card_action_tokens[token] = now
        return False

