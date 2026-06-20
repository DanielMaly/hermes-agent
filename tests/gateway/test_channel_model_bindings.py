"""Tests for channel_model_bindings resolution and runtime application."""

import logging
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


def _ensure_discord_mock():
    if "discord" in sys.modules and hasattr(sys.modules["discord"], "__file__"):
        return
    discord_mod = types.ModuleType("discord")
    setattr(discord_mod, "Intents", MagicMock())
    getattr(discord_mod, "Intents").default.return_value = MagicMock()
    setattr(discord_mod, "DMChannel", type("DMChannel", (), {}))
    setattr(discord_mod, "Thread", type("Thread", (), {}))
    setattr(discord_mod, "ForumChannel", type("ForumChannel", (), {}))
    setattr(discord_mod, "Interaction", object)
    ext_mod = MagicMock()
    commands_mod = MagicMock()
    commands_mod.Bot = MagicMock
    ext_mod.commands = commands_mod
    sys.modules.setdefault("discord", discord_mod)
    sys.modules.setdefault("discord.ext", ext_mod)
    sys.modules.setdefault("discord.ext.commands", commands_mod)


def _make_adapter():
    _ensure_discord_mock()
    from plugins.platforms.discord.adapter import DiscordAdapter

    adapter = object.__new__(DiscordAdapter)
    adapter.config = MagicMock()
    adapter.config.extra = {}
    return adapter


def _make_source() -> SessionSource:
    return SessionSource(
        platform=Platform.DISCORD,
        chat_id="12345",
        chat_type="thread",
        user_id="user-1",
    )


def _make_runner():
    runner = object.__new__(gateway_run.GatewayRunner)
    runner._session_model_overrides = {}
    runner._session_key_for_source = lambda source: "agent:main:discord:thread:12345"
    return runner


class TestResolveChannelModelBindings:
    def test_no_bindings_returns_none(self):
        adapter = _make_adapter()
        assert adapter._resolve_channel_model_binding("123") is None

    def test_match_by_channel_id_with_provider_and_model(self):
        adapter = _make_adapter()
        adapter.config.extra = {
            "channel_model_bindings": {
                "100": {"provider": "openrouter", "model": "openrouter/auto"}
            }
        }
        assert adapter._resolve_channel_model_binding("100") == {
            "provider": "openrouter",
            "model": "openrouter/auto",
        }

    def test_match_by_parent_id(self):
        adapter = _make_adapter()
        adapter.config.extra = {
            "channel_model_bindings": {
                "200": {"provider": "anthropic", "model": "claude-sonnet-4-6"}
            }
        }
        assert adapter._resolve_channel_model_binding("999", parent_id="200") == {
            "provider": "anthropic",
            "model": "claude-sonnet-4-6",
        }

    def test_exact_channel_overrides_parent(self):
        adapter = _make_adapter()
        adapter.config.extra = {
            "channel_model_bindings": {
                "999": {"provider": "openrouter", "model": "openrouter/auto"},
                "200": {"provider": "anthropic", "model": "claude-sonnet-4-6"},
            }
        }
        assert adapter._resolve_channel_model_binding("999", parent_id="200") == {
            "provider": "openrouter",
            "model": "openrouter/auto",
        }

    def test_string_value_is_model_shorthand(self):
        adapter = _make_adapter()
        adapter.config.extra = {"channel_model_bindings": {"100": "openrouter/auto"}}
        assert adapter._resolve_channel_model_binding("100") == {"model": "openrouter/auto"}

    def test_malformed_bindings_root_warns_and_is_ignored(self, caplog):
        adapter = _make_adapter()
        adapter.config.extra = {"channel_model_bindings": ["bad"]}

        with caplog.at_level(logging.WARNING, logger="gateway.platforms.base"):
            assert adapter._resolve_channel_model_binding("100") is None

        assert "Ignoring malformed channel_model_bindings config" in caplog.text
        assert "expected mapping, got list" in caplog.text

    def test_blank_or_invalid_bindings_warn_and_are_ignored(self, caplog):
        adapter = _make_adapter()
        adapter.config.extra = {"channel_model_bindings": {"100": "   ", "200": ["bad"]}}

        with caplog.at_level(logging.WARNING, logger="gateway.platforms.base"):
            assert adapter._resolve_channel_model_binding("100") is None
            assert adapter._resolve_channel_model_binding("200") is None

        assert "Ignoring malformed channel_model_bindings entry for channel 100" in caplog.text
        assert "got str" in caplog.text
        assert "Ignoring malformed channel_model_bindings entry for channel 200" in caplog.text
        assert "got list" in caplog.text

    def test_build_slash_event_sets_channel_model_binding(self):
        adapter = _make_adapter()
        adapter.config.extra = {
            "channel_model_bindings": {
                "321": {"provider": "openrouter", "model": "openrouter/auto"}
            }
        }
        adapter.build_source = MagicMock(return_value=SimpleNamespace())
        adapter._get_effective_topic = MagicMock(return_value=None)

        interaction = SimpleNamespace(
            channel_id=321,
            channel=SimpleNamespace(name="general", guild=None, parent_id=None),
            user=SimpleNamespace(id=1, display_name="Brenner"),
        )

        event = adapter._build_slash_event(interaction, "/model")

        assert event.channel_model_binding == {
            "provider": "openrouter",
            "model": "openrouter/auto",
        }

    @pytest.mark.asyncio
    async def test_dispatch_thread_session_inherits_parent_channel_model_binding(self):
        adapter = _make_adapter()
        adapter.config.extra = {
            "channel_model_bindings": {
                "200": {"provider": "openrouter", "model": "openrouter/auto"}
            }
        }
        adapter.build_source = MagicMock(return_value=SimpleNamespace())
        adapter._get_effective_topic = MagicMock(return_value=None)
        adapter.handle_message = AsyncMock()

        interaction = SimpleNamespace(
            guild=SimpleNamespace(name="Wetlands"),
            channel=SimpleNamespace(id=200, parent=None),
            user=SimpleNamespace(id=1, display_name="Brenner"),
        )

        await adapter._dispatch_thread_session(interaction, "999", "new-thread", "hello")

        dispatched_event = adapter.handle_message.await_args.args[0]
        assert dispatched_event.channel_model_binding == {
            "provider": "openrouter",
            "model": "openrouter/auto",
        }


def test_config_bridges_discord_channel_model_bindings(monkeypatch, tmp_path):
    import yaml
    from gateway.config import load_gateway_config

    (tmp_path / "config.yaml").write_text(
        yaml.dump(
            {
                "discord": {
                    "enabled": True,
                    "channel_model_bindings": {
                        1234567890: {
                            "provider": "openrouter",
                            "model": "openrouter/auto",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    config = load_gateway_config()

    discord_extra = config.platforms[Platform.DISCORD].extra
    assert discord_extra["channel_model_bindings"] == {
        "1234567890": {"provider": "openrouter", "model": "openrouter/auto"}
    }


@pytest.mark.parametrize(
    ("platform_name", "platform"),
    [
        ("slack", Platform.SLACK),
        ("telegram", Platform.TELEGRAM),
        ("mattermost", Platform.MATTERMOST),
        ("whatsapp", Platform.WHATSAPP),
    ],
)
def test_config_bridges_channel_model_bindings_for_channel_platforms(
    platform_name, platform, monkeypatch, tmp_path
):
    import yaml
    from gateway.config import load_gateway_config

    (tmp_path / "config.yaml").write_text(
        yaml.dump(
            {
                platform_name: {
                    "enabled": True,
                    "channel_model_bindings": {
                        "channel-1": {
                            "provider": "openrouter",
                            "model": "openrouter/auto",
                        },
                        "channel-2": "anthropic/claude-sonnet-4",
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    config = load_gateway_config()

    assert config.platforms[platform].extra["channel_model_bindings"] == {
        "channel-1": {"provider": "openrouter", "model": "openrouter/auto"},
        "channel-2": "anthropic/claude-sonnet-4",
    }


def test_channel_model_binding_applies_provider_and_model(monkeypatch):
    runner = _make_runner()
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "global-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "openai",
            "api_key": "global-key",
            "base_url": "https://api.openai.com/v1",
            "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_channel_binding_runtime_kwargs",
        lambda binding: {
            "provider": binding["provider"],
            "api_key": "channel-key",
            "base_url": "https://openrouter.ai/api/v1",
            "api_mode": "chat_completions",
        },
    )

    model, runtime = runner._resolve_session_agent_runtime(
        source=_make_source(),
        session_key="agent:main:discord:thread:12345",
        user_config={},
        channel_model_binding={"provider": "openrouter", "model": "openrouter/auto"},
    )

    assert model == "openrouter/auto"
    assert runtime["provider"] == "openrouter"
    assert runtime["api_key"] == "channel-key"
    assert runtime["base_url"] == "https://openrouter.ai/api/v1"


def test_channel_provider_binding_uses_runtime_default_model(monkeypatch):
    runner = _make_runner()
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "global-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: pytest.fail("global runtime should not resolve before a provider-bound channel"),
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_channel_binding_runtime_kwargs",
        lambda binding: {
            "provider": binding["provider"],
            "api_key": "custom-key",
            "base_url": "https://example.test/v1",
            "api_mode": "chat_completions",
            "model": "custom-default-model",
        },
    )

    model, runtime = runner._resolve_session_agent_runtime(
        source=_make_source(),
        session_key="agent:main:discord:thread:12345",
        user_config={},
        channel_model_binding={"provider": "custom:test"},
    )

    assert model == "custom-default-model"
    assert runtime["provider"] == "custom:test"
    assert "model" not in runtime


def test_channel_binding_runtime_resolution_uses_bound_model(monkeypatch):
    seen = {}

    def fake_resolve_runtime_provider(**kwargs):
        seen.update(kwargs)
        return {
            "provider": kwargs["requested"],
            "api_key": "channel-key",
            "base_url": kwargs["explicit_base_url"],
            "api_mode": "chat_completions",
        }

    monkeypatch.setitem(
        sys.modules,
        "hermes_cli.runtime_provider",
        SimpleNamespace(
            resolve_runtime_provider=fake_resolve_runtime_provider,
            format_runtime_provider_error=lambda exc: str(exc),
        ),
    )

    runtime = gateway_run._resolve_channel_binding_runtime_kwargs(
        {
            "provider": "opencode-zen",
            "model": "anthropic/claude-sonnet-4",
            "base_url": "https://opencode.ai/zen",
        }
    )

    assert seen == {
        "requested": "opencode-zen",
        "explicit_base_url": "https://opencode.ai/zen",
        "target_model": "anthropic/claude-sonnet-4",
    }
    assert runtime["provider"] == "opencode-zen"


def test_channel_binding_runtime_resolution_preserves_provider_default_model(monkeypatch):
    def fake_resolve_runtime_provider(**kwargs):
        assert kwargs == {
            "requested": "custom:test",
            "explicit_base_url": None,
            "target_model": None,
        }
        return {
            "provider": "custom:test",
            "api_key": "channel-key",
            "base_url": "https://example.test/v1",
            "api_mode": "chat_completions",
            "model": "custom-default-model",
        }

    monkeypatch.setitem(
        sys.modules,
        "hermes_cli.runtime_provider",
        SimpleNamespace(
            resolve_runtime_provider=fake_resolve_runtime_provider,
            format_runtime_provider_error=lambda exc: str(exc),
        ),
    )

    runtime = gateway_run._resolve_channel_binding_runtime_kwargs({"provider": "custom:test"})

    assert runtime["provider"] == "custom:test"
    assert runtime["model"] == "custom-default-model"


def test_synthetic_event_source_can_resolve_channel_binding(monkeypatch):
    runner = _make_runner()
    runner.adapters = {  # type: ignore[assignment]
        Platform.DISCORD: SimpleNamespace(
            config=SimpleNamespace(
                extra={
                    "channel_model_bindings": {
                        "parent-1": {"provider": "openrouter", "model": "openrouter/auto"}
                    }
                }
            )
        )
    }
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="thread-1",
        chat_type="thread",
        thread_id="thread-1",
        parent_chat_id="parent-1",
        user_id="user-1",
    )
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "global-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "openai",
            "api_key": "global-key",
            "base_url": "https://api.openai.com/v1",
            "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_channel_binding_runtime_kwargs",
        lambda binding: {
            "provider": binding["provider"],
            "api_key": "channel-key",
            "base_url": "https://openrouter.ai/api/v1",
            "api_mode": "chat_completions",
        },
    )

    model, runtime = runner._resolve_session_agent_runtime(
        source=source,
        session_key="agent:main:discord:thread:thread-1",
        user_config={},
        channel_model_binding=None,
    )

    assert model == "openrouter/auto"
    assert runtime["provider"] == "openrouter"


@pytest.mark.parametrize("platform", [Platform.SLACK, Platform.TELEGRAM, Platform.MATTERMOST, Platform.WHATSAPP])
def test_synthetic_event_source_resolves_non_discord_channel_binding(platform, monkeypatch):
    runner = _make_runner()
    setattr(
        runner,
        "adapters",
        {
            platform: SimpleNamespace(
                config=SimpleNamespace(
                    extra={
                        "channel_model_bindings": {
                            "parent-channel": {
                                "provider": "openrouter",
                                "model": "openrouter/auto",
                            }
                        }
                    }
                )
            )
        },
    )
    source = SessionSource(
        platform=platform,
        chat_id="parent-channel",
        chat_type="thread",
        thread_id="thread-1",
        user_id="user-1",
    )
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "global-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "openai",
            "api_key": "global-key",
            "base_url": "https://api.openai.com/v1",
            "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_channel_binding_runtime_kwargs",
        lambda binding: {
            "provider": binding["provider"],
            "api_key": "channel-key",
            "base_url": "https://openrouter.ai/api/v1",
            "api_mode": "chat_completions",
        },
    )

    model, runtime = runner._resolve_session_agent_runtime(
        source=source,
        session_key=f"agent:main:{platform.value}:thread:thread-1",
        user_config={},
        channel_model_binding=None,
    )

    assert model == "openrouter/auto"
    assert runtime["provider"] == "openrouter"


def test_session_model_override_wins_over_channel_binding(monkeypatch):
    runner = _make_runner()
    session_key = "agent:main:discord:thread:12345"
    runner._session_model_overrides[session_key] = {
        "model": "claude-sonnet-4-6",
        "provider": "anthropic",
        "base_url": "https://api.anthropic.com",
        "api_mode": "chat_completions",
    }
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "global-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "openai",
            "api_key": "global-key",
            "base_url": "https://api.openai.com/v1",
            "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_channel_binding_runtime_kwargs",
        lambda binding: pytest.fail("channel binding should not be resolved when /model override exists"),
    )

    model, runtime = runner._resolve_session_agent_runtime(
        source=_make_source(),
        session_key=session_key,
        user_config={},
        channel_model_binding={"provider": "openrouter", "model": "openrouter/auto"},
    )

    assert model == "claude-sonnet-4-6"
    assert runtime["provider"] == "anthropic"
    assert runtime["base_url"] == "https://api.anthropic.com"


@pytest.mark.asyncio
async def test_retry_preserves_channel_model_binding(monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    runner._session_key_for_source = lambda source: "agent:main:discord:thread:12345"
    runner.session_store = SimpleNamespace(
        get_or_create_session=lambda source: SimpleNamespace(session_id="session-1", last_prompt_tokens=10),
        load_transcript=lambda session_id: [
            {"role": "user", "content": "original message"},
            {"role": "assistant", "content": "old reply"},
        ],
        rewrite_transcript=MagicMock(),
    )
    runner._handle_message = AsyncMock(return_value="ok")

    event = MessageEvent(
        text="/retry",
        message_type=gateway_run.MessageType.COMMAND,
        source=_make_source(),
        raw_message=SimpleNamespace(),
        channel_model_binding={"provider": "openrouter", "model": "openrouter/auto"},
    )

    result = await runner._handle_retry_command(event)

    assert result == "ok"
    retried_event = runner._handle_message.await_args.args[0]
    assert retried_event.channel_model_binding == {
        "provider": "openrouter",
        "model": "openrouter/auto",
    }


def test_whatsapp_jid_resolves_channel_model_binding():
    """WhatsApp JIDs (DM @s.whatsapp.net, group @g.us) work as channel keys."""
    from gateway.platforms.base import resolve_channel_model_binding

    config_extra = {
        "channel_model_bindings": {
            "420777123456@s.whatsapp.net": {
                "provider": "openrouter",
                "model": "openrouter/auto",
            },
            "120363000000000000@g.us": "anthropic/claude-sonnet-4",
        }
    }

    # DM JID
    assert resolve_channel_model_binding(
        config_extra, "420777123456@s.whatsapp.net", None
    ) == {"provider": "openrouter", "model": "openrouter/auto"}

    # Group JID
    assert resolve_channel_model_binding(
        config_extra, "120363000000000000@g.us", None
    ) == {"model": "anthropic/claude-sonnet-4"}

    # Unbound JID falls back to None
    assert resolve_channel_model_binding(
        config_extra, "9999999999@s.whatsapp.net", None
    ) is None
