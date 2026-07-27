"""Regression contract test for finalize_turn caller/callee signature agreement.

Catches the 2026-07-12/2026-07-24 rebase regression where upstream added a
parameter to ``finalize_turn`` but a fork cherry-pick resolution silently
dropped it, causing a ``TypeError`` at runtime:

    finalize_turn() got an unexpected keyword argument '_pending_verification_response'
    (and later '_pending_verification_response_previewed')

The test inspects the actual call site in ``conversation_loop.py`` against the
live signature of ``finalize_turn``, AND exercises the preview-state budget path.
"""

import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from agent.turn_finalizer import finalize_turn


class _ContractAgent:
    """Minimal agent double sufficient for the contract exercise."""

    def __init__(self, *, max_iterations=60, budget_remaining=0):
        self.max_iterations = max_iterations
        self.iteration_budget = SimpleNamespace(
            remaining=budget_remaining, used=max_iterations, max_total=max_iterations
        )
        self.quiet_mode = True
        self.model = "test-model"
        self.provider = "test-provider"
        self.base_url = ""
        self.session_id = "sess-test"
        self.context_compressor = SimpleNamespace(last_prompt_tokens=0)
        self.session_input_tokens = 0
        self.session_output_tokens = 0
        self.session_cache_read_tokens = 0
        self.session_cache_write_tokens = 0
        self.session_reasoning_tokens = 0
        self.session_prompt_tokens = 0
        self.session_completion_tokens = 0
        self.session_total_tokens = 0
        self.session_estimated_cost_usd = 0
        self.session_cost_status = "unknown"
        self.session_cost_source = "test"
        self._tool_guardrail_halt_decision = None
        self._interrupt_message = None
        self._response_was_previewed = False
        self._skill_nudge_interval = 0
        self._iters_since_skill = 0
        self.valid_tool_names = []
        self.persisted_messages = None
        self._handle_max_iterations_called = False

    def _handle_max_iterations(self, messages, api_call_count):
        self._handle_max_iterations_called = True
        return "summary from extra call"

    def _emit_status(self, *_args, **_kwargs):
        pass

    def _safe_print(self, *_args, **_kwargs):
        pass

    def _save_trajectory(self, *_args, **_kwargs):
        pass

    def _cleanup_task_resources(self, *_args, **_kwargs):
        pass

    def _drop_trailing_empty_response_scaffolding(self, messages):
        pass

    def _persist_session(self, messages, conversation_history):
        self.persisted_messages = list(messages)

    def _file_mutation_verifier_enabled(self):
        return False

    def _turn_completion_explainer_enabled(self):
        return False

    def _format_turn_completion_explanation(self, _reason):
        return "explanation"

    def _drain_pending_steer(self):
        return None

    def clear_interrupt(self):
        pass

    def _sync_external_memory_for_turn(self, **_kwargs):
        pass


def _finalize(
    agent,
    *,
    final_response,
    exit_reason,
    api_call_count=60,
    pending_verification_response=None,
    pending_verification_response_previewed=False,
):
    return finalize_turn(
        agent,
        final_response=final_response,
        api_call_count=api_call_count,
        interrupted=False,
        failed=False,
        messages=[{"role": "user", "content": "task"}],
        conversation_history=[],
        effective_task_id="task",
        turn_id="turn",
        user_message="task",
        original_user_message="task",
        _should_review_memory=False,
        _turn_exit_reason=exit_reason,
        _pending_verification_response=pending_verification_response,
        _pending_verification_response_previewed=pending_verification_response_previewed,
    )


def _extract_finalize_turn_call_keywords(source_path: Path) -> set[str]:
    """Parse the file and return every keyword argument passed to finalize_turn()."""
    source = source_path.read_text()
    tree = ast.parse(source)
    keywords = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            # The call may be ``finalize_turn(...)`` or ``from agent.turn_finalizer import finalize_turn``
            func = node.func
            if isinstance(func, ast.Name) and func.id == "finalize_turn":
                for kw in node.keywords:
                    keywords.add(kw.arg)
            elif isinstance(func, ast.Attribute) and func.attr == "finalize_turn":
                for kw in node.keywords:
                    keywords.add(kw.arg)
    return keywords


def test_finalize_turn_signature_matches_conversation_loop_callers():
    """All keyword args passed by conversation_loop.py must exist in the signature."""
    sig = inspect.signature(finalize_turn)
    sig_params = set(sig.parameters.keys())

    repo_root = Path(__file__).parent.parent.parent
    conv_loop = repo_root / "agent" / "conversation_loop.py"
    assert conv_loop.exists(), f"{conv_loop} not found"

    caller_keywords = _extract_finalize_turn_call_keywords(conv_loop)
    # The first positional arg ``agent`` is passed positionally, so we don't
    # expect it as a keyword.
    assert "agent" not in caller_keywords

    missing = caller_keywords - sig_params
    assert not missing, (
        f"conversation_loop.py passes these keyword args not accepted by "
        f"finalize_turn: {missing} — caller/callee contract divergence"
    )


def test_pending_verification_response_previewed_sets_preview_flag(monkeypatch):
    """When the pending response was already previewed, flag it so downstream
    knows not to re-stream it as brand-new content."""
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_a, **_kw: [])
    agent = _ContractAgent()
    report = "budget-exhausted but previewed report"

    result = _finalize(
        agent,
        final_response=None,
        exit_reason="unknown",
        pending_verification_response=report,
        pending_verification_response_previewed=True,
    )

    assert result["final_response"] == report
    assert result["response_previewed"] is True
    assert agent._handle_max_iterations_called is False


def test_pending_verification_response_not_previewed_does_not_set_flag(monkeypatch):
    """When the pending response was NOT previewed, the flag stays False."""
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_a, **_kw: [])
    agent = _ContractAgent()
    report = "budget-exhausted unseen report"

    result = _finalize(
        agent,
        final_response=None,
        exit_reason="unknown",
        pending_verification_response=report,
        pending_verification_response_previewed=False,
    )

    assert result["final_response"] == report
    assert result["response_previewed"] is False
    assert agent._handle_max_iterations_called is False
