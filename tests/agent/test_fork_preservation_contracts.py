"""Fork-preservation semantic contracts."""
import pytest
from agent.transports.chat_completions import ChatCompletionsTransport


@pytest.mark.parametrize('provider', [None, 'openrouter'])
def test_transport_session_metadata_survives_both_paths(provider):
    from providers import get_provider_profile
    params = dict(session_id='fork-session', extra_body_additions={'metadata': {'other': 'kept'}})
    if provider:
        params['profile'] = get_provider_profile(provider)
    kwargs = ChatCompletionsTransport().build_kwargs(model='test/model', messages=[], tools=[], **params)
    assert kwargs['extra_body']['metadata']['session_id'] == 'fork-session'


def test_review_and_curator_keep_applicability_conditions():
    from agent.background_review import _SKILL_REVIEW_PROMPT, _COMBINED_REVIEW_PROMPT
    from agent.curator import CURATOR_REVIEW_PROMPT
    for prompt in [_SKILL_REVIEW_PROMPT, _COMBINED_REVIEW_PROMPT, CURATOR_REVIEW_PROMPT]:
        assert 'applicability conditions' in prompt
        assert 'STRUCTURE' in prompt
    assert 'fewer than 10 archives' not in CURATOR_REVIEW_PROMPT
