"""Keep upstream's macOS monotonic births without changing its numeric spawn ledger."""
from types import SimpleNamespace

import pytest

import hermes_process_identity as identity


@pytest.mark.parametrize('host', ['darwin', 'win32'])
def test_non_linux_uses_upstream_birth_for_hello_and_matching(monkeypatch, host):
    from hermes_cli import process_identity as upstream
    monkeypatch.setattr(identity.sys, 'platform', host)
    monkeypatch.setattr(upstream, 'process_birth', lambda process: 123.0)
    process = SimpleNamespace(create_time=lambda: 456.0)
    assert identity.process_birth(process) == 123.0
    assert identity.matches_process_birth(process, 123.0)
    assert not identity.matches_process_birth(process, 456.0)


def test_linux_legacy_timestamp_remains_exact(monkeypatch):
    monkeypatch.setattr(identity.sys, 'platform', 'linux')
    process = SimpleNamespace(create_time=lambda: 456.0)
    assert identity.matches_process_birth(process, 456.0)
    assert not identity.matches_process_birth(process, 123.0)
