from __future__ import annotations

import socket

import pytest

import synor


def test_offline_policy_denies_before_dns() -> None:
    with pytest.raises(synor.PolicyViolation, match="network access is disabled"):
        with synor.policy_scope(synor.EgressPolicy.offline()):
            socket.getaddrinfo("example.invalid", 443)


def test_offline_policy_allows_local_ipc_without_af_unix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(socket, "AF_UNIX", raising=False)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()

        with synor.policy_scope(synor.EgressPolicy.offline()):
            with socket.create_connection(listener.getsockname()) as client:
                connection, _address = listener.accept()
                connection.close()
                assert client.getpeername() == listener.getsockname()

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as external:
                with pytest.raises(
                    synor.PolicyViolation, match="network access is disabled"
                ):
                    external.connect(("192.0.2.1", 443))


def test_allow_list_and_classification_are_enforced() -> None:
    policy = synor.EgressPolicy(
        allowed_hosts=frozenset({"api.example.test", "*.trusted.test"}),
        max_data_classification=synor.DataClassification.INTERNAL,
    )
    assert policy.decide(
        synor.EgressRequest(
            destination="https://api.example.test/v1",
            purpose="test",
        )
    ).allowed
    assert policy.decide(
        synor.EgressRequest(
            destination="worker.trusted.test",
            purpose="test",
        )
    ).allowed
    assert not policy.decide(
        synor.EgressRequest(destination="elsewhere.test", purpose="test")
    ).allowed
    assert not policy.decide(
        synor.EgressRequest(
            destination="api.example.test",
            purpose="test",
            classification=synor.DataClassification.CONFIDENTIAL,
        )
    ).allowed


def test_policy_scope_audits_metadata_and_restores_socket() -> None:
    original = socket.create_connection
    events: list[dict[str, object]] = []
    request = synor.EgressRequest(destination="127.0.0.1", purpose="test")

    with synor.policy_scope(synor.EgressPolicy.offline(), audit_sink=events.append):
        with pytest.raises(synor.PolicyViolation):
            synor.authorize_egress(request)
        assert socket.create_connection is not original

    assert socket.create_connection is original
    assert events == [
        {
            "allowed": False,
            "rule": "network_access",
            "destination": "127.0.0.1",
            "purpose": "test",
            "classification": "internal",
            "byte_count": None,
        }
    ]


def test_policy_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYNOR_ALLOWED_HOSTS", "one.test, *.two.test")
    monkeypatch.setenv("SYNOR_MAX_EGRESS_CLASSIFICATION", "public")
    policy = synor.policy_from_env(offline=True)
    assert policy.network_access is synor.NetworkAccess.DENY
    assert policy.allowed_hosts == frozenset({"one.test", "*.two.test"})
    assert policy.max_data_classification is synor.DataClassification.PUBLIC
