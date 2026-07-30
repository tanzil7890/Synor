"""Network and data-egress policy for controlled Synor runs."""

from __future__ import annotations

import contextlib as _contextlib
import contextvars as _contextvars
import dataclasses as _dataclasses
import enum as _enum
import ipaddress as _ipaddress
import os as _os
import socket as _socket
import threading as _threading
import typing as _typing
import urllib.parse as _urlparse

__all__ = [
    "DataClassification",
    "EgressPolicy",
    "EgressRequest",
    "NetworkAccess",
    "PolicyDecision",
    "PolicyViolation",
    "authorize_egress",
    "current_policy",
    "network_is_denied",
    "policy_from_env",
    "policy_scope",
]


class NetworkAccess(str, _enum.Enum):
    """Whether network connections are allowed during a controlled run."""

    ALLOW = "allow"
    DENY = "deny"


class DataClassification(str, _enum.Enum):
    """Sensitivity assigned to data before it crosses a trust boundary."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


_CLASSIFICATION_RANK = {
    DataClassification.PUBLIC: 0,
    DataClassification.INTERNAL: 1,
    DataClassification.CONFIDENTIAL: 2,
    DataClassification.RESTRICTED: 3,
}


@_dataclasses.dataclass(frozen=True, slots=True)
class EgressRequest:
    """A proposed transfer to an external destination.

    ``destination`` is a hostname or URL. It must not contain credentials,
    tokens, query strings, prompts, or record content.
    """

    destination: str
    purpose: str
    classification: DataClassification = DataClassification.INTERNAL
    byte_count: int | None = None

    def __post_init__(self) -> None:
        if not self.destination.strip():
            raise ValueError("destination must not be empty")
        if not self.purpose.strip():
            raise ValueError("purpose must not be empty")
        if self.byte_count is not None and self.byte_count < 0:
            raise ValueError("byte_count must be non-negative")


@_dataclasses.dataclass(frozen=True, slots=True)
class PolicyDecision:
    """The result of evaluating one egress request."""

    allowed: bool
    rule: str
    reason: str


class PolicyViolation(PermissionError):
    """Raised before a denied network or data-egress operation."""

    def __init__(self, request: EgressRequest, decision: PolicyDecision) -> None:
        self.request = request
        self.decision = decision
        super().__init__(
            f"Egress denied by {decision.rule}: {decision.reason} "
            f"(destination={_safe_destination(request.destination)!r})"
        )


@_dataclasses.dataclass(frozen=True, slots=True)
class EgressPolicy:
    """Policy applied to network connections and classified data egress.

    ``allowed_hosts=None`` permits any host when networking is enabled. An
    explicit set acts as an allow-list. Entries may be exact hostnames,
    ``*.example.com`` suffix rules, or IP addresses.
    """

    network_access: NetworkAccess = NetworkAccess.ALLOW
    allowed_hosts: frozenset[str] | None = None
    max_data_classification: DataClassification = DataClassification.RESTRICTED

    def __post_init__(self) -> None:
        if self.allowed_hosts is not None:
            normalized = frozenset(
                _normalize_host(item) for item in self.allowed_hosts if item.strip()
            )
            object.__setattr__(self, "allowed_hosts", normalized)

    @classmethod
    def offline(cls) -> "EgressPolicy":
        """Return a policy that denies every network connection."""

        return cls(network_access=NetworkAccess.DENY)

    def decide(self, request: EgressRequest) -> PolicyDecision:
        """Evaluate a request without performing the transfer."""

        if self.network_access is NetworkAccess.DENY:
            return PolicyDecision(
                allowed=False,
                rule="network_access",
                reason="network access is disabled",
            )

        host = _destination_host(request.destination)
        if self.allowed_hosts is not None and not _host_is_allowed(
            host, self.allowed_hosts
        ):
            return PolicyDecision(
                allowed=False,
                rule="allowed_hosts",
                reason=f"host {host!r} is not on the allow-list",
            )

        if (
            _CLASSIFICATION_RANK[request.classification]
            > _CLASSIFICATION_RANK[self.max_data_classification]
        ):
            return PolicyDecision(
                allowed=False,
                rule="max_data_classification",
                reason=(
                    f"{request.classification.value} data exceeds the "
                    f"{self.max_data_classification.value} egress limit"
                ),
            )

        return PolicyDecision(
            allowed=True,
            rule="allow",
            reason="request satisfies the active egress policy",
        )

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return a manifest-safe representation."""

        return {
            "network_access": self.network_access.value,
            "allowed_hosts": (
                sorted(self.allowed_hosts) if self.allowed_hosts is not None else None
            ),
            "max_data_classification": self.max_data_classification.value,
        }


_AuditSink = _typing.Callable[[dict[str, _typing.Any]], None]
_policy_var: _contextvars.ContextVar[EgressPolicy | None] = _contextvars.ContextVar(
    "synor_egress_policy", default=None
)
_audit_sink_var: _contextvars.ContextVar[_AuditSink | None] = _contextvars.ContextVar(
    "synor_policy_audit_sink", default=None
)
_scope_lock = _threading.RLock()
_process_scopes: list[tuple[EgressPolicy, _AuditSink | None]] = []

_original_socket_connect = _socket.socket.connect
_original_socket_connect_ex = _socket.socket.connect_ex
_original_socket_sendto = _socket.socket.sendto
_original_create_connection = _socket.create_connection
_original_getaddrinfo = _socket.getaddrinfo


def _normalize_host(value: str) -> str:
    return value.strip().lower().rstrip(".")


def _destination_host(destination: str) -> str:
    candidate = destination.strip()
    if "://" in candidate:
        parsed = _urlparse.urlsplit(candidate)
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("destination must not contain credentials")
        host = parsed.hostname
        if host is None:
            raise ValueError(f"destination has no hostname: {destination!r}")
        return _normalize_host(host)

    if candidate.startswith("[") and "]" in candidate:
        return _normalize_host(candidate[1 : candidate.index("]")])

    try:
        _ipaddress.ip_address(candidate)
    except ValueError:
        if candidate.count(":") == 1:
            host, port = candidate.rsplit(":", 1)
            if port.isdigit():
                candidate = host
    return _normalize_host(candidate)


def _safe_destination(destination: str) -> str:
    try:
        return _destination_host(destination)
    except ValueError:
        return "<invalid>"


def _host_is_allowed(host: str, allowed_hosts: frozenset[str]) -> bool:
    for rule in allowed_hosts:
        if rule.startswith("*."):
            suffix = rule[1:]
            if host.endswith(suffix) and host != suffix[1:]:
                return True
        elif host == rule:
            return True
    return False


def current_policy() -> EgressPolicy:
    """Return the policy active in this context or process scope."""

    contextual = _policy_var.get()
    if contextual is not None:
        return contextual
    with _scope_lock:
        if _process_scopes:
            return _process_scopes[-1][0]
    return EgressPolicy()


def network_is_denied() -> bool:
    """Return whether the active policy denies network access."""

    return current_policy().network_access is NetworkAccess.DENY


def _current_audit_sink() -> _AuditSink | None:
    contextual = _audit_sink_var.get()
    if contextual is not None:
        return contextual
    with _scope_lock:
        if _process_scopes:
            return _process_scopes[-1][1]
    return None


def authorize_egress(request: EgressRequest) -> PolicyDecision:
    """Authorize one request or raise :class:`PolicyViolation`.

    The emitted audit event contains metadata only. It never includes the
    transferred data.
    """

    decision = current_policy().decide(request)
    sink = _current_audit_sink()
    if sink is not None:
        sink(
            {
                "allowed": decision.allowed,
                "rule": decision.rule,
                "destination": _safe_destination(request.destination),
                "purpose": request.purpose,
                "classification": request.classification.value,
                "byte_count": request.byte_count,
            }
        )
    if not decision.allowed:
        raise PolicyViolation(request, decision)
    return decision


def _socket_destination(sock: _socket.socket, address: _typing.Any) -> str | None:
    if sock.family == _socket.AF_UNIX:
        return None
    if isinstance(address, tuple) and address:
        return str(address[0])
    if isinstance(address, str) and sock.family in {
        _socket.AF_INET,
        _socket.AF_INET6,
    }:
        return address
    return None


def _guarded_connect(sock: _socket.socket, address: _typing.Any) -> _typing.Any:
    destination = _socket_destination(sock, address)
    if destination is not None:
        authorize_egress(
            EgressRequest(destination=destination, purpose="socket connection")
        )
    return _original_socket_connect(sock, address)


def _guarded_connect_ex(sock: _socket.socket, address: _typing.Any) -> int:
    destination = _socket_destination(sock, address)
    if destination is not None:
        authorize_egress(
            EgressRequest(destination=destination, purpose="socket connection")
        )
    return _original_socket_connect_ex(sock, address)


def _guarded_sendto(sock: _socket.socket, data: _typing.Any, *args: _typing.Any) -> int:
    address = args[-1] if args else None
    destination = _socket_destination(sock, address)
    if destination is not None:
        authorize_egress(
            EgressRequest(
                destination=destination,
                purpose="datagram send",
                byte_count=len(data) if hasattr(data, "__len__") else None,
            )
        )
    return _original_socket_sendto(sock, data, *args)


def _guarded_create_connection(
    address: tuple[str, int], *args: _typing.Any, **kwargs: _typing.Any
) -> _socket.socket:
    authorize_egress(
        EgressRequest(destination=str(address[0]), purpose="socket connection")
    )
    return _original_create_connection(address, *args, **kwargs)


def _guarded_getaddrinfo(
    host: str | bytes | None,
    *args: _typing.Any,
    **kwargs: _typing.Any,
) -> list[tuple[_typing.Any, ...]]:
    if host is not None:
        destination = host.decode() if isinstance(host, bytes) else host
        authorize_egress(
            EgressRequest(destination=destination, purpose="DNS resolution")
        )
    return _original_getaddrinfo(host, *args, **kwargs)


def _install_socket_guard() -> None:
    if len(_process_scopes) == 1:
        _socket.socket.connect = _guarded_connect  # type: ignore[assignment]
        _socket.socket.connect_ex = _guarded_connect_ex  # type: ignore[assignment]
        _socket.socket.sendto = _guarded_sendto  # type: ignore[assignment]
        _socket.create_connection = _guarded_create_connection  # type: ignore[assignment]
        _socket.getaddrinfo = _guarded_getaddrinfo


def _remove_socket_guard() -> None:
    if not _process_scopes:
        _socket.socket.connect = _original_socket_connect  # type: ignore[assignment]
        _socket.socket.connect_ex = _original_socket_connect_ex  # type: ignore[assignment]
        _socket.socket.sendto = _original_socket_sendto  # type: ignore[assignment]
        _socket.create_connection = _original_create_connection
        _socket.getaddrinfo = _original_getaddrinfo


@_contextlib.contextmanager
def policy_scope(
    policy: EgressPolicy,
    *,
    audit_sink: _AuditSink | None = None,
) -> _typing.Iterator[None]:
    """Apply a policy to the current context and standard socket operations."""

    policy_token = _policy_var.set(policy)
    sink_token = _audit_sink_var.set(audit_sink)
    with _scope_lock:
        _process_scopes.append((policy, audit_sink))
        _install_socket_guard()
    try:
        yield
    finally:
        _audit_sink_var.reset(sink_token)
        _policy_var.reset(policy_token)
        with _scope_lock:
            for index in range(len(_process_scopes) - 1, -1, -1):
                if _process_scopes[index] == (policy, audit_sink):
                    del _process_scopes[index]
                    break
            _remove_socket_guard()


def _env_truthy(name: str) -> bool:
    value = _os.getenv(name)
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def policy_from_env(*, offline: bool = False) -> EgressPolicy:
    """Build a policy from Synor environment variables.

    Supported variables:

    - ``SYNOR_OFFLINE``
    - ``SYNOR_ALLOWED_HOSTS`` (comma-separated)
    - ``SYNOR_MAX_EGRESS_CLASSIFICATION``
    """

    network_access = (
        NetworkAccess.DENY
        if offline or _env_truthy("SYNOR_OFFLINE")
        else NetworkAccess.ALLOW
    )
    raw_hosts = _os.getenv("SYNOR_ALLOWED_HOSTS")
    allowed_hosts = (
        frozenset(part.strip() for part in raw_hosts.split(",") if part.strip())
        if raw_hosts is not None
        else None
    )
    raw_classification = _os.getenv(
        "SYNOR_MAX_EGRESS_CLASSIFICATION",
        DataClassification.RESTRICTED.value,
    )
    try:
        max_classification = DataClassification(raw_classification.strip().lower())
    except ValueError as error:
        allowed = ", ".join(item.value for item in DataClassification)
        raise ValueError(
            "SYNOR_MAX_EGRESS_CLASSIFICATION must be one of: " + allowed
        ) from error
    return EgressPolicy(
        network_access=network_access,
        allowed_hosts=allowed_hosts,
        max_data_classification=max_classification,
    )
