from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, field

_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True, slots=True)
class IntegrityProfile:
    """Versioned rules for one exact source-to-target integrity mapping.

    Inspectors normalize both sides to the same source identity digest. The
    profile controls comparison rules and re-keys every identifier exported in
    reports. Keep ``report_key`` secret and stable for reports that must be
    correlated across scans.
    """

    name: str
    version: str
    report_key: bytes = field(repr=False)
    compare_revisions: bool = True
    compare_content: bool = False
    minimum_targets: int = 1

    def __post_init__(self) -> None:
        for field_name, value in (("name", self.name), ("version", self.version)):
            if not isinstance(value, str) or _SAFE_NAME.fullmatch(value) is None:
                raise ValueError(f"{field_name} must be a safe lowercase identifier")
        if not isinstance(self.report_key, bytes) or len(self.report_key) < 32:
            raise ValueError("report_key must contain at least 32 bytes")
        if type(self.compare_revisions) is not bool:
            raise TypeError("compare_revisions must be a bool")
        if type(self.compare_content) is not bool:
            raise TypeError("compare_content must be a bool")
        if (
            not isinstance(self.minimum_targets, int)
            or isinstance(self.minimum_targets, bool)
            or self.minimum_targets < 1
        ):
            raise ValueError("minimum_targets must be a positive integer")

    @classmethod
    def from_hex_key(
        cls,
        *,
        name: str,
        version: str,
        report_key_hex: str,
        compare_revisions: bool = True,
        compare_content: bool = False,
        minimum_targets: int = 1,
    ) -> IntegrityProfile:
        """Construct a profile from a 64-or-more-character hexadecimal key."""

        try:
            key = bytes.fromhex(report_key_hex)
        except (TypeError, ValueError):
            raise ValueError("report_key_hex must be valid hexadecimal") from None
        return cls(
            name=name,
            version=version,
            report_key=key,
            compare_revisions=compare_revisions,
            compare_content=compare_content,
            minimum_targets=minimum_targets,
        )

    def digest(self) -> str:
        """Return a resume-safe digest of the rules and key identity."""

        payload = {
            "name": self.name,
            "version": self.version,
            "report_key_fingerprint": hashlib.sha256(self.report_key).hexdigest(),
            "compare_revisions": self.compare_revisions,
            "compare_content": self.compare_content,
            "minimum_targets": self.minimum_targets,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def export_digest(self, domain: str, value: str) -> str:
        """Return a domain-separated keyed digest safe for report export."""

        if not isinstance(domain, str) or _SAFE_NAME.fullmatch(domain) is None:
            raise ValueError("domain must be a safe lowercase identifier")
        if not isinstance(value, str) or not value:
            raise ValueError("value must be a non-empty string")
        payload = b"synor-integrity-export-v1\x00" + domain.encode("ascii")
        payload += b"\x00" + value.encode("utf-8")
        return hmac.new(self.report_key, payload, hashlib.sha256).hexdigest()
