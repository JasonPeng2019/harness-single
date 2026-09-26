"""Known-secret and control-credential guards for derived product payloads.

The authoritative task source is never rewritten.  Mandatory content fails
closed when it contains a configured control credential; optional content is
sanitized or omitted by the caller.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

REDACTION_MARKER = "[REDACTED]"
_UNICODE_ESCAPE = re.compile(r"\\u([0-9a-fA-F]{4})")
_ASSIGNMENT = re.compile(
    r"(?<![A-Za-z0-9_.-])"
    r"(?P<key>[A-Za-z][A-Za-z0-9]*(?:[ \t_.-]+[A-Za-z0-9]+){0,7})"
    r"[ \t]*[\"']?[ \t]*[:=]"
)
_CREDENTIAL_WORDS = {"password", "passwd", "secret", "credential", "authorization", "bearer", "privatekey"}
_AUTHORITY_ACTIONS = {"approve", "approval", "publish", "publication", "revoke", "revocation",
                      "mutate", "mutation", "policy", "control"}
_AUTHORITY_GRANTS = {"allow", "enabled", "enable", "may", "can", "grant", "write", "authority", "permission"}
_SAFE_AUTHORITY = {"none", "excluded", "historical_evidence_only", "evidence_only", "false"}
_SAFE_SCALAR = re.compile(
    "|".join(re.escape(token) for token in sorted(_SAFE_AUTHORITY | {"0", "null"}, key=len, reverse=True)),
    re.IGNORECASE,
)
_CONCEPTS = tuple(sorted(_CREDENTIAL_WORDS | _AUTHORITY_ACTIONS | _AUTHORITY_GRANTS | {
    "api", "access", "refresh", "root", "role", "token", "manager", "admin", "execute", "parent",
    "memory", "plane", "private", "key", "approval", "publication", "mutation",
}, key=len, reverse=True))


class PrivacyError(ValueError):
    """Base class for privacy-boundary failures."""


class MandatorySecretError(PrivacyError):
    """A prohibited secret appeared in mandatory worker content."""


class RemotePayloadPrivacyError(PrivacyError):
    """A reusable remote payload contains a configured secret or sensitive value."""


@dataclass(frozen=True)
class PrivacyPolicy:
    known_secrets: tuple[str, ...] = ()
    forbidden_environment_keys: tuple[str, ...] = (
        "MEMORY_HARNESS_CONTROL_TOKEN",
        "MEMORY_HARNESS_POLICY_TOKEN",
    )
    # Populated only from an authenticated coordinator/source-owner boundary.
    # A record's self-asserted issuer, schema, or hash never populates this set.
    trusted_approval_hashes: tuple[str, ...] = ()

    def detect(self, value: Any) -> list[str]:
        return _find_known_secrets(value, self.known_secrets)


def _find_known_secrets(value: Any, known_secrets: tuple[str, ...]) -> list[str]:
    if not known_secrets:
        return []
    findings: list[str] = []
    serialized = _decode_unicode(_stringify(value))
    for secret in known_secrets:
        if secret and secret in serialized:
            findings.append(secret)
    return findings


def _stringify(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    except TypeError:
        return str(value)


def _decode_unicode(value: str) -> str:
    return _UNICODE_ESCAPE.sub(lambda match: chr(int(match.group(1), 16)), value)


def _structured_views(value: str) -> list[tuple[list[Any], str]]:
    """Parse each bounded JSON span once; leave only prose for assignment scanning."""
    views = (value, re.sub(r'\\(["\\])', r'\1', value))
    decoder = json.JSONDecoder()
    result: list[tuple[list[Any], str]] = []
    for view in views:
        fragments: list[Any] = []
        residual = list(view)
        index = 0
        while index < len(view):
            if view[index] not in "{[":
                index += 1
                continue
            try:
                parsed, end = decoder.raw_decode(view, index)
            except ValueError:
                index += 1
                continue
            if isinstance(parsed, (dict, list)):
                fragments.append(parsed)
                residual[index:end] = " " * (end - index)
                index = end
            else:
                index += 1
        result.append((fragments, "".join(residual)))
    return result


def _canonical_key(key: str) -> str:
    return "".join(char for char in _decode_unicode(key).casefold() if char.isascii() and char.isalnum())


def _concepts(key: str) -> list[str]:
    """Find governed concepts in canonical identity regardless of case or separators."""
    result: list[str] = []
    index = 0
    while index < len(key):
        concept = next((word for word in _CONCEPTS if key.startswith(word, index)), None)
        if concept:
            result.append(concept)
            index += len(concept)
        else:
            index += 1
    return result


def _key_kind(key: str) -> str | None:
    normalized = _canonical_key(key)
    if len(normalized) > 256:
        return "credential"
    if normalized == "taskcredentialchannel":
        return "task_channel"
    if normalized in {"rolelabel", "sharedpublication"}:
        return None
    words = _concepts(normalized)
    concepts = set(words)
    if concepts & _CREDENTIAL_WORDS or ("key" in concepts and concepts & {"api", "private"}):
        return "credential"
    if "token" in concepts and concepts & {"api", "access", "refresh", "root", "approval", "approve",
                                               "control", "policy", "publish", "manager", "admin"}:
        return "credential"
    if {"role", "enabled"} <= concepts and concepts & {"root", "manager", "admin", "control"}:
        return "authority"
    if concepts & _AUTHORITY_ACTIONS and concepts & _AUTHORITY_GRANTS:
        return "authority"
    if {"memory", "authority"} <= concepts or {"may", "execute", "parent"} <= concepts:
        return "authority"
    if normalized == "authority":
        return "authority_value"
    return None


def _safe_authority_value(value: Any) -> bool:
    return value is None or value is False or value == 0 or (
        isinstance(value, str) and value.casefold() in _SAFE_AUTHORITY | {"0", "null"}
    )


def _safe_assignment_scalar(text: str, start: int) -> bool:
    """Accept only an exact safe scalar at a real value boundary."""
    index = start
    while index < len(text) and text[index] in " \t":
        index += 1
    opening = text[index] if index < len(text) and text[index] in "\"'" else None
    if opening:
        index += 1
    match = _SAFE_SCALAR.match(text, index)
    if match is None:
        return False
    index = match.end()
    if opening:
        if index >= len(text) or text[index] != opening:
            return False
        index += 1
    trailer = ""
    while len(trailer) < 8 and index < len(text) and text[index] in ")]}.?!,;\"'":
        trailer += text[index]
        index += 1
    return (index == len(text) or text[index].isspace()
            or (trailer.endswith((",", ";")) and _ASSIGNMENT.match(text, index) is not None))


def _residual_assignments(text: str):
    """Yield governed assignments from at most eight adjacent key components."""
    for match in _ASSIGNMENT.finditer(text):
        components = re.findall(r"[A-Za-z0-9]+", match.group("key"))
        # An incidental prose prefix must not turn task-only channel metadata
        # into a generic credential field.
        channel = next((index for index in range(len(components))
                        if _key_kind("".join(components[index:])) == "task_channel"), None)
        if channel is not None:
            yield "task_channel", _safe_assignment_scalar(text, match.end())
            continue
        for index, component in enumerate(components):
            if not _concepts(_canonical_key(component)):
                continue
            kind = _key_kind("".join(components[index:]))
            if kind:
                yield kind, _safe_assignment_scalar(text, match.end())
                break


def _task_channel(value: Any) -> bool:
    return (isinstance(value, Mapping) and set(value) == {"scope", "validated", "channel_id"}
            and value.get("scope") == "task_only" and value.get("validated") is True
            and isinstance(value.get("channel_id"), str) and bool(value["channel_id"]))


def worker_bound_finding(value: Any, policy: PrivacyPolicy | None = None) -> str | None:
    """Classify bounded worker-visible meaning without returning sensitive values."""
    selected = policy or PrivacyPolicy()
    seen = 0

    def visit(item: Any, depth: int = 0, coordinator_approval: bool = False) -> str | None:
        nonlocal seen
        seen += 1
        if depth > 24 or seen > 10000:
            return "scan limit"
        if isinstance(item, Mapping):
            if item.get("content_hash") in selected.trusted_approval_hashes:
                try:
                    from . import contracts
                    if item.get("schema") == contracts.PROCEDURE_APPROVAL_SCHEMA:
                        contracts.validate_procedure_approval(item)
                        coordinator_approval = True
                    elif item.get("schema") == contracts.PROCEDURE_COMPACT_APPROVAL_SCHEMA:
                        contracts.validate_procedure_compact_approval(item)
                        coordinator_approval = True
                except (ValueError, TypeError, KeyError):
                    pass
            approved_pair = False
            approved_compact = False
            pair = item.get("content") if isinstance(item.get("content"), Mapping) else item
            if isinstance(pair.get("procedure"), Mapping) and isinstance(pair.get("approval"), Mapping):
                try:
                    from . import contracts
                    contracts.validate_procedure_approval(pair["approval"], procedure=pair["procedure"])
                    approved_pair = pair["approval"]["content_hash"] in selected.trusted_approval_hashes
                    if isinstance(item.get("compact_representation"), Mapping) and isinstance(item.get("compact_approval"), Mapping):
                        contracts.validate_procedure_compact_approval(
                            item["compact_approval"], procedure=pair["procedure"],
                            full_approval=pair["approval"], representation=item["compact_representation"],
                        )
                        approved_compact = item["compact_approval"]["content_hash"] in selected.trusted_approval_hashes
                except (ValueError, TypeError, KeyError):
                    pass
            for key, child in item.items():
                if not isinstance(key, str):
                    return "non-text key"
                if coordinator_approval and key == "authority_evidence":
                    if _find_known_secrets(child, selected.known_secrets) or _credential_field_finding(child):
                        return "credential in approval evidence"
                    continue
                kind = _key_kind(key)
                if kind == "task_channel":
                    if not _task_channel(child):
                        return "invalid task credential channel"
                    finding = visit(child, depth + 1)
                    if finding:
                        return finding
                    continue
                if kind == "credential":
                    return "credential field"
                if kind == "authority" and not _safe_authority_value(child):
                    return "worker authority field"
                if kind == "authority_value" and not _safe_authority_value(child):
                    return "worker authority field"
                finding = visit(child, depth + 1, (approved_pair and key == "approval")
                                or (approved_compact and key == "compact_approval"))
                if finding:
                    return finding
            return None
        if isinstance(item, (list, tuple)):
            for child in item:
                finding = visit(child, depth + 1)
                if finding:
                    return finding
            return None
        if isinstance(item, str):
            decoded = _decode_unicode(item)
            if len(decoded) > 65536 or decoded.count("{") + decoded.count("[") > 64:
                return "scan limit"
            if _find_known_secrets(decoded, selected.known_secrets):
                return "configured secret"
            for fragments, residual in _structured_views(decoded):
                for fragment in fragments:
                    finding = visit(fragment, depth + 1)
                    if finding:
                        return finding
                for kind, safe_scalar in _residual_assignments(residual):
                    if kind in {"credential", "task_channel"} or (kind in {"authority", "authority_value"} and not safe_scalar):
                        return "worker assignment"
        return None

    return visit(value)


def guard_worker_bound(value: Any, policy: PrivacyPolicy | None = None) -> None:
    """Reject worker-bound task/body/search/configuration meaning before use."""
    finding = worker_bound_finding(value, policy)
    if finding:
        raise MandatorySecretError(f"worker-bound content contains prohibited {finding}")


def guard_worker_bound_remote(value: Any, policy: PrivacyPolicy | None = None) -> None:
    """Lane-4 callable: check procedure behavior.body or representation.search_text."""
    if worker_bound_finding(value, policy):
        raise RemotePayloadPrivacyError("remote worker-bound content contains prohibited meaning")


def sanitize_text(text: str, policy: PrivacyPolicy) -> str:
    sanitized = _decode_unicode(text)
    for secret in policy.known_secrets:
        if secret:
            sanitized = sanitized.replace(secret, REDACTION_MARKER)
    return REDACTION_MARKER if worker_bound_finding(sanitized, policy) else sanitized


def sanitize_payload(value: Any, policy: PrivacyPolicy) -> Any:
    """Recursively sanitize known secret values while preserving JSON shape."""

    if isinstance(value, str):
        return sanitize_text(value, policy)
    if isinstance(value, Mapping):
        return {str(key): sanitize_payload(item, policy) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_payload(item, policy) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_payload(item, policy) for item in value)
    return value


def detect_secrets(value: Any, policy: PrivacyPolicy) -> list[str]:
    return policy.detect(value)


def guard_mandatory(value: Any, policy: PrivacyPolicy) -> None:
    guard_worker_bound(value, policy)


def sanitize_optional(value: Any, policy: PrivacyPolicy) -> Any:
    return None if worker_bound_finding(value, policy) else value


def guard_remote_payload(value: Any, policy: PrivacyPolicy | None = None) -> None:
    """Reject remote egress rather than silently mutate approved meaning.

    Optional historical evidence can be redacted before it is approved.  Once
    a procedure revision is approved, replacing sensitive text under the same
    content identity would break the trust binding, so publication must stop
    until an explicitly sanitized/reapproved revision exists.
    """

    selected_policy = policy or PrivacyPolicy()
    if detect_secrets(value, selected_policy) or _credential_field_finding(value):
        raise RemotePayloadPrivacyError(
            "remote procedure payload contains a prohibited secret or sensitive value"
        )


def _credential_field_finding(value: Any) -> bool:
    """Scan whole publications for credentials while retaining approved authority."""
    if isinstance(value, Mapping):
        return any((isinstance(key, str) and _key_kind(key) == "credential")
                   or _credential_field_finding(child) for key, child in value.items())
    if isinstance(value, (list, tuple)):
        return any(_credential_field_finding(child) for child in value)
    if isinstance(value, str):
        decoded = _decode_unicode(value)
        if len(decoded) > 65536 or decoded.count("{") + decoded.count("[") > 64:
            return True
        for fragments, residual in _structured_views(decoded):
            if any(_credential_field_finding(fragment) for fragment in fragments):
                return True
            if any(kind == "credential" for kind, _safe in _residual_assignments(residual)):
                return True
        return False
    return False


def worker_environment(
    environment: Mapping[str, str], policy: PrivacyPolicy | None = None
) -> dict[str, str]:
    selected_policy = policy or PrivacyPolicy()
    result: dict[str, str] = {}
    for key, value in environment.items():
        if not isinstance(key, str) or key.upper() in {
            item.upper() for item in selected_policy.forbidden_environment_keys
        } or (key != "OPENAI_API_KEY" and (
            _key_kind(key) in {"credential", "authority", "authority_value", "task_channel"}
            or (key.upper().startswith("MEMORY_HARNESS_") and any(
                part in key.upper().split("_") for part in ("CONTROL", "POLICY", "PUBLISH", "APPROVAL", "ADMIN", "MANAGER")
            ))
        )):
            continue
        if key == "OPENAI_API_KEY":
            result[key] = value
        elif isinstance(value, str) and not worker_bound_finding(value, selected_policy):
            result[key] = value
    return result


def worker_prompt(
    task_text: str,
    plan_content: Mapping[str, Any] | Any,
    *,
    optional_content: Any = None,
    privacy_policy: PrivacyPolicy | None = None,
) -> str:
    policy = privacy_policy or PrivacyPolicy()
    guard_mandatory({"task": task_text, "plan": plan_content}, policy)
    parts = ["## Task", str(task_text), "## Accepted plan", _render(plan_content)]
    if optional_content is not None:
        selected_optional = (
            [item for item in optional_content if sanitize_optional(item, policy) is not None]
            if isinstance(optional_content, (list, tuple))
            else sanitize_optional(optional_content, policy)
        )
        if selected_optional is not None:
            parts.extend(["## Optional historical evidence", _render(selected_optional)])
    parts.append("## Execution boundary")
    parts.append("Do not approve, publish, revoke, or mutate product policy.")
    return "\n\n".join(parts) + "\n"


def _render(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2)


def safe_query_payload(value: Any, policy: PrivacyPolicy | None = None) -> Any:
    selected_policy = policy or PrivacyPolicy()
    if worker_bound_finding(value, selected_policy):
        raise RemotePayloadPrivacyError("query contains prohibited worker-bound meaning")
    return value


__all__ = [
    "PrivacyError",
    "MandatorySecretError",
    "RemotePayloadPrivacyError",
    "PrivacyPolicy",
    "REDACTION_MARKER",
    "detect_secrets",
    "guard_mandatory",
    "guard_worker_bound",
    "guard_worker_bound_remote",
    "guard_remote_payload",
    "safe_query_payload",
    "sanitize_optional",
    "sanitize_payload",
    "sanitize_text",
    "worker_environment",
    "worker_bound_finding",
    "worker_prompt",
]

