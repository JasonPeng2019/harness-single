"""Explicit role/provider/environment profiles for child processes."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping

from .provider import provider_registry


PROFILE_SCHEMA = "orchestrator-runtime-profile/v1"
_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")
_DENIED_PREFIXES = (
    "MCP_",
    "BYO_MCP_",
    "PYOCD_",
    "FIRMWARE_",
    "DEVICE_",
    "USB_",
    "JTAG_",
    "SWD_",
    "GPIO_",
    "PROBE_",
    "TARGET_",
    "SERIAL_",
    "OPENOCD_",
    "JLINK_",
    "CREDENTIAL_",
    "SECRET_",
    "TOKEN_",
    "AWS_",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
)
_DENIED_MARKERS = (
    "API_KEY",
    "ACCESS_KEY",
    "PRIVATE_KEY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "CREDENTIAL",
)
_BASE_ENV_ALLOW = frozenset(
    {
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "COMSPEC",
        "WINDIR",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "LANG",
        "LC_ALL",
        "PYTHONUTF8",
        "PYTHONIOENCODING",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    }
)


class ProfileError(ValueError):
    """Raised when a profile or environment grant is unsafe or malformed."""


def _denied_name(value: str) -> bool:
    return any(value.startswith(prefix) for prefix in _DENIED_PREFIXES) or any(
        marker in value for marker in _DENIED_MARKERS
    )


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProfileError(f"{name} must be a non-empty string")
    return value.strip()


def _strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ProfileError(f"{name} must be a list of non-empty strings")
    normalized = tuple(item.strip() for item in value)
    if len(normalized) != len(set(normalized)):
        raise ProfileError(f"{name} must not contain duplicates")
    return normalized


def _env_names(value: object, name: str) -> tuple[str, ...]:
    names = _strings(value, name)
    for item in names:
        if _ENV_NAME.fullmatch(item) is None:
            raise ProfileError(f"{name} contains an invalid environment variable name")
        if _denied_name(item):
            raise ProfileError(
                f"{name} cannot grant physical, MCP, or credential variable {item}"
            )
    return names


def _capability_names(value: object, name: str) -> tuple[str, ...]:
    names = _strings(value, name)
    if any(_denied_name(item) for item in names):
        raise ProfileError(
            f"{name} cannot grant physical, MCP, or credential capability"
        )
    return names


@dataclass(frozen=True)
class RuntimeProfile:
    profile_id: str
    role: str
    provider: str
    model: str
    tools: tuple[str, ...]
    capabilities: tuple[str, ...]
    resources: tuple[str, ...]
    provider_needs: tuple[str, ...] = ()
    workflow_grants: tuple[str, ...] = ()
    schema: str = PROFILE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != PROFILE_SCHEMA:
            raise ProfileError("unsupported profile schema")
        if self.provider not in provider_registry():
            raise ProfileError("profile provider is not a registered provider")
        _text(self.profile_id, "profile_id")
        _text(self.role, "role")
        _text(self.model, "model")
        _strings(list(self.tools), "tools")
        _capability_names(list(self.capabilities), "capabilities")
        _strings(list(self.resources), "resources")
        _env_names(list(self.provider_needs), "provider_needs")
        _capability_names(list(self.workflow_grants), "workflow_grants")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "RuntimeProfile":
        required = {
            "schema",
            "id",
            "role",
            "provider",
            "model",
            "tools",
            "capabilities",
            "resources",
        }
        optional = {"provider_needs", "workflow_grants"}
        if set(value) - required - optional or not required.issubset(value):
            raise ProfileError("profile has an invalid closed shape")
        return cls(
            schema=_text(value.get("schema"), "profile.schema"),
            profile_id=_text(value.get("id"), "profile.id"),
            role=_text(value.get("role"), "profile.role"),
            provider=_text(value.get("provider"), "profile.provider"),
            model=_text(value.get("model"), "profile.model"),
            tools=_strings(value.get("tools"), "profile.tools"),
            capabilities=_strings(value.get("capabilities"), "profile.capabilities"),
            resources=_strings(value.get("resources"), "profile.resources"),
            provider_needs=_env_names(
                value.get("provider_needs", []), "profile.provider_needs"
            ),
            workflow_grants=_capability_names(
                value.get("workflow_grants", []), "profile.workflow_grants"
            ),
        )

    def to_record(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "id": self.profile_id,
            "role": self.role,
            "provider": self.provider,
            "model": self.model,
            "tools": list(self.tools),
            "capabilities": list(self.capabilities),
            "resources": list(self.resources),
            "provider_needs": list(self.provider_needs),
            "workflow_grants": list(self.workflow_grants),
        }


def build_child_environment(
    profile: RuntimeProfile,
    inherited: Mapping[str, str] | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Allow only base runtime variables plus explicit safe profile grants."""

    source = dict(os.environ if inherited is None else inherited)
    allowed_names = (
        _BASE_ENV_ALLOW
        | set(profile.provider_needs)
        | {item for item in profile.workflow_grants if _ENV_NAME.fullmatch(item)}
    )
    allowed: dict[str, str] = {}
    cleared: list[str] = []
    for key, value in source.items():
        upper = key.upper()
        if upper in allowed_names and not _denied_name(upper):
            allowed[key] = value
        else:
            cleared.append(key)
    return allowed, sorted(cleared)


RoleProfile = RuntimeProfile
EnvironmentProfile = RuntimeProfile


__all__ = [
    "EnvironmentProfile",
    "PROFILE_SCHEMA",
    "ProfileError",
    "RoleProfile",
    "RuntimeProfile",
    "build_child_environment",
]
