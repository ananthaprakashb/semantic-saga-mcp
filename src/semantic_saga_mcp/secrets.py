from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Protocol, runtime_checkable


class SecretResolutionError(RuntimeError):
    """A configured secret reference could not be resolved safely."""


@runtime_checkable
class SecretProvider(Protocol):
    """Resolve secret references without persisting secret material in action definitions."""

    def resolve(self, reference: str) -> str: ...


@dataclass(frozen=True)
class EnvironmentSecretProvider:
    """Resolve ``env://NAME`` references from the process environment."""

    environ: Mapping[str, str] | None = None

    def resolve(self, reference: str) -> str:
        if not reference.startswith("env://"):
            raise SecretResolutionError(
                f"Unsupported secret reference {reference!r}; the built-in provider accepts env://NAME"
            )
        name = reference.removeprefix("env://")
        if not name or any(ch.isspace() for ch in name):
            raise SecretResolutionError(f"Invalid environment secret reference: {reference!r}")
        source = self.environ if self.environ is not None else os.environ
        value = source.get(name)
        if value is None:
            raise SecretResolutionError(f"Environment secret is not configured: {name}")
        return value


@dataclass(frozen=True)
class MappingSecretProvider:
    """Small deterministic provider useful for embedding adapters and tests."""

    values: Mapping[str, str]

    def resolve(self, reference: str) -> str:
        try:
            return self.values[reference]
        except KeyError as exc:
            raise SecretResolutionError(f"Secret reference is not configured: {reference}") from exc


def resolve_secret_value(value: object, provider: SecretProvider | None) -> str:
    """Resolve a header value, including structured secret references.

    A secret-bearing header uses a JSON object such as::

        {"secret_ref": "env://GITHUB_TOKEN", "prefix": "Bearer "}

    The action snapshot stores only the reference, never the resolved value.
    """

    if isinstance(value, str):
        return value
    if not isinstance(value, dict) or set(value) - {"secret_ref", "prefix", "suffix"}:
        raise SecretResolutionError("HTTP header values must be strings or secret_ref objects")
    reference = value.get("secret_ref")
    prefix = value.get("prefix", "")
    suffix = value.get("suffix", "")
    if not isinstance(reference, str) or not reference:
        raise SecretResolutionError("secret_ref must be a non-empty string")
    if not isinstance(prefix, str) or not isinstance(suffix, str):
        raise SecretResolutionError("secret_ref prefix and suffix must be strings")
    if provider is None:
        raise SecretResolutionError(f"No secret provider is configured for {reference}")
    return f"{prefix}{provider.resolve(reference)}{suffix}"
