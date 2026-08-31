from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError as JsonSchemaValidationError

from .actions import Action, DryRunHttpAction, HttpAction, HttpRequest
from .engine import EnginePolicyError, normalize_execution_policy
from .secrets import EnvironmentSecretProvider, SecretProvider


class ActionRegistryError(RuntimeError):
    """An action contract is missing, invalid, or unsafe to recover."""


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def definition_hash(snapshot: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(snapshot).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ActionDefinition:
    action_id: str
    version: str
    kind: str
    semantic: dict[str, Any] = field(default_factory=dict)
    input_schema: dict[str, Any] = field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] | None = None
    forward: dict[str, Any] | None = None
    compensation: dict[str, Any] | None = None
    implementation: str | None = None
    implementation_hash: str | None = None
    execution_policy: dict[str, Any] | None = None
    active: bool = True

    def __post_init__(self) -> None:
        if not self.action_id or not self.version:
            raise ActionRegistryError("Action id and version must be non-empty")
        if self.kind not in {"http", "runtime"}:
            raise ActionRegistryError(f"Unsupported action kind: {self.kind}")
        if self.kind == "http" and (not isinstance(self.forward, dict) or not isinstance(self.compensation, dict)):
            raise ActionRegistryError(f"HTTP action {self.action_id}@{self.version} requires forward and compensation")
        try:
            Draft202012Validator.check_schema(self.input_schema)
            if self.output_schema is not None:
                Draft202012Validator.check_schema(self.output_schema)
            if self.execution_policy is not None:
                normalize_execution_policy(self.execution_policy)
        except SchemaError as exc:
            raise ActionRegistryError(f"Invalid JSON Schema for {self.action_id}@{self.version}: {exc.message}") from exc
        except EnginePolicyError as exc:
            raise ActionRegistryError(f"Invalid execution policy for {self.action_id}@{self.version}: {exc}") from exc

    @property
    def policy(self) -> dict[str, Any]:
        return normalize_execution_policy(self.execution_policy)

    def snapshot(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "id": self.action_id,
            "version": self.version,
            "kind": self.kind,
            "semantic": self.semantic,
            "input_schema": self.input_schema,
        }
        if self.output_schema is not None:
            value["output_schema"] = self.output_schema
        if self.forward is not None:
            value["forward"] = self.forward
        if self.compensation is not None:
            value["compensation"] = self.compensation
        if self.implementation is not None:
            value["implementation"] = self.implementation
        if self.implementation_hash is not None:
            value["implementation_hash"] = self.implementation_hash
        if self.execution_policy is not None:
            value["execution_policy"] = self.execution_policy
        return value

    @property
    def hash(self) -> str:
        return definition_hash(self.snapshot())

    @classmethod
    def from_snapshot(cls, value: Mapping[str, Any], *, active: bool = True) -> "ActionDefinition":
        try:
            return cls(
                action_id=str(value["id"]),
                version=str(value["version"]),
                kind=str(value["kind"]),
                semantic=dict(value.get("semantic", {})),
                input_schema=dict(value.get("input_schema", {"type": "object"})),
                output_schema=dict(value["output_schema"]) if value.get("output_schema") is not None else None,
                forward=dict(value["forward"]) if value.get("forward") is not None else None,
                compensation=dict(value["compensation"]) if value.get("compensation") is not None else None,
                implementation=str(value["implementation"]) if value.get("implementation") is not None else None,
                implementation_hash=str(value["implementation_hash"]) if value.get("implementation_hash") is not None else None,
                execution_policy=dict(value["execution_policy"]) if value.get("execution_policy") is not None else None,
                active=active,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActionRegistryError(f"Invalid persisted action definition: {exc}") from exc


@dataclass(frozen=True)
class RegisteredAction:
    definition: ActionDefinition
    action: Action


class ActionRegistry:
    """Immutable, version-addressable action contracts with safe recovery semantics."""

    def __init__(
        self,
        *,
        secret_provider: SecretProvider | None = None,
        dry_run: bool = False,
        log: Callable[[str], None] | None = None,
        allow_legacy_recovery: bool = False,
    ) -> None:
        self.secret_provider = secret_provider or EnvironmentSecretProvider()
        self.dry_run = dry_run
        self.log = log or (lambda _message: None)
        self.allow_legacy_recovery = allow_legacy_recovery
        self._versions: dict[tuple[str, str], RegisteredAction] = {}
        self._active: dict[str, str] = {}

    def _build_http(self, definition: ActionDefinition) -> Action:
        assert definition.forward is not None and definition.compensation is not None
        action = HttpAction(
            HttpRequest(secret_provider=self.secret_provider, **definition.forward),
            HttpRequest(secret_provider=self.secret_provider, **definition.compensation),
        )
        return DryRunHttpAction(action, self.log) if self.dry_run else action

    def register_definition(self, definition: ActionDefinition) -> None:
        key = (definition.action_id, definition.version)
        action = self._build_http(definition) if definition.kind == "http" else None
        if action is None:
            raise ActionRegistryError("Runtime definitions must be registered with register_runtime")
        existing = self._versions.get(key)
        if existing and existing.definition.hash != definition.hash:
            raise ActionRegistryError(f"Action version is immutable: {definition.action_id}@{definition.version}")
        self._versions[key] = RegisteredAction(definition, action)
        if definition.active:
            previous = self._active.get(definition.action_id)
            if previous is not None and previous != definition.version:
                raise ActionRegistryError(
                    f"Multiple active versions configured for {definition.action_id}: {previous}, {definition.version}"
                )
            self._active[definition.action_id] = definition.version

    @staticmethod
    def _runtime_implementation(action: Action) -> tuple[str, str]:
        cls = type(action)
        name = f"{cls.__module__}.{cls.__qualname__}"
        try:
            source = inspect.getsource(cls)
        except (OSError, TypeError):
            source = name
        return name, hashlib.sha256(source.encode("utf-8")).hexdigest()

    def register_runtime(
        self,
        action_id: str,
        action: Action,
        *,
        version: str = "runtime-v1",
        semantic: dict[str, Any] | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        execution_policy: dict[str, Any] | None = None,
        active: bool = True,
    ) -> None:
        implementation, implementation_hash = self._runtime_implementation(action)
        definition = ActionDefinition(
            action_id=action_id,
            version=version,
            kind="runtime",
            semantic=semantic or {"reversibility": "implementation-defined", "risk": "unknown"},
            input_schema=input_schema or {"type": "object"},
            output_schema=output_schema,
            implementation=implementation,
            implementation_hash=implementation_hash,
            execution_policy=execution_policy,
            active=active,
        )
        key = (action_id, version)
        existing = self._versions.get(key)
        if existing and existing.definition.hash != definition.hash:
            raise ActionRegistryError(f"Action version is immutable: {action_id}@{version}")
        self._versions[key] = RegisteredAction(definition, action)
        if active:
            previous = self._active.get(action_id)
            if previous is not None and previous != version:
                raise ActionRegistryError(f"Multiple active versions configured for {action_id}: {previous}, {version}")
            self._active[action_id] = version

    def resolve(self, action_id: str, version: str | None = None) -> RegisteredAction:
        selected = version or self._active.get(action_id)
        if selected is None:
            raise ActionRegistryError(f"Unknown action: {action_id}")
        try:
            return self._versions[(action_id, selected)]
        except KeyError as exc:
            raise ActionRegistryError(f"Action version unavailable: {action_id}@{selected}") from exc

    @staticmethod
    def _validate(schema: dict[str, Any] | None, value: Any, *, label: str) -> None:
        if schema is None:
            return
        try:
            Draft202012Validator(schema).validate(value)
        except JsonSchemaValidationError as exc:
            path = ".".join(str(item) for item in exc.absolute_path)
            suffix = f" at {path}" if path else ""
            raise ActionRegistryError(f"{label} schema validation failed{suffix}: {exc.message}") from exc

    def prepare(self, action_id: str, values: dict[str, Any]) -> RegisteredAction:
        registered = self.resolve(action_id)
        self._validate(registered.definition.input_schema, values, label=f"{action_id} input")
        return registered

    def validate_output(self, definition: ActionDefinition, value: Any) -> None:
        self._validate(definition.output_schema, value, label=f"{definition.action_id} output")

    def resolve_for_step(self, step: Mapping[str, Any]) -> RegisteredAction:
        snapshot = step.get("action_definition")
        stored_hash = step.get("action_definition_hash")
        stored_version = step.get("action_version")
        action_id = str(step.get("action", ""))
        if snapshot is None or stored_hash is None or stored_version is None:
            if not self.allow_legacy_recovery:
                raise ActionRegistryError(
                    f"Step {step.get('id')} predates immutable action snapshots; "
                    "enable legacy recovery explicitly or recover it with the pre-upgrade release"
                )
            return self.resolve(action_id)
        if not isinstance(snapshot, dict) or not isinstance(stored_hash, str) or not isinstance(stored_version, str):
            raise ActionRegistryError(f"Step {step.get('id')} has invalid persisted action metadata")
        actual_hash = definition_hash(snapshot)
        if actual_hash != stored_hash:
            raise ActionRegistryError(f"Step {step.get('id')} action definition hash mismatch")
        definition = ActionDefinition.from_snapshot(snapshot)
        if definition.action_id != action_id or definition.version != stored_version:
            raise ActionRegistryError(f"Step {step.get('id')} action identity does not match its persisted snapshot")
        if definition.kind == "http":
            return RegisteredAction(definition, self._build_http(definition))
        current = self.resolve(definition.action_id, definition.version)
        if current.definition.hash != stored_hash:
            raise ActionRegistryError(
                f"Runtime action {definition.action_id}@{definition.version} implementation changed; "
                "refusing unsafe compensation"
            )
        return current

    def list_definitions(self) -> list[dict[str, Any]]:
        result = []
        for action_id in sorted(self._active):
            registered = self.resolve(action_id)
            definition = registered.definition
            result.append(
                {
                    "id": definition.action_id,
                    "version": definition.version,
                    "kind": definition.kind,
                    "definition_hash": definition.hash,
                    "semantic": definition.semantic,
                    "input_schema": definition.input_schema,
                    "output_schema": definition.output_schema,
                    "execution_policy": definition.policy,
                }
            )
        return result

    def get_definition(self, action_id: str, version: str | None = None) -> dict[str, Any]:
        definition = self.resolve(action_id, version).definition
        value = definition.snapshot()
        value["definition_hash"] = definition.hash
        value["resolved_execution_policy"] = definition.policy
        return value


def _legacy_definition(action_id: str, raw: Mapping[str, Any]) -> ActionDefinition:
    return ActionDefinition(
        action_id=action_id,
        version="legacy-v1",
        kind="http",
        semantic={"domain": "unspecified", "operation": action_id, "reversibility": "configured", "risk": "unknown"},
        input_schema={"type": "object"},
        forward=dict(raw["forward"]),
        compensation=dict(raw["rollback"]),
    )


def load_action_registry(
    path: str | None,
    *,
    secret_provider: SecretProvider | None = None,
    dry_run: bool = False,
    log: Callable[[str], None] | None = None,
    allow_legacy_recovery: bool = False,
) -> ActionRegistry:
    registry = ActionRegistry(
        secret_provider=secret_provider,
        dry_run=dry_run,
        log=log,
        allow_legacy_recovery=allow_legacy_recovery,
    )
    if path is None:
        return registry
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ActionRegistryError("Action registry file must be a JSON object")
    if "actions" not in raw:
        for action_id, definition in raw.items():
            if not isinstance(action_id, str) or not isinstance(definition, dict):
                raise ActionRegistryError("Legacy action entries must map names to objects")
            registry.register_definition(_legacy_definition(action_id, definition))
        return registry
    actions = raw.get("actions")
    if raw.get("schema_version", 1) != 1 or not isinstance(actions, list):
        raise ActionRegistryError("Action registry requires schema_version 1 and an actions array")
    for item in actions:
        if not isinstance(item, dict):
            raise ActionRegistryError("Each action definition must be an object")
        definition = ActionDefinition(
            action_id=str(item.get("id", "")),
            version=str(item.get("version", "")),
            kind=str(item.get("kind", "http")),
            semantic=dict(item.get("semantic", {})),
            input_schema=dict(item.get("input_schema", {"type": "object"})),
            output_schema=dict(item["output_schema"]) if item.get("output_schema") is not None else None,
            forward=dict(item["forward"]) if item.get("forward") is not None else None,
            compensation=dict(item["compensation"]) if item.get("compensation") is not None else None,
            execution_policy=dict(item["execution_policy"]) if item.get("execution_policy") is not None else None,
            active=bool(item.get("active", True)),
        )
        registry.register_definition(definition)
    return registry
