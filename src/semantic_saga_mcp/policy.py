from __future__ import annotations

import contextlib
import fnmatch
import json
import os
import urllib.error
import urllib.request
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol


class PolicyError(RuntimeError):
    """Policy configuration/evaluation failed or returned an unsafe result."""


_EFFECTS = {"allow", "deny", "require_approval"}
_RISK_ORDER = {"unknown": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
_DEFAULT_RISK_WEIGHTS = {"unknown": 3, "low": 1, "medium": 2, "high": 5, "critical": 10}


@dataclass(frozen=True)
class PolicySubject:
    tenant_id: str
    principal_id: str
    principal_type: str
    roles: tuple[str, ...]
    scopes: tuple[str, ...]
    authenticated: bool = True


_subject_context: ContextVar[PolicySubject | None] = ContextVar("semantic_saga_policy_subject", default=None)


@contextlib.contextmanager
def policy_subject_scope(subject: PolicySubject) -> Iterator[None]:
    token = _subject_context.set(subject)
    try:
        yield
    finally:
        _subject_context.reset(token)


def current_policy_subject() -> PolicySubject | None:
    return _subject_context.get()


@dataclass(frozen=True)
class PolicyDecision:
    decision_id: str
    effect: str
    reason: str
    revision: str
    backend: str
    matched_rules: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.effect not in _EFFECTS:
            raise PolicyError(f"Unsupported policy effect: {self.effect}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "effect": self.effect,
            "reason": self.reason,
            "revision": self.revision,
            "backend": self.backend,
            "matched_rules": list(self.matched_rules),
        }


class PolicyEngine(Protocol):
    def decide(self, context: Mapping[str, Any]) -> PolicyDecision: ...
    def status(self, tenant_id: str) -> dict[str, Any]: ...


def risk_name(action: Mapping[str, Any] | None) -> str:
    semantic = (action or {}).get("semantic")
    value = semantic.get("risk") if isinstance(semantic, Mapping) else None
    return str(value).lower() if isinstance(value, str) and value else "unknown"


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return tuple(value)
    if value is None:
        return ()
    raise PolicyError("Policy string-list fields must be a string or list of strings")


def _effect(value: Any, *, label: str) -> str:
    if value not in _EFFECTS:
        raise PolicyError(f"{label} must be one of {sorted(_EFFECTS)}")
    return str(value)


class NoopPolicyEngine:
    def decide(self, context: Mapping[str, Any]) -> PolicyDecision:
        return PolicyDecision(
            decision_id=str(uuid.uuid4()),
            effect="allow",
            reason="Governance policy is disabled",
            revision="disabled",
            backend="none",
        )

    def status(self, tenant_id: str) -> dict[str, Any]:
        return {"backend": "none", "revision": "disabled", "tenant_id": tenant_id, "enabled": False}


class JsonPolicyEngine:
    """Deterministic tenant-scoped policy engine with hard resource budgets.

    Budgets are evaluated before rules and therefore cannot be bypassed by an
    allow rule. Rules use first-match semantics: tenant-specific rules first,
    then wildcard rules. Policy inputs intentionally contain control-plane
    metadata only; action input/result payloads are never required.
    """

    def __init__(self, document: Mapping[str, Any]) -> None:
        self.document = dict(document)
        if self.document.get("schema_version") != 1:
            raise PolicyError("Policy document requires schema_version: 1")
        self.revision = str(self.document.get("revision") or "unversioned")
        self.default_effect = _effect(self.document.get("default_effect", "deny"), label="default_effect")
        weights = dict(_DEFAULT_RISK_WEIGHTS)
        custom_weights = self.document.get("risk_weights", {})
        if not isinstance(custom_weights, Mapping):
            raise PolicyError("risk_weights must be an object")
        for key, value in custom_weights.items():
            if not isinstance(value, int) or value < 0:
                raise PolicyError(f"risk_weights.{key} must be a non-negative integer")
            weights[str(key).lower()] = value
        self.risk_weights = weights
        tenants = self.document.get("tenants", {})
        if not isinstance(tenants, Mapping):
            raise PolicyError("tenants must be an object")
        self.tenants = {str(key): self._validate_tenant(value, str(key)) for key, value in tenants.items()}

    @classmethod
    def from_file(cls, path: str) -> "JsonPolicyEngine":
        try:
            document = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PolicyError(f"Unable to load governance policy {path}: {exc}") from exc
        if not isinstance(document, Mapping):
            raise PolicyError("Governance policy root must be an object")
        return cls(document)

    @staticmethod
    def _validate_tenant(value: Any, tenant_id: str) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise PolicyError(f"Tenant policy {tenant_id} must be an object")
        result = dict(value)
        budgets = result.get("budgets", {})
        if not isinstance(budgets, Mapping):
            raise PolicyError(f"Tenant policy {tenant_id}.budgets must be an object")
        for key in ("max_steps_per_saga", "max_planned_nodes", "max_risk_units", "max_parallel"):
            if key in budgets and (not isinstance(budgets[key], int) or budgets[key] < 1):
                raise PolicyError(f"Tenant policy {tenant_id}.budgets.{key} must be a positive integer")
        threshold = result.get("approval_at_or_above_risk")
        if threshold is not None and str(threshold).lower() not in _RISK_ORDER:
            raise PolicyError(f"Tenant policy {tenant_id}.approval_at_or_above_risk is invalid")
        if "default_effect" in result:
            _effect(result["default_effect"], label=f"tenants.{tenant_id}.default_effect")
        rules = result.get("rules", [])
        if not isinstance(rules, list):
            raise PolicyError(f"Tenant policy {tenant_id}.rules must be a list")
        ids: set[str] = set()
        for rule in rules:
            if not isinstance(rule, Mapping):
                raise PolicyError(f"Tenant policy {tenant_id} rules must be objects")
            rule_id = rule.get("id")
            if not isinstance(rule_id, str) or not rule_id:
                raise PolicyError(f"Tenant policy {tenant_id} rule requires a non-empty id")
            if rule_id in ids:
                raise PolicyError(f"Duplicate policy rule id for {tenant_id}: {rule_id}")
            ids.add(rule_id)
            _effect(rule.get("effect"), label=f"rule {rule_id}.effect")
            match = rule.get("match", {})
            if not isinstance(match, Mapping):
                raise PolicyError(f"rule {rule_id}.match must be an object")
            for field in (
                "actions", "phases", "risks", "domains", "operations", "resources",
                "principal_types", "roles_any", "roles_all", "roles_none", "scopes_any",
            ):
                _strings(match.get(field))
        return result

    def _effective(self, tenant_id: str) -> dict[str, Any]:
        wildcard = self.tenants.get("*", {})
        tenant = self.tenants.get(tenant_id, {})
        budgets = dict(wildcard.get("budgets", {}))
        budgets.update(tenant.get("budgets", {}))
        threshold = tenant.get("approval_at_or_above_risk", wildcard.get("approval_at_or_above_risk"))
        rules = list(tenant.get("rules", [])) + list(wildcard.get("rules", []))
        default_effect = tenant.get("default_effect", wildcard.get("default_effect", self.default_effect))
        return {
            "budgets": budgets,
            "approval_at_or_above_risk": str(threshold).lower() if threshold is not None else None,
            "rules": rules,
            "default_effect": default_effect,
        }

    @staticmethod
    def _field(context: Mapping[str, Any], name: str) -> str:
        action = context.get("action") if isinstance(context.get("action"), Mapping) else {}
        semantic = action.get("semantic") if isinstance(action.get("semantic"), Mapping) else {}
        values = {
            "actions": action.get("id"),
            "phases": context.get("phase"),
            "risks": semantic.get("risk", "unknown"),
            "domains": semantic.get("domain"),
            "operations": semantic.get("operation"),
            "resources": semantic.get("resource"),
            "principal_types": (context.get("principal") or {}).get("type") if isinstance(context.get("principal"), Mapping) else None,
        }
        value = values[name]
        return str(value) if value is not None else ""

    @classmethod
    def _matches(cls, match: Mapping[str, Any], context: Mapping[str, Any]) -> bool:
        for field in ("actions", "phases", "risks", "domains", "operations", "resources", "principal_types"):
            patterns = _strings(match.get(field))
            if patterns and not any(fnmatch.fnmatchcase(cls._field(context, field), pattern) for pattern in patterns):
                return False
        principal = context.get("principal") if isinstance(context.get("principal"), Mapping) else {}
        roles = {str(item).lower() for item in principal.get("roles", []) if isinstance(item, str)}
        scopes = {str(item) for item in principal.get("scopes", []) if isinstance(item, str)}
        roles_any = {item.lower() for item in _strings(match.get("roles_any"))}
        roles_all = {item.lower() for item in _strings(match.get("roles_all"))}
        roles_none = {item.lower() for item in _strings(match.get("roles_none"))}
        scopes_any = set(_strings(match.get("scopes_any")))
        if roles_any and not roles.intersection(roles_any):
            return False
        if roles_all and not roles_all.issubset(roles):
            return False
        if roles_none and roles.intersection(roles_none):
            return False
        if scopes_any and not scopes.intersection(scopes_any):
            return False
        return True

    def _budget_decision(self, effective: Mapping[str, Any], context: Mapping[str, Any]) -> PolicyDecision | None:
        budgets = effective.get("budgets", {})
        request = context.get("request") if isinstance(context.get("request"), Mapping) else {}
        checks = (
            ("max_steps_per_saga", request.get("prospective_steps"), "saga step budget"),
            ("max_planned_nodes", request.get("prospective_planned_nodes"), "planned-node budget"),
            ("max_risk_units", request.get("prospective_risk_units"), "risk-unit budget"),
            ("max_parallel", request.get("requested_max_parallel"), "parallelism budget"),
        )
        for key, value, label in checks:
            limit = budgets.get(key)
            if isinstance(limit, int) and isinstance(value, int) and value > limit:
                return PolicyDecision(
                    decision_id=str(uuid.uuid4()),
                    effect="deny",
                    reason=f"{label} exceeded: requested {value}, limit {limit}",
                    revision=self.revision,
                    backend="json",
                    matched_rules=(f"budget:{key}",),
                )
        return None

    def decide(self, context: Mapping[str, Any]) -> PolicyDecision:
        tenant_id = str(context.get("tenant_id") or "default")
        effective = self._effective(tenant_id)
        budget = self._budget_decision(effective, context)
        if budget is not None:
            return budget

        for rule in effective["rules"]:
            match = rule.get("match", {})
            if self._matches(match, context):
                effect = str(rule["effect"])
                if effect == "require_approval" and bool((context.get("request") or {}).get("approval_granted")):
                    effect = "allow"
                return PolicyDecision(
                    decision_id=str(uuid.uuid4()),
                    effect=effect,
                    reason=str(rule.get("reason") or f"Matched policy rule {rule['id']}"),
                    revision=self.revision,
                    backend="json",
                    matched_rules=(str(rule["id"]),),
                )

        threshold = effective.get("approval_at_or_above_risk")
        current_risk = risk_name(context.get("action") if isinstance(context.get("action"), Mapping) else None)
        if threshold is not None and _RISK_ORDER.get(current_risk, 0) >= _RISK_ORDER[threshold]:
            if not bool((context.get("request") or {}).get("approval_granted")):
                return PolicyDecision(
                    decision_id=str(uuid.uuid4()),
                    effect="require_approval",
                    reason=f"{current_risk} risk requires approval at threshold {threshold}",
                    revision=self.revision,
                    backend="json",
                    matched_rules=("approval-threshold",),
                )

        return PolicyDecision(
            decision_id=str(uuid.uuid4()),
            effect=str(effective["default_effect"]),
            reason="Tenant default policy effect",
            revision=self.revision,
            backend="json",
        )

    def status(self, tenant_id: str) -> dict[str, Any]:
        effective = self._effective(tenant_id)
        return {
            "enabled": True,
            "backend": "json",
            "revision": self.revision,
            "tenant_id": tenant_id,
            "default_effect": effective["default_effect"],
            "budgets": dict(effective["budgets"]),
            "approval_at_or_above_risk": effective["approval_at_or_above_risk"],
            "rule_ids": [str(rule["id"]) for rule in effective["rules"]],
            "risk_weights": dict(self.risk_weights),
        }


class OpaPolicyEngine:
    """Fail-closed OPA REST policy decision adapter.

    OPA receives only the already-sanitized governance context. The endpoint is
    the standard Data API path `/v1/data/<decision_path>`. A boolean result is
    accepted as allow/deny; object results may return effect/reason/revision and
    matched_rules.
    """

    def __init__(
        self,
        base_url: str,
        *,
        decision_path: str = "semantic_saga/decision",
        timeout_seconds: float = 2.0,
        bearer_token: str | None = None,
    ) -> None:
        if not base_url:
            raise PolicyError("OPA base URL is required")
        if timeout_seconds <= 0:
            raise PolicyError("OPA timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self.decision_path = decision_path.strip("/").replace(".", "/")
        self.timeout_seconds = timeout_seconds
        self.bearer_token = bearer_token

    @property
    def endpoint(self) -> str:
        return f"{self.base_url}/v1/data/{self.decision_path}"

    def decide(self, context: Mapping[str, Any]) -> PolicyDecision:
        payload = json.dumps({"input": dict(context)}, separators=(",", ":")).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        request = urllib.request.Request(self.endpoint, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                document = json.loads(response.read() or b"{}")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            raise PolicyError(f"OPA policy evaluation failed closed: {exc}") from exc
        if "result" not in document:
            return PolicyDecision(
                decision_id=str(uuid.uuid4()), effect="deny", reason="OPA decision was undefined",
                revision="unknown", backend="opa",
            )
        result = document["result"]
        if isinstance(result, bool):
            return PolicyDecision(
                decision_id=str(uuid.uuid4()), effect="allow" if result else "deny",
                reason="OPA boolean decision", revision="unknown", backend="opa",
            )
        if not isinstance(result, Mapping):
            raise PolicyError("OPA result must be a boolean or object")
        effect = result.get("effect")
        if effect is None and isinstance(result.get("allow"), bool):
            effect = "allow" if result["allow"] else "deny"
        effect = _effect(effect, label="OPA result.effect")
        rules = result.get("matched_rules", [])
        if not isinstance(rules, list) or not all(isinstance(item, str) for item in rules):
            raise PolicyError("OPA matched_rules must be a list of strings")
        return PolicyDecision(
            decision_id=str(result.get("decision_id") or uuid.uuid4()),
            effect=effect,
            reason=str(result.get("reason") or "OPA policy decision"),
            revision=str(result.get("revision") or "unknown"),
            backend="opa",
            matched_rules=tuple(rules),
        )

    def status(self, tenant_id: str) -> dict[str, Any]:
        return {
            "enabled": True,
            "backend": "opa",
            "tenant_id": tenant_id,
            "decision_path": self.decision_path,
            "endpoint": self.endpoint,
            "fail_closed": True,
        }


def load_policy_engine(
    mode: str,
    *,
    policy_file: str | None = None,
    opa_url: str | None = None,
    opa_decision_path: str = "semantic_saga/decision",
    opa_timeout_seconds: float = 2.0,
    opa_token_env: str = "SAGA_OPA_TOKEN",
) -> PolicyEngine:
    if mode == "none":
        return NoopPolicyEngine()
    if mode == "json":
        if not policy_file:
            raise PolicyError("JSON policy mode requires --policy-file")
        return JsonPolicyEngine.from_file(policy_file)
    if mode == "opa":
        if not opa_url:
            raise PolicyError("OPA policy mode requires --policy-opa-url")
        return OpaPolicyEngine(
            opa_url,
            decision_path=opa_decision_path,
            timeout_seconds=opa_timeout_seconds,
            bearer_token=os.getenv(opa_token_env) if opa_token_env else None,
        )
    raise PolicyError(f"Unsupported policy mode: {mode}")
