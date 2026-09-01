from __future__ import annotations

import uuid
from typing import Any, Mapping

from .coordinator import SagaError
from .engine import engine_state
from .instrumented_coordinator import InstrumentedCoordinator
from .observability import current_actor, monotonic_seconds
from .policy import (
    NoopPolicyEngine,
    PolicyDecision,
    PolicyEngine,
    PolicyError,
    PolicySubject,
    current_policy_subject,
    risk_name,
)
from .registry import ActionDefinition, ActionRegistryError, RegisteredAction


_DEFAULT_RISK_WEIGHTS = {"unknown": 3, "low": 1, "medium": 2, "high": 5, "critical": 10}


class GovernedCoordinator(InstrumentedCoordinator):
    """Policy enforcement around the durable, observable saga engine.

    Policy evaluation receives identity, semantic action metadata, state counts,
    and control-plane parameters only. Action input/result payloads are excluded.
    Rollback remains available as the safety operation even when business policy
    rejects new forward work.
    """

    def __init__(self, *args: Any, policy_engine: PolicyEngine | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.policy_engine = policy_engine or NoopPolicyEngine()

    def _subject(self, saga: Mapping[str, Any] | None = None) -> PolicySubject:
        current = current_policy_subject()
        if current is not None:
            return current
        principal_id, principal_type = current_actor()
        return PolicySubject(
            tenant_id=str((saga or {}).get("tenant_id") or "default"),
            principal_id=principal_id or str((saga or {}).get("creator_principal_id") or "unknown"),
            principal_type=principal_type or "unknown",
            roles=(),
            scopes=(),
            authenticated=False,
        )

    @staticmethod
    def _action_view(definition: ActionDefinition | None) -> dict[str, Any] | None:
        if definition is None:
            return None
        return {
            "id": definition.action_id,
            "version": definition.version,
            "kind": definition.kind,
            "semantic": dict(definition.semantic),
        }

    @staticmethod
    def _action_view_from_snapshot(snapshot: Any) -> dict[str, Any] | None:
        if not isinstance(snapshot, Mapping):
            return None
        semantic = snapshot.get("semantic")
        return {
            "id": str(snapshot.get("id") or ""),
            "version": str(snapshot.get("version") or ""),
            "kind": str(snapshot.get("kind") or ""),
            "semantic": dict(semantic) if isinstance(semantic, Mapping) else {},
        }

    def _risk_weight(self, action: Mapping[str, Any] | None) -> int:
        weights = getattr(self.policy_engine, "risk_weights", _DEFAULT_RISK_WEIGHTS)
        if not isinstance(weights, Mapping):
            weights = _DEFAULT_RISK_WEIGHTS
        value = weights.get(risk_name(action), weights.get("unknown", 3))
        return int(value) if isinstance(value, int) and value >= 0 else 3

    def _usage(self, saga: Mapping[str, Any]) -> dict[str, int]:
        steps = self.store.list_steps(str(saga["id"]))
        metadata = saga.get("metadata") if isinstance(saga.get("metadata"), Mapping) else {}
        nodes: list[dict[str, Any]] = []
        if isinstance(metadata, Mapping) and "_engine" in metadata:
            try:
                nodes = list(engine_state(dict(metadata))["nodes"].values())
            except Exception:
                nodes = []
        node_step_ids = {str(node.get("step_id")) for node in nodes if node.get("step_id")}
        risk_units = sum(
            self._risk_weight(self._action_view_from_snapshot(node.get("action_definition")))
            for node in nodes
        )
        for step in steps:
            if str(step.get("id")) not in node_step_ids:
                risk_units += self._risk_weight(self._action_view_from_snapshot(step.get("action_definition")))
        return {"steps": len(steps), "planned_nodes": len(nodes), "risk_units": risk_units}

    def _context(
        self,
        *,
        saga: Mapping[str, Any],
        phase: str,
        action: Mapping[str, Any] | None,
        request: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        subject = self._subject(saga)
        usage = self._usage(saga)
        return {
            "tenant_id": subject.tenant_id,
            "principal": {
                "id": subject.principal_id,
                "type": subject.principal_type,
                "roles": list(subject.roles),
                "scopes": list(subject.scopes),
                "authenticated": subject.authenticated,
            },
            "phase": phase,
            "action": dict(action) if action is not None else None,
            "saga": {
                "id": str(saga["id"]),
                "status": str(saga.get("status")),
                "steps": usage["steps"],
                "planned_nodes": usage["planned_nodes"],
                "risk_units": usage["risk_units"],
            },
            "request": dict(request or {}),
        }

    def _record_policy_decision(
        self,
        saga_id: str,
        decision: PolicyDecision,
        *,
        phase: str,
        action: Mapping[str, Any] | None,
        request: Mapping[str, Any] | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        data: dict[str, Any] = {
            "decision_id": decision.decision_id,
            "effect": decision.effect,
            "reason": decision.reason,
            "revision": decision.revision,
            "backend": decision.backend,
            "matched_rules": list(decision.matched_rules),
            "phase": phase,
            "risk": risk_name(action),
        }
        for key in (
            "prospective_steps", "prospective_planned_nodes", "prospective_risk_units",
            "requested_max_parallel", "approval_granted",
        ):
            value = (request or {}).get(key)
            if isinstance(value, (bool, int)):
                data[key] = value
        self._audit(
            saga_id,
            "POLICY_DECISION",
            action=str(action.get("id")) if action else None,
            action_version=str(action.get("version")) if action else None,
            status=decision.effect.upper(),
            data=data,
        )
        counter = getattr(self.telemetry, "policy_counter", None)
        if counter is not None:
            counter.add(1, {"semantic_saga.policy.effect": decision.effect, "semantic_saga.policy.backend": decision.backend})
        histogram = getattr(self.telemetry, "policy_duration", None)
        if histogram is not None and duration_seconds is not None:
            histogram.record(duration_seconds, {"semantic_saga.policy.backend": decision.backend})

    def _decide(
        self,
        *,
        saga: Mapping[str, Any],
        phase: str,
        action: Mapping[str, Any] | None,
        request: Mapping[str, Any] | None = None,
    ) -> PolicyDecision:
        started = monotonic_seconds()
        try:
            decision = self.policy_engine.decide(self._context(saga=saga, phase=phase, action=action, request=request))
        except PolicyError as exc:
            decision = PolicyDecision(
                decision_id=str(uuid.uuid4()),
                effect="deny",
                reason=f"Policy evaluation failed closed: {exc}",
                revision="unknown",
                backend="error",
            )
        self._record_policy_decision(
            str(saga["id"]), decision, phase=phase, action=action, request=request,
            duration_seconds=monotonic_seconds() - started,
        )
        return decision

    @staticmethod
    def _enforce(decision: PolicyDecision, *, approval_supported: bool = False) -> None:
        if decision.effect == "deny":
            raise SagaError(f"Policy denied operation ({decision.decision_id}): {decision.reason}")
        if decision.effect == "require_approval" and not approval_supported:
            raise SagaError(
                f"Policy requires approval ({decision.decision_id}): {decision.reason}. "
                "Use plan_saga_step and approve_saga_step before execution."
            )

    def _active_registered(self, action_name: str, values: dict[str, Any]) -> RegisteredAction:
        self._sync_legacy_action(action_name)
        try:
            return self.registry.prepare(action_name, values)
        except ActionRegistryError as exc:
            raise SagaError(str(exc)) from exc

    def execute(self, saga_id: str, action_name: str, values: dict[str, Any], *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        registered = self._active_registered(action_name, values)
        action = self._action_view(registered.definition)
        usage = self._usage(saga)
        decision = self._decide(
            saga=saga,
            phase="execute",
            action=action,
            request={
                "prospective_steps": usage["steps"] + 1,
                "prospective_planned_nodes": usage["planned_nodes"],
                "prospective_risk_units": usage["risk_units"] + self._risk_weight(action),
                "approval_granted": False,
            },
        )
        self._enforce(decision)
        return super().execute(saga_id, action_name, values, session_id=session_id)

    def plan_step(self, saga_id: str, action_name: str, values: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        session_id = kwargs.get("session_id", "default")
        saga = self._require(saga_id, session_id)
        registered = self._active_registered(action_name, values)
        action = self._action_view(registered.definition)
        usage = self._usage(saga)
        already_required = bool(registered.definition.policy.get("approval_required") or kwargs.get("approval_required") is True)
        decision = self._decide(
            saga=saga,
            phase="plan",
            action=action,
            request={
                "prospective_steps": usage["steps"],
                "prospective_planned_nodes": usage["planned_nodes"] + 1,
                "prospective_risk_units": usage["risk_units"] + self._risk_weight(action),
                "approval_granted": already_required,
            },
        )
        if decision.effect == "deny":
            self._enforce(decision)
        if decision.effect == "require_approval":
            kwargs["approval_required"] = True
        return super().plan_step(saga_id, action_name, values, **kwargs)

    def _node(self, saga: Mapping[str, Any], node_id: str) -> dict[str, Any]:
        metadata = saga.get("metadata") if isinstance(saga.get("metadata"), Mapping) else {}
        if not isinstance(metadata, Mapping) or "_engine" not in metadata:
            raise SagaError(f"Workflow node not found: {node_id}")
        node = engine_state(dict(metadata))["nodes"].get(node_id)
        if node is None:
            raise SagaError(f"Workflow node not found: {node_id}")
        return node

    def run_ready_steps(self, saga_id: str, **kwargs: Any) -> dict[str, Any]:
        session_id = kwargs.get("session_id", "default")
        saga = self._require(saga_id, session_id)
        metadata = saga.get("metadata") if isinstance(saga.get("metadata"), Mapping) else {}
        state = engine_state(dict(metadata)) if isinstance(metadata, Mapping) and "_engine" in metadata else {"nodes": {}}
        ready = [node for node in state["nodes"].values() if node.get("status") == "READY"]
        max_parallel = int(kwargs.get("max_parallel", 4))
        max_steps = int(kwargs.get("max_steps", 100))
        usage = self._usage(saga)
        wave_size = min(len(ready), max_parallel, max_steps)
        common_request = {
            "prospective_steps": usage["steps"] + wave_size,
            "prospective_planned_nodes": usage["planned_nodes"],
            "prospective_risk_units": usage["risk_units"],
            "requested_max_parallel": max_parallel,
            "approval_granted": True,
        }
        control = self._decide(saga=saga, phase="run", action=None, request=common_request)
        self._enforce(control, approval_supported=True)
        for node in ready[:wave_size]:
            action = self._action_view_from_snapshot(node.get("action_definition"))
            approved = not node.get("approval_required") or (node.get("approval") or {}).get("status") == "APPROVED"
            node_request = dict(common_request)
            node_request["approval_granted"] = approved
            decision = self._decide(saga=saga, phase="run", action=action, request=node_request)
            if decision.effect == "require_approval" and not approved:
                raise SagaError(
                    f"Policy now requires approval for workflow node {node['id']} ({decision.decision_id}): "
                    f"{decision.reason}. Re-plan the node with an approval gate before running it."
                )
            self._enforce(decision, approval_supported=approved)
        return super().run_ready_steps(saga_id, **kwargs)

    def approve_step(self, saga_id: str, node_id: str, **kwargs: Any) -> dict[str, Any]:
        session_id = kwargs.get("session_id", "default")
        saga = self._require(saga_id, session_id)
        if not bool(kwargs.get("approved", True)):
            self._audit(
                saga_id,
                "POLICY_SAFETY_OVERRIDE",
                node_id=node_id,
                status="SAFE_REJECTION",
                data={"phase": "approve", "reason": "Approval rejection remains available as a fail-safe operation"},
            )
            return super().approve_step(saga_id, node_id, **kwargs)
        node = self._node(saga, node_id)
        action = self._action_view_from_snapshot(node.get("action_definition"))
        decision = self._decide(saga=saga, phase="approve", action=action, request={"approval_granted": True})
        self._enforce(decision, approval_supported=True)
        return super().approve_step(saga_id, node_id, **kwargs)

    def retry_step(self, saga_id: str, node_id: str, **kwargs: Any) -> dict[str, Any]:
        session_id = kwargs.get("session_id", "default")
        saga = self._require(saga_id, session_id)
        node = self._node(saga, node_id)
        action = self._action_view_from_snapshot(node.get("action_definition"))
        approved = not node.get("approval_required") or (node.get("approval") or {}).get("status") == "APPROVED"
        decision = self._decide(saga=saga, phase="retry", action=action, request={"approval_granted": approved})
        if decision.effect == "require_approval" and not approved:
            raise SagaError(f"Policy requires approval before retry ({decision.decision_id}): {decision.reason}")
        self._enforce(decision, approval_supported=approved)
        return super().retry_step(saga_id, node_id, **kwargs)

    def commit(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        usage = self._usage(saga)
        decision = self._decide(
            saga=saga,
            phase="commit",
            action=None,
            request={
                "prospective_steps": usage["steps"],
                "prospective_planned_nodes": usage["planned_nodes"],
                "prospective_risk_units": usage["risk_units"],
                "approval_granted": True,
            },
        )
        self._enforce(decision, approval_supported=True)
        return super().commit(saga_id, session_id=session_id)

    def rollback(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        self._require(saga_id, session_id)
        self._audit(
            saga_id,
            "POLICY_SAFETY_OVERRIDE",
            status="ROLLBACK_SAFETY_PATH",
            data={"phase": "rollback", "reason": "Rollback remains available for compensation safety"},
        )
        return super().rollback(saga_id, session_id=session_id)

    def get_policy_status(self, tenant_id: str | None = None) -> dict[str, Any]:
        subject = current_policy_subject()
        selected = tenant_id or (subject.tenant_id if subject is not None else "default")
        if subject is not None and tenant_id is not None and tenant_id != subject.tenant_id and "admin" not in {r.lower() for r in subject.roles}:
            raise SagaError("Policy status is limited to the caller's tenant")
        return self.policy_engine.status(selected)

    def get_policy_decisions(self, saga_id: str, *, session_id: str = "default", limit: int = 500) -> dict[str, Any]:
        self._require(saga_id, session_id)
        return {
            "saga_id": saga_id,
            "events": self.audit.list(
                saga_id,
                limit=limit,
                event_types={"POLICY_DECISION", "POLICY_SAFETY_OVERRIDE"},
            ),
        }
