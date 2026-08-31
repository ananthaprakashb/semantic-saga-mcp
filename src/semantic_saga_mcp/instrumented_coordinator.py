from __future__ import annotations

import threading
from typing import Any

from .audit import AuditJournalProtocol, StoreAuditJournal
from .coordinator import Coordinator, SagaError, now
from .observability import (
    Telemetry,
    actor_scope,
    attach_otel_context,
    capture_otel_context,
    current_actor,
    current_trace_ids,
    monotonic_seconds,
)
from .registry import RegisteredAction


class InstrumentedCoordinator(Coordinator):
    """Coordinator facade that adds durable audit evidence and OTel signals.

    Business semantics remain in :class:`Coordinator`; this class records the
    control-plane transitions without persisting action input/result payloads.
    """

    def __init__(
        self,
        store: Any,
        actions: Any,
        compensation_retries: int = 3,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
        audit: AuditJournalProtocol | None = None,
        telemetry: Telemetry | None = None,
    ) -> None:
        super().__init__(
            store,
            actions,
            compensation_retries,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )
        self.audit = audit or StoreAuditJournal(store)
        self.telemetry = telemetry or Telemetry()
        self._parallel_context: dict[str, tuple[Any, tuple[str | None, str | None]]] = {}
        self._parallel_context_lock = threading.RLock()

    def _audit(
        self,
        saga_id: str,
        event_type: str,
        *,
        action: str | None = None,
        action_version: str | None = None,
        step_id: str | None = None,
        node_id: str | None = None,
        status: str | None = None,
        data: dict[str, Any] | None = None,
        actor_principal_id: str | None = None,
        actor_type: str | None = None,
    ) -> dict[str, Any]:
        current_principal, current_type = current_actor()
        trace_id, span_id = current_trace_ids()
        event = self.audit.append(
            {
                "saga_id": saga_id,
                "event_type": event_type,
                "actor_principal_id": actor_principal_id if actor_principal_id is not None else current_principal,
                "actor_type": actor_type if actor_type is not None else current_type,
                "action": action,
                "action_version": action_version,
                "step_id": step_id,
                "node_id": node_id,
                "status": status,
                "data": data or {},
                "trace_id": trace_id,
                "span_id": span_id,
                "created_at": now(),
            }
        )
        self.telemetry.event(
            f"semantic_saga.audit.{event_type.lower()}",
            {
                "semantic_saga.event_type": event_type,
                "semantic_saga.action": action,
                "semantic_saga.status": status,
            },
        )
        return event

    def begin(
        self,
        metadata: dict[str, Any] | None = None,
        *,
        session_id: str = "default",
        tenant_id: str | None = None,
        principal_id: str | None = None,
    ) -> dict[str, Any]:
        with self.telemetry.span("semantic_saga.begin"):
            saga = super().begin(
                metadata,
                session_id=session_id,
                tenant_id=tenant_id,
                principal_id=principal_id,
            )
            self._audit(
                saga["id"],
                "SAGA_STARTED",
                status=saga["status"],
                actor_principal_id=principal_id,
            )
            self.telemetry.saga_counter.add(1, {"semantic_saga.operation": "begin", "semantic_saga.outcome": "success"})
            return saga

    def _invoke_forward(
        self,
        registered: RegisteredAction,
        values: dict[str, Any],
        saga_id: str,
        step_id: str,
        lease: Any,
    ) -> tuple[bool, Any, str | None, int]:
        with self._parallel_context_lock:
            parallel = self._parallel_context.get(saga_id)
        parent_context = parallel[0] if parallel else None
        actor = parallel[1] if parallel else current_actor()
        with attach_otel_context(parent_context), actor_scope(*actor):
            started = monotonic_seconds()
            with self.telemetry.span(
                "semantic_saga.action.forward",
                {
                    "semantic_saga.action": registered.definition.action_id,
                    "semantic_saga.action_version": registered.definition.version,
                    "semantic_saga.operation": "forward",
                },
            ):
                result = super()._invoke_forward(registered, values, saga_id, step_id, lease)
                success, _value, _error, attempts = result
                elapsed = monotonic_seconds() - started
                outcome = "success" if success else "failure"
                self.telemetry.record_action(
                    action=registered.definition.action_id,
                    attempts=attempts,
                    outcome=outcome,
                    duration_seconds=elapsed,
                )
                self._audit(
                    saga_id,
                    "ACTION_ATTEMPT_SUMMARY",
                    action=registered.definition.action_id,
                    action_version=registered.definition.version,
                    step_id=step_id,
                    status="COMPLETED" if success else "FAILED",
                    data={"attempts": attempts, "retry_count": max(0, attempts - 1)},
                )
                return result

    def execute(self, saga_id: str, action_name: str, values: dict[str, Any], *, session_id: str = "default") -> dict[str, Any]:
        self._require(saga_id, session_id)
        with self.telemetry.span(
            "semantic_saga.execute",
            {"semantic_saga.action": action_name, "semantic_saga.operation": "execute"},
        ):
            self._audit(saga_id, "ACTION_REQUESTED", action=action_name, status="REQUESTED")
            try:
                step = super().execute(saga_id, action_name, values, session_id=session_id)
            except Exception as exc:
                self._audit(
                    saga_id,
                    "ACTION_FAILED",
                    action=action_name,
                    status="FAILED",
                    data={"error_type": type(exc).__name__},
                )
                raise
            self._audit(
                saga_id,
                "ACTION_COMPLETED",
                action=step.get("action"),
                action_version=step.get("action_version"),
                step_id=step.get("id"),
                status=step.get("status"),
            )
            return step

    def plan_step(self, saga_id: str, action_name: str, values: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        session_id = kwargs.get("session_id", "default")
        self._require(saga_id, session_id)
        with self.telemetry.span(
            "semantic_saga.workflow.plan",
            {"semantic_saga.action": action_name, "semantic_saga.operation": "plan"},
        ):
            node = super().plan_step(saga_id, action_name, values, **kwargs)
            self._audit(
                saga_id,
                "WORKFLOW_NODE_PLANNED",
                action=node.get("action"),
                action_version=node.get("action_version"),
                node_id=node.get("id"),
                status=node.get("status"),
                data={
                    "dependency_count": len(node.get("depends_on") or []),
                    "approval_required": bool(node.get("approval_required")),
                    "key": node.get("key"),
                },
            )
            return node

    def run_ready_steps(self, saga_id: str, **kwargs: Any) -> dict[str, Any]:
        session_id = kwargs.get("session_id", "default")
        before = self.get(saga_id, session_id=session_id)
        before_status = {
            node["id"]: node.get("status") for node in before.get("workflow", {}).get("nodes", [])
        }
        with self.telemetry.span("semantic_saga.workflow.run", {"semantic_saga.operation": "run_ready"}):
            self._audit(
                saga_id,
                "WORKFLOW_RUN_REQUESTED",
                status=before.get("status"),
                data={
                    "max_parallel": int(kwargs.get("max_parallel", 4)),
                    "max_steps": int(kwargs.get("max_steps", 100)),
                },
            )
            with self._parallel_context_lock:
                self._parallel_context[saga_id] = (capture_otel_context(), current_actor())
            try:
                result = super().run_ready_steps(saga_id, **kwargs)
            finally:
                with self._parallel_context_lock:
                    self._parallel_context.pop(saga_id, None)

            for node in result.get("workflow", {}).get("nodes", []):
                old = before_status.get(node["id"])
                new = node.get("status")
                if old == new:
                    continue
                self._audit(
                    saga_id,
                    f"WORKFLOW_NODE_{str(new).upper()}",
                    action=node.get("action"),
                    action_version=node.get("action_version"),
                    step_id=node.get("step_id"),
                    node_id=node.get("id"),
                    status=new,
                    data={
                        "attempts": int(node.get("attempts", 0)),
                        "uncertain_outcome": bool(node.get("uncertain_outcome")),
                    },
                )
            self._audit(saga_id, "WORKFLOW_RUN_COMPLETED", status=result.get("status"))
            return result

    def approve_step(self, saga_id: str, node_id: str, **kwargs: Any) -> dict[str, Any]:
        approved = bool(kwargs.get("approved", True))
        with self.telemetry.span("semantic_saga.approval", {"semantic_saga.approved": approved}):
            node = super().approve_step(saga_id, node_id, **kwargs)
            decision = "approved" if approved else "rejected"
            self.telemetry.approval_counter.add(1, {"semantic_saga.decision": decision})
            self._audit(
                saga_id,
                "APPROVAL_DECIDED",
                action=node.get("action"),
                action_version=node.get("action_version"),
                node_id=node_id,
                status=node.get("status"),
                data={"decision": decision, "reason_present": bool(kwargs.get("reason"))},
                actor_principal_id=kwargs.get("principal_id"),
            )
            return node

    def retry_step(self, saga_id: str, node_id: str, **kwargs: Any) -> dict[str, Any]:
        with self.telemetry.span("semantic_saga.operator.retry", {"semantic_saga.force": bool(kwargs.get("force", False))}):
            node = super().retry_step(saga_id, node_id, **kwargs)
            self._audit(
                saga_id,
                "OPERATOR_RETRY_REQUESTED",
                action=node.get("action"),
                action_version=node.get("action_version"),
                step_id=node.get("step_id"),
                node_id=node_id,
                status=node.get("status"),
                data={"force": bool(kwargs.get("force", False))},
            )
            return node

    def checkpoint(self, saga_id: str, name: str, data: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        with self.telemetry.span("semantic_saga.checkpoint", {"semantic_saga.checkpoint.name": name}):
            checkpoint = super().checkpoint(saga_id, name, data, **kwargs)
            self._audit(
                saga_id,
                "CHECKPOINT_CREATED",
                status="RECORDED",
                data={"checkpoint_id": checkpoint["id"], "name": name},
                actor_principal_id=kwargs.get("principal_id"),
            )
            return checkpoint

    def commit(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        self._require(saga_id, session_id)
        with self.telemetry.span("semantic_saga.commit"):
            saga = super().commit(saga_id, session_id=session_id)
            self._audit(saga_id, "SAGA_COMMITTED", status=saga.get("status"))
            self.telemetry.saga_counter.add(1, {"semantic_saga.operation": "commit", "semantic_saga.outcome": "success"})
            return saga

    def rollback(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        self._require(saga_id, session_id)
        with self.telemetry.span("semantic_saga.rollback"):
            self._audit(saga_id, "ROLLBACK_REQUESTED", status="REQUESTED")
            started = monotonic_seconds()
            result = super().rollback(saga_id, session_id=session_id)
            elapsed = monotonic_seconds() - started
            compensated = 0
            failed = 0
            total_attempts = 0
            for step in result.get("steps", []):
                attempts = int(step.get("compensation_attempts", 0))
                if attempts <= 0:
                    continue
                total_attempts += attempts
                if step.get("status") == "COMPENSATED":
                    compensated += 1
                elif step.get("status") == "COMPENSATION_FAILED":
                    failed += 1
                self._audit(
                    saga_id,
                    "COMPENSATION_RESULT",
                    action=step.get("action"),
                    action_version=step.get("action_version"),
                    step_id=step.get("id"),
                    status=step.get("status"),
                    data={"attempts": attempts},
                )
            if total_attempts:
                self.telemetry.compensation_attempt_counter.add(
                    total_attempts,
                    {"semantic_saga.outcome": "failure" if failed else "success"},
                )
                self.telemetry.compensation_duration.record(
                    elapsed,
                    {"semantic_saga.outcome": "failure" if failed else "success"},
                )
            self._audit(
                saga_id,
                "ROLLBACK_COMPLETED" if result.get("status") == "ROLLED_BACK" else "ROLLBACK_FAILED",
                status=result.get("status"),
                data={"compensated_steps": compensated, "failed_steps": failed},
            )
            self.telemetry.saga_counter.add(
                1,
                {
                    "semantic_saga.operation": "rollback",
                    "semantic_saga.outcome": "success" if result.get("status") == "ROLLED_BACK" else "failure",
                },
            )
            return result

    def resume_pending_rollbacks(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.telemetry.span("semantic_saga.recovery", {"semantic_saga.recovery.limit": limit}):
            results = super().resume_pending_rollbacks(limit=limit)
            for saga in results:
                operator_required = saga.get("status") == "RECOVERY_REQUIRED"
                event_type = "RECOVERY_REQUIRES_OPERATOR" if operator_required else "RECOVERY_COMPLETED"
                self._audit(saga["id"], event_type, status=saga.get("status"))
                self.telemetry.recovery_counter.add(
                    1,
                    {"semantic_saga.outcome": "operator_required" if operator_required else "completed"},
                )
            return results

    def get_audit_events(
        self,
        saga_id: str,
        *,
        session_id: str = "default",
        limit: int = 500,
        event_types: set[str] | None = None,
    ) -> dict[str, Any]:
        self._require(saga_id, session_id)
        return {"saga_id": saga_id, "events": self.audit.list(saga_id, limit=limit, event_types=event_types)}

    def verify_audit_chain(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        self._require(saga_id, session_id)
        return self.audit.verify(saga_id)

    def get_timeline(self, saga_id: str, *, session_id: str = "default", limit: int = 1000) -> dict[str, Any]:
        saga = self.get(saga_id, session_id=session_id)
        steps = [
            {
                "id": step.get("id"),
                "sequence": step.get("sequence"),
                "action": step.get("action"),
                "action_version": step.get("action_version"),
                "status": step.get("status"),
                "compensation_attempts": step.get("compensation_attempts", 0),
                "created_at": step.get("created_at"),
                "updated_at": step.get("updated_at"),
            }
            for step in saga.get("steps", [])
        ]
        workflow_nodes = [
            {
                "id": node.get("id"),
                "key": node.get("key"),
                "action": node.get("action"),
                "action_version": node.get("action_version"),
                "depends_on": node.get("depends_on", []),
                "status": node.get("status"),
                "step_id": node.get("step_id"),
                "attempts": node.get("attempts", 0),
                "approval_status": (node.get("approval") or {}).get("status"),
                "uncertain_outcome": bool(node.get("uncertain_outcome")),
                "planned_at": node.get("planned_at"),
                "updated_at": node.get("updated_at"),
            }
            for node in saga.get("workflow", {}).get("nodes", [])
        ]
        return {
            "saga_id": saga_id,
            "status": saga.get("status"),
            "created_at": saga.get("created_at"),
            "updated_at": saga.get("updated_at"),
            "steps": steps,
            "workflow_nodes": workflow_nodes,
            "audit": self.audit.list(saga_id, limit=limit),
            "audit_integrity": self.audit.verify(saga_id),
        }
