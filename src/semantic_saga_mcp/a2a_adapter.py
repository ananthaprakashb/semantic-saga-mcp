from __future__ import annotations

import hashlib
import logging
from typing import Any, Literal, Mapping

import anyio
from pydantic import BaseModel, ConfigDict, Field, StrictStr, model_validator

from .auth import AuthorizationError, AuthorizationPolicy
from .coordinator import SagaError
from .observability import actor_scope, attach_mcp_trace
from .policy import PolicySubject, policy_subject_scope


logger = logging.getLogger(__name__)

_OPERATION_TO_TOOL = {
    "begin": "begin_saga",
    "execute": "execute_saga_step",
    "plan": "plan_saga_step",
    "run": "run_ready_steps",
    "approve": "approve_saga_step",
    "retry": "retry_saga_step",
    "checkpoint": "checkpoint_saga",
    "commit": "commit_saga",
    "rollback": "rollback_saga",
    "get": "get_saga",
    "timeline": "get_saga_timeline",
    "audit": "get_audit_events",
    "verify_audit": "verify_audit_chain",
    "list_actions": "list_actions",
    "get_action": "get_action",
    "policy_status": "get_policy_status",
    "policy_decisions": "get_policy_decisions",
}


class A2ACommand(BaseModel):
    """Strict structured command accepted from peer agents.

    The A2A ingress intentionally does not execute free-form natural-language
    instructions. Peer agents send one application/json Part.data command so
    orchestration remains deterministic and policy/audit inputs are explicit.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    operation: Literal[
        "begin", "execute", "plan", "run", "approve", "retry", "checkpoint",
        "commit", "rollback", "get", "timeline", "audit", "verify_audit",
        "list_actions", "get_action", "policy_status", "policy_decisions",
    ]
    saga_id: StrictStr | None = None
    action: StrictStr | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    key: StrictStr | None = None
    depends_on: list[StrictStr] = Field(default_factory=list)
    approval_required: bool | None = None
    node_id: StrictStr | None = None
    approved: bool = True
    reason: StrictStr | None = None
    force: bool = False
    name: StrictStr | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    max_parallel: int = Field(default=4, ge=1, le=32)
    max_steps: int = Field(default=100, ge=1, le=1000)
    limit: int = Field(default=500, ge=1, le=5000)
    event_types: list[StrictStr] = Field(default_factory=list)
    version: StrictStr | None = None
    tenant_id: StrictStr | None = None

    @model_validator(mode="after")
    def require_operation_fields(self) -> "A2ACommand":
        saga_ops = {
            "execute", "plan", "run", "approve", "retry", "checkpoint", "commit",
            "rollback", "get", "timeline", "audit", "verify_audit", "policy_decisions",
        }
        action_ops = {"execute", "plan"}
        node_ops = {"approve", "retry"}
        if self.operation in saga_ops and not self.saga_id:
            raise ValueError(f"{self.operation} requires saga_id")
        if self.operation in action_ops and not self.action:
            raise ValueError(f"{self.operation} requires action")
        if self.operation in node_ops and not self.node_id:
            raise ValueError(f"{self.operation} requires node_id")
        if self.operation == "checkpoint" and not self.name:
            raise ValueError("checkpoint requires name")
        if self.operation == "get_action" and not self.action:
            raise ValueError("get_action requires action")
        return self


def tenant_owner_id(tenant_id: str) -> str:
    return "tenant:" + hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()


def _identity(context: Any) -> dict[str, Any]:
    state = getattr(getattr(context, "call_context", None), "state", {}) or {}
    value = state.get("semantic_saga_identity")
    if not isinstance(value, Mapping):
        raise SagaError("Authenticated A2A identity is required")
    return dict(value)


def _data_command(message: Any) -> dict[str, Any]:
    from google.protobuf.json_format import MessageToDict

    if message is None:
        raise SagaError("A2A request requires a message")
    values: list[Any] = []
    for part in message.parts:
        if part.WhichOneof("content") == "data":
            values.append(MessageToDict(part.data))
    if len(values) != 1 or not isinstance(values[0], dict):
        raise SagaError("A2A commands require exactly one structured application/json Part.data object")
    return values[0]


def _result_part(value: dict[str, Any]) -> Any:
    from a2a.types import Part
    from google.protobuf.json_format import ParseDict
    from google.protobuf.struct_pb2 import Value

    return Part(data=ParseDict(value, Value()), media_type="application/json")


class SemanticSagaA2AExecutor:
    """A2A AgentExecutor adapter for the governed Semantic Saga coordinator."""

    def __init__(self, coordinator: Any, authorization: AuthorizationPolicy | None = None) -> None:
        self.coordinator = coordinator
        self.authorization = authorization or AuthorizationPolicy()

    def _dispatch(self, command: A2ACommand, identity: Mapping[str, Any], context: Any) -> dict[str, Any]:
        tenant_id = str(identity["tenant_id"])
        principal_id = str(identity["principal_id"])
        principal_type = str(identity.get("principal_type") or "service")
        roles = tuple(str(item) for item in identity.get("roles", ()))
        scopes = tuple(str(item) for item in identity.get("scopes", ()))
        owner_id = tenant_owner_id(tenant_id)
        self.authorization.authorize(_OPERATION_TO_TOOL[command.operation], roles=roles, scopes=scopes)
        subject = PolicySubject(
            tenant_id=tenant_id,
            principal_id=principal_id,
            principal_type=principal_type,
            roles=roles,
            scopes=scopes,
            authenticated=bool(identity.get("authenticated", True)),
        )
        with actor_scope(principal_id, principal_type), policy_subject_scope(subject):
            if command.operation == "begin":
                metadata = dict(command.metadata)
                metadata["_identity"] = {
                    "tenant_id": tenant_id,
                    "principal_id": principal_id,
                    "principal_type": principal_type,
                    "roles": list(roles),
                    "identity_source": "a2a-bearer" if identity.get("authenticated", True) else "a2a-development",
                }
                metadata["_interop"] = {
                    "protocol": "a2a",
                    "protocol_version": "1.0",
                    "context_id": getattr(context, "context_id", None),
                    "task_id": getattr(context, "task_id", None),
                }
                return self.coordinator.begin(
                    metadata,
                    session_id=owner_id,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                )
            if command.operation == "execute":
                return self.coordinator.execute(command.saga_id, command.action, command.input, session_id=owner_id)
            if command.operation == "plan":
                return self.coordinator.plan_step(
                    command.saga_id,
                    command.action,
                    command.input,
                    session_id=owner_id,
                    key=command.key,
                    depends_on=list(command.depends_on),
                    approval_required=command.approval_required,
                )
            if command.operation == "run":
                return self.coordinator.run_ready_steps(
                    command.saga_id,
                    session_id=owner_id,
                    max_parallel=command.max_parallel,
                    max_steps=command.max_steps,
                )
            if command.operation == "approve":
                return self.coordinator.approve_step(
                    command.saga_id,
                    command.node_id,
                    session_id=owner_id,
                    approved=command.approved,
                    reason=command.reason,
                    principal_id=principal_id,
                )
            if command.operation == "retry":
                return self.coordinator.retry_step(
                    command.saga_id, command.node_id, session_id=owner_id, force=command.force
                )
            if command.operation == "checkpoint":
                return self.coordinator.checkpoint(
                    command.saga_id,
                    command.name,
                    command.data,
                    session_id=owner_id,
                    principal_id=principal_id,
                )
            if command.operation == "commit":
                return self.coordinator.commit(command.saga_id, session_id=owner_id)
            if command.operation == "rollback":
                return self.coordinator.rollback(command.saga_id, session_id=owner_id)
            if command.operation == "get":
                return self.coordinator.get(command.saga_id, session_id=owner_id)
            if command.operation == "timeline":
                return self.coordinator.get_timeline(command.saga_id, session_id=owner_id, limit=command.limit)
            if command.operation == "audit":
                return self.coordinator.get_audit_events(
                    command.saga_id,
                    session_id=owner_id,
                    limit=command.limit,
                    event_types=set(command.event_types) if command.event_types else None,
                )
            if command.operation == "verify_audit":
                return self.coordinator.verify_audit_chain(command.saga_id, session_id=owner_id)
            if command.operation == "list_actions":
                return self.coordinator.list_actions()
            if command.operation == "get_action":
                return self.coordinator.get_action(command.action, command.version)
            if command.operation == "policy_status":
                return self.coordinator.get_policy_status(command.tenant_id)
            if command.operation == "policy_decisions":
                return self.coordinator.get_policy_decisions(
                    command.saga_id, session_id=owner_id, limit=command.limit
                )
        raise SagaError(f"Unsupported A2A operation: {command.operation}")

    async def execute(self, context: Any, event_queue: Any) -> None:
        from a2a.helpers import new_task_from_user_message, new_text_message
        from a2a.server.tasks import TaskUpdater
        from a2a.types import TaskState

        task = context.current_task
        if task is None:
            if context.message is None:
                raise SagaError("A2A request requires a message")
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue=event_queue, task_id=task.id, context_id=task.context_id)
        await updater.update_status(
            state=TaskState.TASK_STATE_WORKING,
            message=new_text_message("Semantic Saga is evaluating the governed command."),
        )
        try:
            command = A2ACommand.model_validate(_data_command(context.message))
            identity = _identity(context)
            headers = context.call_context.state.get("headers", {}) if context.call_context else {}
            with attach_mcp_trace(None, headers):
                result = await anyio.to_thread.run_sync(self._dispatch, command, identity, context)
            await updater.add_artifact(
                parts=[_result_part(result)],
                name="semantic-saga-result",
                metadata={"semantic_saga_operation": command.operation},
            )
            await updater.update_status(
                state=TaskState.TASK_STATE_COMPLETED,
                message=new_text_message(f"Semantic Saga completed the {command.operation} command."),
            )
        except (ValueError, SagaError, AuthorizationError) as exc:
            await updater.update_status(
                state=TaskState.TASK_STATE_REJECTED,
                message=new_text_message(str(exc)[:1000]),
            )
        except Exception as exc:
            logger.exception("A2A command execution failed")
            await updater.update_status(
                state=TaskState.TASK_STATE_FAILED,
                message=new_text_message(f"Semantic Saga command failed: {type(exc).__name__}"),
            )

    async def cancel(self, context: Any, event_queue: Any) -> None:
        # Interrupting an unknown external side effect is unsafe. A2A callers use
        # the explicit rollback command once reconciliation/compensation is desired.
        raise NotImplementedError("A2A task cancellation is not supported; use explicit saga rollback")
