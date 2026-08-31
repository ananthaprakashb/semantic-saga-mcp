from __future__ import annotations

import json
from typing import Any

import anyio
from mcp.server import Server, ServerRequestContext
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    GetPromptRequestParams,
    GetPromptResult,
    ListPromptsResult,
    ListToolsResult,
    Prompt,
    PromptMessage,
    TextContent,
    Tool,
)
from pydantic import ValidationError

from . import __version__
from .auth import AuthorizationError, AuthorizationPolicy, IdentityError
from .coordinator import Coordinator, SagaError
from .execution import ExecutionContextResolver
from .observability import Telemetry, actor_scope, attach_mcp_trace
from .server import (
    ARGUMENT_MODELS,
    ApprovalArguments,
    AuditArguments,
    BeginArguments,
    CheckpointArguments,
    ExecuteArguments,
    GetActionArguments,
    ListActionsArguments,
    PlanStepArguments,
    RetryStepArguments,
    RunReadyArguments,
    SYSTEM_PROMPT,
    TimelineArguments,
    TOOLS,
)


PROMPT = Prompt(
    name="saga-coordinator",
    title="Saga Coordinator",
    description=SYSTEM_PROMPT,
    arguments=[],
)


def _tool_result(value: dict[str, Any]) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(value, separators=(",", ":")))],
        structured_content=value,
    )


def _tool_error(message: str) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=message)],
        is_error=True,
    )


def build_mcp_server(
    coordinator: Coordinator,
    resolver: ExecutionContextResolver,
    policy: AuthorizationPolicy | None = None,
) -> Server:
    policy = policy or AuthorizationPolicy()
    telemetry = getattr(coordinator, "telemetry", Telemetry())
    tools = [
        Tool(
            name=definition["name"],
            description=definition["description"],
            input_schema=definition["inputSchema"],
        )
        for definition in TOOLS
    ]

    async def list_tools(_: ServerRequestContext, __: Any) -> ListToolsResult:
        return ListToolsResult(tools=tools)

    async def call_tool(ctx: ServerRequestContext, params: CallToolRequestParams) -> CallToolResult:
        model = ARGUMENT_MODELS.get(params.name)
        if model is None:
            return _tool_error(f"Unknown action tool: {params.name}")
        request = getattr(ctx, "request", None)
        headers = getattr(request, "headers", None)
        meta = getattr(ctx, "meta", None)
        with attach_mcp_trace(meta if isinstance(meta, dict) else None, headers):
            with telemetry.span(
                "mcp.tools.call",
                {
                    "rpc.system": "mcp",
                    "rpc.method": "tools/call",
                    "mcp.tool.name": params.name,
                },
            ):
                try:
                    args = model.model_validate(params.arguments or {})
                    execution = resolver.resolve(ctx)
                    policy.authorize(params.name, roles=execution.roles, scopes=execution.scopes)

                    def invoke() -> dict[str, Any]:
                        with actor_scope(execution.principal_id, execution.principal_type):
                            if isinstance(args, BeginArguments):
                                metadata = dict(args.metadata)
                                metadata["_identity"] = execution.audit_metadata()
                                return coordinator.begin(
                                    metadata,
                                    session_id=execution.owner_id,
                                    tenant_id=execution.tenant_id,
                                    principal_id=execution.principal_id,
                                )
                            if isinstance(args, PlanStepArguments):
                                return coordinator.plan_step(
                                    args.saga_id,
                                    args.action,
                                    args.input,
                                    session_id=execution.owner_id,
                                    key=args.key,
                                    depends_on=list(args.depends_on),
                                    approval_required=args.approval_required,
                                )
                            if isinstance(args, ExecuteArguments):
                                return coordinator.execute(
                                    args.saga_id,
                                    args.action,
                                    args.input,
                                    session_id=execution.owner_id,
                                )
                            if isinstance(args, RunReadyArguments):
                                return coordinator.run_ready_steps(
                                    args.saga_id,
                                    session_id=execution.owner_id,
                                    max_parallel=args.max_parallel,
                                    max_steps=args.max_steps,
                                )
                            if isinstance(args, ApprovalArguments):
                                return coordinator.approve_step(
                                    args.saga_id,
                                    args.node_id,
                                    session_id=execution.owner_id,
                                    approved=args.approved,
                                    reason=args.reason,
                                    principal_id=execution.principal_id,
                                )
                            if isinstance(args, RetryStepArguments):
                                return coordinator.retry_step(
                                    args.saga_id,
                                    args.node_id,
                                    session_id=execution.owner_id,
                                    force=args.force,
                                )
                            if isinstance(args, CheckpointArguments):
                                return coordinator.checkpoint(
                                    args.saga_id,
                                    args.name,
                                    args.data,
                                    session_id=execution.owner_id,
                                    principal_id=execution.principal_id,
                                )
                            if isinstance(args, AuditArguments):
                                if params.name == "verify_audit_chain":
                                    return coordinator.verify_audit_chain(args.saga_id, session_id=execution.owner_id)
                                return coordinator.get_audit_events(
                                    args.saga_id,
                                    session_id=execution.owner_id,
                                    limit=args.limit,
                                    event_types=set(args.event_types) if args.event_types else None,
                                )
                            if isinstance(args, TimelineArguments):
                                return coordinator.get_timeline(
                                    args.saga_id,
                                    session_id=execution.owner_id,
                                    limit=args.limit,
                                )
                            if isinstance(args, ListActionsArguments):
                                return coordinator.list_actions()
                            if isinstance(args, GetActionArguments):
                                return coordinator.get_action(args.action, args.version)
                            if params.name == "commit_saga":
                                return coordinator.commit(args.saga_id, session_id=execution.owner_id)
                            if params.name in {"rollback_saga", "trigger_rollback"}:
                                return coordinator.rollback(args.saga_id, session_id=execution.owner_id)
                            return coordinator.get(args.saga_id, session_id=execution.owner_id)

                    value = await anyio.to_thread.run_sync(invoke)
                    return _tool_result(value)
                except ValidationError as exc:
                    return _tool_error(f"Invalid arguments: {exc}")
                except (IdentityError, AuthorizationError, SagaError, KeyError, TypeError, ValueError) as exc:
                    return _tool_error(str(exc))
                except Exception as exc:
                    return _tool_error(f"Internal error: {exc}")

    async def list_prompts(_: ServerRequestContext, __: Any) -> ListPromptsResult:
        return ListPromptsResult(prompts=[PROMPT])

    async def get_prompt(_: ServerRequestContext, params: GetPromptRequestParams) -> GetPromptResult:
        if params.name != PROMPT.name:
            raise ValueError(f"Unknown prompt: {params.name}")
        return GetPromptResult(
            description=SYSTEM_PROMPT,
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=SYSTEM_PROMPT),
                )
            ],
        )

    return Server(
        "semantic-saga-mcp",
        version=__version__,
        instructions=SYSTEM_PROMPT,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
        on_list_prompts=list_prompts,
        on_get_prompt=get_prompt,
    )


async def run_stdio(server: Server) -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )
