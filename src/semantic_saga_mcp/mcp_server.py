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
from .server import (
    ARGUMENT_MODELS,
    BeginArguments,
    ExecuteArguments,
    GetActionArguments,
    ListActionsArguments,
    SYSTEM_PROMPT,
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
        try:
            args = model.model_validate(params.arguments or {})
            execution = resolver.resolve(ctx)
            policy.authorize(params.name, roles=execution.roles, scopes=execution.scopes)

            def invoke() -> dict[str, Any]:
                if isinstance(args, BeginArguments):
                    metadata = dict(args.metadata)
                    metadata["_identity"] = execution.audit_metadata()
                    return coordinator.begin(
                        metadata,
                        session_id=execution.owner_id,
                        tenant_id=execution.tenant_id,
                        principal_id=execution.principal_id,
                    )
                if isinstance(args, ExecuteArguments):
                    return coordinator.execute(
                        args.saga_id,
                        args.action,
                        args.input,
                        session_id=execution.owner_id,
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
