from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictStr, ValidationError

from . import __version__
from .actions import Action, FileTransactionTool, load_actions
from .coordinator import Coordinator, SagaError
from .store import SagaStore, SQLiteSagaStore


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class BeginArguments(StrictModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class SagaArguments(StrictModel):
    saga_id: StrictStr = Field(min_length=1)


class ExecuteArguments(SagaArguments):
    action: StrictStr = Field(min_length=1)
    input: dict[str, Any]


class ToolCallParams(StrictModel):
    name: Literal["begin_saga", "execute_saga_step", "commit_saga", "rollback_saga", "trigger_rollback", "get_saga"]
    arguments: dict[str, Any] = Field(default_factory=dict)


class PromptGetParams(StrictModel):
    name: Literal["saga-coordinator"]
    arguments: dict[str, Any] = Field(default_factory=dict)


class JsonRpcRequest(StrictModel):
    jsonrpc: Literal["2.0"]
    id: int | StrictStr | None = None
    method: StrictStr
    params: dict[str, Any] = Field(default_factory=dict)


ARGUMENT_MODELS = {
    "begin_saga": BeginArguments,
    "execute_saga_step": ExecuteArguments,
    "commit_saga": SagaArguments,
    "rollback_saga": SagaArguments,
    "trigger_rollback": SagaArguments,
    "get_saga": SagaArguments,
}

TOOLS = [
    {"name": "begin_saga", "description": "Start a durable transactional workflow.", "inputSchema": BeginArguments.model_json_schema()},
    {"name": "execute_saga_step", "description": "Execute a configured action and durably register its compensation.", "inputSchema": ExecuteArguments.model_json_schema()},
    {"name": "commit_saga", "description": "Commit a successfully completed saga; it can no longer be rolled back.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "rollback_saga", "description": "Compensate saga steps in reverse order.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "trigger_rollback", "description": "Immediately compensate saga steps in reverse order after an error.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "get_saga", "description": "Inspect saga and step status, results, and rollback failures.", "inputSchema": SagaArguments.model_json_schema()},
]

SYSTEM_PROMPT = (
    "When executing multi-step infrastructure changes, wrap your actions in the Saga Coordinator. "
    "If you encounter an error, immediately call the `trigger_rollback` tool."
)
PROMPTS = [
    {
        "name": "saga-coordinator",
        "title": "Saga Coordinator",
        "description": SYSTEM_PROMPT,
        "arguments": [],
    }
]


class McpServer:
    """Deprecated 2025-era JSON-RPC/SSE compatibility server.

    New stdio and remote deployments use the official MCP v2 SDK. This class
    remains only so existing dedicated-SSE integrations can migrate without an
    immediate break.
    """

    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator

    def dispatch(self, message: dict[str, Any], session_id: str = "default") -> dict[str, Any] | None:
        request_id = message.get("id") if isinstance(message, dict) else None
        try:
            request = JsonRpcRequest.model_validate(message)
            if request.id is None:
                return None
            if request.method == "initialize":
                result = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": False}, "prompts": {"listChanged": False}}, "serverInfo": {"name": "semantic-saga-mcp", "version": __version__}}
            elif request.method == "ping":
                result = {}
            elif request.method == "tools/list":
                result = {"tools": TOOLS}
            elif request.method == "tools/call":
                result = self._call(request.params, session_id)
            elif request.method == "prompts/list":
                result = {"prompts": PROMPTS}
            elif request.method == "prompts/get":
                params = PromptGetParams.model_validate(request.params)
                result = {
                    "description": PROMPTS[0]["description"],
                    "messages": [{"role": "user", "content": {"type": "text", "text": SYSTEM_PROMPT}}],
                }
            else:
                return self._error(request.id, -32601, f"Method not found: {request.method}")
            return {"jsonrpc": "2.0", "id": request.id, "result": result}
        except ValidationError as exc:
            return self._error(request_id, -32602, f"Invalid request: {exc}")
        except (SagaError, KeyError, TypeError, ValueError) as exc:
            return self._error(request_id, -32602, str(exc))
        except Exception as exc:
            return self._error(request_id, -32603, f"Internal error: {exc}")

    def _call(self, raw_params: dict[str, Any], session_id: str) -> dict[str, Any]:
        params = ToolCallParams.model_validate(raw_params)
        args = ARGUMENT_MODELS[params.name].model_validate(params.arguments)
        if isinstance(args, BeginArguments):
            value = self.coordinator.begin(args.metadata, session_id=session_id)
        elif isinstance(args, ExecuteArguments):
            value = self.coordinator.execute(args.saga_id, args.action, args.input, session_id=session_id)
        elif params.name == "commit_saga":
            value = self.coordinator.commit(args.saga_id, session_id=session_id)
        elif params.name in {"rollback_saga", "trigger_rollback"}:
            value = self.coordinator.rollback(args.saga_id, session_id=session_id)
        else:
            value = self.coordinator.get(args.saga_id, session_id=session_id)
        return {"content": [{"type": "text", "text": json.dumps(value, separators=(",", ":"))}], "structuredContent": value}

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}

    def run_stdio(self) -> None:
        session_id = f"stdio:{uuid.uuid4()}"
        for line in sys.stdin:
            try:
                response = self.dispatch(json.loads(line), session_id)
                if response is not None:
                    print(json.dumps(response, separators=(",", ":")), flush=True)
            except json.JSONDecodeError as exc:
                print(json.dumps(self._error(None, -32700, f"Parse error: {exc}")), flush=True)

    def sse_app(self) -> Any:
        from starlette.applications import Starlette
        from starlette.requests import Request
        from starlette.responses import JSONResponse, StreamingResponse
        from starlette.routing import Route

        queues: dict[str, asyncio.Queue[str]] = {}

        async def sse(request: Request) -> StreamingResponse:
            session_id = str(uuid.uuid4())
            queue: asyncio.Queue[str] = asyncio.Queue()
            queues[session_id] = queue

            async def events():
                try:
                    yield f"event: endpoint\ndata: /messages?session_id={session_id}\n\n"
                    while True:
                        yield f"event: message\ndata: {await queue.get()}\n\n"
                finally:
                    queues.pop(session_id, None)

            return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

        async def messages(request: Request) -> JSONResponse:
            session_id = request.query_params.get("session_id", "")
            queue = queues.get(session_id)
            if queue is None:
                return JSONResponse({"error": "Unknown or disconnected session"}, status_code=404)
            try:
                payload = await request.json()
            except (json.JSONDecodeError, UnicodeDecodeError):
                return JSONResponse({"error": "Invalid JSON"}, status_code=400)
            response = self.dispatch(payload, session_id)
            if response is not None:
                await queue.put(json.dumps(response, separators=(",", ":")))
            return JSONResponse({}, status_code=202)

        return Starlette(routes=[Route("/sse", sse, methods=["GET"]), Route("/messages", messages, methods=["POST"])])


def _env_list(name: str) -> list[str]:
    value = os.getenv(name, "")
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable MCP saga coordinator")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--actions", default=os.getenv("SAGA_ACTIONS_FILE"), help="JSON action definitions")
    parser.add_argument("--file-root", default=os.getenv("SAGA_FILE_ROOT", "./saga-files"), help="Root directory for the built-in create_text_file action")
    parser.add_argument("--database", default=os.getenv("SAGA_DATABASE"), help="SQLite path (default: in-memory store)")
    parser.add_argument("--transport", choices=("stdio", "streamable-http", "sse"), default=os.getenv("SAGA_TRANSPORT", "stdio"))
    parser.add_argument("--host", default=os.getenv("SAGA_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SAGA_PORT", "8000")))
    parser.add_argument("--allowed-host", action="append", default=_env_list("SAGA_ALLOWED_HOSTS"), help="Allowed Host value for remote MCP; repeat or set SAGA_ALLOWED_HOSTS as CSV")
    parser.add_argument("--allowed-origin", action="append", default=_env_list("SAGA_ALLOWED_ORIGINS"), help="Allowed Origin value for browser MCP clients; repeat or set SAGA_ALLOWED_ORIGINS as CSV")
    parser.add_argument("--trust-identity-headers", action="store_true", default=os.getenv("SAGA_TRUST_IDENTITY_HEADERS", "").lower() in {"1", "true", "yes"}, help="Trust X-Semantic-Saga-Tenant/Principal from an authenticated reverse proxy")
    parser.add_argument("--allow-unauthenticated-http", action="store_true", default=os.getenv("SAGA_ALLOW_UNAUTHENTICATED_HTTP", "").lower() in {"1", "true", "yes"}, help="Allow non-local Streamable HTTP without trusted proxy identity; private-network migration only")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("SAGA_DRY_RUN", "").lower() in {"1", "true", "yes"}, help="Preview actions, simulate failure, and log compensation without API calls")
    args = parser.parse_args()

    local_hosts = {"127.0.0.1", "localhost", "::1"}
    remote_streamable_http = args.transport == "streamable-http" and args.host not in local_hosts
    if args.transport == "streamable-http":
        if remote_streamable_http and not args.allowed_host:
            parser.error("--allowed-host (or SAGA_ALLOWED_HOSTS) is required when Streamable HTTP binds beyond localhost")
        if args.allowed_origin and not args.allowed_host:
            parser.error("--allowed-origin requires at least one --allowed-host")
        if remote_streamable_http and not args.trust_identity_headers and not args.allow_unauthenticated_http:
            parser.error(
                "non-local Streamable HTTP requires --trust-identity-headers behind an authenticated reverse proxy; "
                "use --allow-unauthenticated-http only for a controlled private network"
            )

    store = SQLiteSagaStore(args.database) if args.database else SagaStore()
    logger = lambda message: print(message, file=sys.stderr, flush=True)
    actions: dict[str, Action] = load_actions(args.actions, dry_run=args.dry_run, log=logger) if args.actions else {}
    actions["create_text_file"] = FileTransactionTool(Path(args.file_root), logger)
    coordinator = Coordinator(store, actions)
    coordinator.resume_pending_rollbacks()

    if args.transport == "sse":
        print("[deprecated] dedicated SSE transport is retained only for migration; use --transport streamable-http", file=sys.stderr, flush=True)
        import uvicorn

        uvicorn.run(McpServer(coordinator).sse_app(), host=args.host, port=args.port)
        return

    from .execution import ExecutionContextResolver
    from .mcp_server import build_mcp_server, run_stdio

    resolver = ExecutionContextResolver(
        trust_proxy_headers=args.trust_identity_headers,
        require_proxy_identity=remote_streamable_http and not args.allow_unauthenticated_http,
    )
    mcp_server = build_mcp_server(coordinator, resolver)

    if args.transport == "stdio":
        import anyio

        anyio.run(run_stdio, mcp_server)
        return

    from mcp.server.transport_security import TransportSecuritySettings
    import uvicorn

    transport_security = None
    if args.allowed_host or args.allowed_origin:
        transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=args.allowed_host,
            allowed_origins=args.allowed_origin,
        )

    app = mcp_server.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        transport_security=transport_security,
        host=args.host,
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
