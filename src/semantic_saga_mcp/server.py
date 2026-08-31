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
from .actions import FileTransactionTool
from .coordinator import Coordinator, SagaError
from .registry import ActionRegistryError, load_action_registry
from .store import PostgresSagaStore, SagaStore, SQLiteSagaStore


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class BeginArguments(StrictModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class SagaArguments(StrictModel):
    saga_id: StrictStr = Field(min_length=1)


class ExecuteArguments(SagaArguments):
    action: StrictStr = Field(min_length=1)
    input: dict[str, Any]


class PlanStepArguments(ExecuteArguments):
    key: StrictStr | None = None
    depends_on: list[StrictStr] = Field(default_factory=list)
    approval_required: bool | None = None


class RunReadyArguments(SagaArguments):
    max_parallel: int = Field(default=4, ge=1, le=32)
    max_steps: int = Field(default=100, ge=1, le=1000)


class ApprovalArguments(SagaArguments):
    node_id: StrictStr = Field(min_length=1)
    approved: bool = True
    reason: StrictStr | None = None


class RetryStepArguments(SagaArguments):
    node_id: StrictStr = Field(min_length=1)
    force: bool = False


class CheckpointArguments(SagaArguments):
    name: StrictStr = Field(min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)


class ListActionsArguments(StrictModel):
    pass


class GetActionArguments(StrictModel):
    action: StrictStr = Field(min_length=1)
    version: StrictStr | None = None


class ToolCallParams(StrictModel):
    name: Literal[
        "begin_saga",
        "execute_saga_step",
        "plan_saga_step",
        "run_ready_steps",
        "approve_saga_step",
        "retry_saga_step",
        "checkpoint_saga",
        "commit_saga",
        "rollback_saga",
        "trigger_rollback",
        "get_saga",
        "list_actions",
        "get_action",
    ]
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
    "plan_saga_step": PlanStepArguments,
    "run_ready_steps": RunReadyArguments,
    "approve_saga_step": ApprovalArguments,
    "retry_saga_step": RetryStepArguments,
    "checkpoint_saga": CheckpointArguments,
    "commit_saga": SagaArguments,
    "rollback_saga": SagaArguments,
    "trigger_rollback": SagaArguments,
    "get_saga": SagaArguments,
    "list_actions": ListActionsArguments,
    "get_action": GetActionArguments,
}

TOOLS = [
    {"name": "begin_saga", "description": "Start a durable transactional workflow.", "inputSchema": BeginArguments.model_json_schema()},
    {"name": "execute_saga_step", "description": "Immediately execute the active immutable action version. This compatibility path still applies action retry/failure policy.", "inputSchema": ExecuteArguments.model_json_schema()},
    {"name": "plan_saga_step", "description": "Persist a version-pinned workflow node with dependencies and optional approval before any side effect occurs.", "inputSchema": PlanStepArguments.model_json_schema()},
    {"name": "run_ready_steps", "description": "Execute ready DAG nodes in dependency waves, running independent nodes concurrently under one fenced saga lease.", "inputSchema": RunReadyArguments.model_json_schema()},
    {"name": "approve_saga_step", "description": "Approve or reject a workflow node that is waiting for human/operator approval.", "inputSchema": ApprovalArguments.model_json_schema()},
    {"name": "retry_saga_step", "description": "Return a failed/rejected/blocked workflow node to scheduling; uncertain outcomes require explicit force after reconciliation.", "inputSchema": RetryStepArguments.model_json_schema()},
    {"name": "checkpoint_saga", "description": "Persist a named workflow checkpoint and operator/agent-provided checkpoint data.", "inputSchema": CheckpointArguments.model_json_schema()},
    {"name": "commit_saga", "description": "Commit a successfully completed saga; all planned workflow nodes must be COMPLETED.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "rollback_saga", "description": "Compensate saga steps in reverse order using each step's persisted action definition and compensation retry policy.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "trigger_rollback", "description": "Immediately compensate saga steps in reverse order after an error.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "get_saga", "description": "Inspect saga, workflow DAG, checkpoints, action versions, results, approvals, and recovery state.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "list_actions", "description": "List active action contracts, schemas, semantic effects, risk, hashes, and resolved execution policies.", "inputSchema": ListActionsArguments.model_json_schema()},
    {"name": "get_action", "description": "Inspect one registered action contract by id and optional version.", "inputSchema": GetActionArguments.model_json_schema()},
]

SYSTEM_PROMPT = (
    "Wrap multi-step side effects in the Saga Coordinator. Inspect actions with `list_actions` or `get_action`. "
    "For orchestrated work, use `plan_saga_step` with dependencies, obtain approvals when required, then call "
    "`run_ready_steps`. Use checkpoints for durable milestones. If a saga enters RECOVERY_REQUIRED, reconcile the "
    "external outcome before forcing a retry; otherwise roll back."
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
    """Deprecated 2025-era JSON-RPC/SSE compatibility server."""

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
                PromptGetParams.model_validate(request.params)
                result = {
                    "description": PROMPTS[0]["description"],
                    "messages": [{"role": "user", "content": {"type": "text", "text": SYSTEM_PROMPT}}],
                }
            else:
                return self._error(request.id, -32601, f"Method not found: {request.method}")
            return {"jsonrpc": "2.0", "id": request.id, "result": result}
        except ValidationError as exc:
            return self._error(request_id, -32602, f"Invalid request: {exc}")
        except (SagaError, ActionRegistryError, KeyError, TypeError, ValueError) as exc:
            return self._error(request_id, -32602, str(exc))
        except Exception as exc:
            return self._error(request_id, -32603, f"Internal error: {exc}")

    def _call(self, raw_params: dict[str, Any], session_id: str) -> dict[str, Any]:
        params = ToolCallParams.model_validate(raw_params)
        args = ARGUMENT_MODELS[params.name].model_validate(params.arguments)
        if isinstance(args, BeginArguments):
            value = self.coordinator.begin(args.metadata, session_id=session_id)
        elif isinstance(args, PlanStepArguments):
            value = self.coordinator.plan_step(
                args.saga_id, args.action, args.input, session_id=session_id,
                key=args.key, depends_on=list(args.depends_on), approval_required=args.approval_required,
            )
        elif isinstance(args, ExecuteArguments):
            value = self.coordinator.execute(args.saga_id, args.action, args.input, session_id=session_id)
        elif isinstance(args, RunReadyArguments):
            value = self.coordinator.run_ready_steps(
                args.saga_id, session_id=session_id, max_parallel=args.max_parallel, max_steps=args.max_steps
            )
        elif isinstance(args, ApprovalArguments):
            value = self.coordinator.approve_step(
                args.saga_id, args.node_id, session_id=session_id, approved=args.approved, reason=args.reason
            )
        elif isinstance(args, RetryStepArguments):
            value = self.coordinator.retry_step(args.saga_id, args.node_id, session_id=session_id, force=args.force)
        elif isinstance(args, CheckpointArguments):
            value = self.coordinator.checkpoint(args.saga_id, args.name, args.data, session_id=session_id)
        elif isinstance(args, ListActionsArguments):
            value = self.coordinator.list_actions()
        elif isinstance(args, GetActionArguments):
            value = self.coordinator.get_action(args.action, args.version)
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
    parser.add_argument("--actions", default=os.getenv("SAGA_ACTIONS_FILE"), help="Versioned action registry JSON (legacy action maps remain readable)")
    parser.add_argument("--allow-legacy-action-recovery", action="store_true", default=os.getenv("SAGA_ALLOW_LEGACY_ACTION_RECOVERY", "").lower() in {"1", "true", "yes"}, help="Explicitly allow pre-0.5 steps without immutable action snapshots to use the currently registered action")
    parser.add_argument("--file-root", default=os.getenv("SAGA_FILE_ROOT", "./saga-files"), help="Root directory for the built-in create_text_file action")
    parser.add_argument("--database", default=os.getenv("SAGA_DATABASE"), help="SQLite path for single-node durable storage")
    parser.add_argument("--postgres-dsn", default=os.getenv("SAGA_POSTGRES_DSN"), help="PostgreSQL DSN for horizontally scaled durable storage")
    parser.add_argument("--postgres-pool-min", type=int, default=int(os.getenv("SAGA_POSTGRES_POOL_MIN", "1")), help="Minimum PostgreSQL connection-pool size")
    parser.add_argument("--postgres-pool-max", type=int, default=int(os.getenv("SAGA_POSTGRES_POOL_MAX", "10")), help="Maximum PostgreSQL connection-pool size")
    parser.add_argument("--worker-id", default=os.getenv("SAGA_WORKER_ID"), help="Unique worker identity; default is generated per process")
    parser.add_argument("--lease-seconds", type=float, default=float(os.getenv("SAGA_LEASE_SECONDS", "30")), help="Saga mutation/recovery lease duration")
    parser.add_argument("--recovery-limit", type=int, default=int(os.getenv("SAGA_RECOVERY_LIMIT", "100")), help="Maximum pending sagas claimed during startup recovery")
    parser.add_argument("--transport", choices=("stdio", "streamable-http", "sse"), default=os.getenv("SAGA_TRANSPORT", "stdio"))
    parser.add_argument("--host", default=os.getenv("SAGA_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SAGA_PORT", "8000")))
    parser.add_argument("--allowed-host", action="append", default=_env_list("SAGA_ALLOWED_HOSTS"), help="Allowed Host value for remote MCP; repeat or set SAGA_ALLOWED_HOSTS as CSV")
    parser.add_argument("--allowed-origin", action="append", default=_env_list("SAGA_ALLOWED_ORIGINS"), help="Allowed Origin value for browser MCP clients; repeat or set SAGA_ALLOWED_ORIGINS as CSV")

    parser.add_argument("--auth-mode", choices=("none", "jwt", "static"), default=os.getenv("SAGA_AUTH_MODE", "none"), help="Native OAuth bearer-token verification mode for Streamable HTTP")
    parser.add_argument("--auth-issuer", default=os.getenv("SAGA_AUTH_ISSUER"), help="OAuth/OIDC issuer URL advertised in protected-resource metadata")
    parser.add_argument("--auth-resource-url", default=os.getenv("SAGA_AUTH_RESOURCE_URL"), help="Public MCP resource URL, for example https://mcp.example.com/mcp")
    parser.add_argument("--auth-audience", default=os.getenv("SAGA_AUTH_AUDIENCE"), help="Expected aud claim for JWT access tokens")
    parser.add_argument("--auth-jwks-url", default=os.getenv("SAGA_AUTH_JWKS_URL"), help="IdP JWKS endpoint for signed JWT verification")
    parser.add_argument("--auth-jwt-algorithm", action="append", default=_env_list("SAGA_AUTH_JWT_ALGORITHMS") or ["RS256"], help="Allowed JWT signing algorithm; repeat for multiple")
    parser.add_argument("--auth-static-tokens", default=os.getenv("SAGA_AUTH_STATIC_TOKENS"), help="Operator-owned JSON token file for development/testing only")
    parser.add_argument("--auth-required-scope", action="append", default=_env_list("SAGA_AUTH_REQUIRED_SCOPES"), help="OAuth scope required by MCP auth middleware; repeat for multiple")
    parser.add_argument("--auth-tenant-claim", action="append", default=_env_list("SAGA_AUTH_TENANT_CLAIMS") or ["tenant_id", "tid", "org_id"], help="Token claim used as tenant ID; repeat to define fallbacks")
    parser.add_argument("--auth-roles-claim", default=os.getenv("SAGA_AUTH_ROLES_CLAIM", "roles"), help="Token claim containing viewer/operator/admin roles")
    parser.add_argument("--auth-principal-type-claim", default=os.getenv("SAGA_AUTH_PRINCIPAL_TYPE_CLAIM", "principal_type"), help="Token claim identifying user/service principal type")
    parser.add_argument("--auth-allow-missing-tenant", action="store_true", default=os.getenv("SAGA_AUTH_ALLOW_MISSING_TENANT", "").lower() in {"1", "true", "yes"}, help="Use tenant 'default' when an authenticated token has no configured tenant claim")

    parser.add_argument("--trust-identity-headers", action="store_true", default=os.getenv("SAGA_TRUST_IDENTITY_HEADERS", "").lower() in {"1", "true", "yes"}, help="Migration mode: trust identity headers from an authenticated reverse proxy")
    parser.add_argument("--allow-unauthenticated-http", action="store_true", default=os.getenv("SAGA_ALLOW_UNAUTHENTICATED_HTTP", "").lower() in {"1", "true", "yes"}, help="Allow non-local Streamable HTTP without native/proxy identity; private-network migration only")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("SAGA_DRY_RUN", "").lower() in {"1", "true", "yes"}, help="Preview actions, simulate failure, and log compensation without API calls")
    args = parser.parse_args()

    if args.database and args.postgres_dsn:
        parser.error("choose --database (SQLite) or --postgres-dsn, not both")
    if args.postgres_pool_min < 1 or args.postgres_pool_max < args.postgres_pool_min:
        parser.error("PostgreSQL pool sizes must satisfy 1 <= min <= max")
    if args.lease_seconds <= 0:
        parser.error("--lease-seconds must be positive")
    if args.recovery_limit < 1:
        parser.error("--recovery-limit must be positive")

    local_hosts = {"127.0.0.1", "localhost", "::1"}
    remote_streamable_http = args.transport == "streamable-http" and args.host not in local_hosts
    native_auth = args.auth_mode != "none"

    if native_auth and args.transport != "streamable-http":
        parser.error("native OAuth authentication is supported only with --transport streamable-http")
    if native_auth and args.trust_identity_headers:
        parser.error("choose native OAuth authentication or trusted proxy identity headers, not both")
    if native_auth and (not args.auth_issuer or not args.auth_resource_url):
        parser.error("--auth-issuer and --auth-resource-url are required when native auth is enabled")
    if args.auth_mode == "jwt" and (not args.auth_audience or not args.auth_jwks_url):
        parser.error("JWT auth requires --auth-audience and --auth-jwks-url")
    if args.auth_mode == "static" and not args.auth_static_tokens:
        parser.error("static auth requires --auth-static-tokens")

    if args.transport == "streamable-http":
        if remote_streamable_http and not args.allowed_host:
            parser.error("--allowed-host (or SAGA_ALLOWED_HOSTS) is required when Streamable HTTP binds beyond localhost")
        if args.allowed_origin and not args.allowed_host:
            parser.error("--allowed-origin requires at least one --allowed-host")
        if remote_streamable_http and not native_auth and not args.trust_identity_headers and not args.allow_unauthenticated_http:
            parser.error(
                "non-local Streamable HTTP requires native OAuth or --trust-identity-headers behind an authenticated proxy; "
                "use --allow-unauthenticated-http only for a controlled private network"
            )

    if args.postgres_dsn:
        store = PostgresSagaStore(args.postgres_dsn, min_pool_size=args.postgres_pool_min, max_pool_size=args.postgres_pool_max)
    elif args.database:
        store = SQLiteSagaStore(args.database)
    else:
        store = SagaStore()

    logger = lambda message: print(message, file=sys.stderr, flush=True)
    registry = load_action_registry(
        args.actions,
        dry_run=args.dry_run,
        log=logger,
        allow_legacy_recovery=args.allow_legacy_action_recovery,
    )
    registry.register_runtime(
        "create_text_file",
        FileTransactionTool(Path(args.file_root), logger),
        version="1.0.0",
        semantic={
            "domain": "filesystem",
            "operation": "create",
            "resource": "text_file",
            "reversibility": "full",
            "risk": "low",
            "effects": {"creates": ["filesystem.text_file"]},
        },
        input_schema={
            "type": "object",
            "required": ["path", "content"],
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "content": {"type": "string"},
                "simulate_error": {"type": "boolean"},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "required": ["path"],
            "properties": {"path": {"type": "string"}},
            "additionalProperties": False,
        },
    )
    coordinator = Coordinator(store, registry, worker_id=args.worker_id, lease_seconds=args.lease_seconds)
    recovered = coordinator.resume_pending_rollbacks(limit=args.recovery_limit)
    if recovered:
        logger(f"[recovery] worker {coordinator.worker_id} recovered {len(recovered)} saga(s)")

    if args.transport == "sse":
        print("[deprecated] dedicated SSE transport is retained only for migration; use --transport streamable-http", file=sys.stderr, flush=True)
        import uvicorn
        uvicorn.run(McpServer(coordinator).sse_app(), host=args.host, port=args.port)
        return

    from .execution import ExecutionContextResolver
    from .mcp_server import build_mcp_server, run_stdio

    resolver = ExecutionContextResolver(
        trust_proxy_headers=args.trust_identity_headers,
        require_proxy_identity=remote_streamable_http and args.trust_identity_headers and not args.allow_unauthenticated_http,
        allow_anonymous_http=args.transport == "streamable-http" and not native_auth and (not remote_streamable_http or args.allow_unauthenticated_http),
        tenant_claims=args.auth_tenant_claim,
        roles_claim=args.auth_roles_claim,
        principal_type_claim=args.auth_principal_type_claim,
        allow_missing_tenant=args.auth_allow_missing_tenant,
    )
    mcp_server = build_mcp_server(coordinator, resolver)

    if args.transport == "stdio":
        import anyio
        anyio.run(run_stdio, mcp_server)
        return

    from mcp.server.auth.settings import AuthSettings
    from mcp.server.transport_security import TransportSecuritySettings
    from pydantic import AnyHttpUrl
    import uvicorn

    transport_security = None
    if args.allowed_host or args.allowed_origin:
        transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=args.allowed_host,
            allowed_origins=args.allowed_origin,
        )

    auth_settings = None
    token_verifier = None
    if native_auth:
        from .auth import JwtTokenVerifier, StaticTokenVerifier
        auth_settings = AuthSettings(
            issuer_url=AnyHttpUrl(args.auth_issuer),
            resource_server_url=AnyHttpUrl(args.auth_resource_url),
            required_scopes=args.auth_required_scope,
        )
        if args.auth_mode == "jwt":
            token_verifier = JwtTokenVerifier(
                issuer=args.auth_issuer,
                audience=args.auth_audience,
                jwks_url=args.auth_jwks_url,
                algorithms=args.auth_jwt_algorithm,
            )
        else:
            token_verifier = StaticTokenVerifier.from_file(args.auth_static_tokens)

    app = mcp_server.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        transport_security=transport_security,
        host=args.host,
        auth=auth_settings,
        token_verifier=token_verifier,
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
