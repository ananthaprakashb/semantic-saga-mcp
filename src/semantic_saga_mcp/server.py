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
from .governance import GovernedCoordinator
from .observability import configure_telemetry
from .policy import PolicyError, load_policy_engine
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


class AuditArguments(SagaArguments):
    limit: int = Field(default=500, ge=1, le=5000)
    event_types: list[StrictStr] = Field(default_factory=list)


class TimelineArguments(SagaArguments):
    limit: int = Field(default=1000, ge=1, le=5000)


class PolicyDecisionsArguments(SagaArguments):
    limit: int = Field(default=500, ge=1, le=5000)


class PolicyStatusArguments(StrictModel):
    tenant_id: StrictStr | None = None


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
        "get_saga_timeline",
        "get_audit_events",
        "verify_audit_chain",
        "get_policy_status",
        "get_policy_decisions",
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
    "get_saga_timeline": TimelineArguments,
    "get_audit_events": AuditArguments,
    "verify_audit_chain": AuditArguments,
    "get_policy_status": PolicyStatusArguments,
    "get_policy_decisions": PolicyDecisionsArguments,
    "list_actions": ListActionsArguments,
    "get_action": GetActionArguments,
}

TOOLS = [
    {"name": "begin_saga", "description": "Start a durable transactional workflow.", "inputSchema": BeginArguments.model_json_schema()},
    {"name": "execute_saga_step", "description": "Immediately execute an action after current governance, schema, and action-policy checks.", "inputSchema": ExecuteArguments.model_json_schema()},
    {"name": "plan_saga_step", "description": "Persist a version-pinned workflow node; governance may add an approval gate or reject the plan.", "inputSchema": PlanStepArguments.model_json_schema()},
    {"name": "run_ready_steps", "description": "Re-evaluate current governance and execute ready DAG nodes in bounded dependency waves.", "inputSchema": RunReadyArguments.model_json_schema()},
    {"name": "approve_saga_step", "description": "Approve or reject a workflow node; approvals are subject to tenant governance while rejection remains fail-safe.", "inputSchema": ApprovalArguments.model_json_schema()},
    {"name": "retry_saga_step", "description": "Return a failed/rejected/blocked workflow node to scheduling after current governance checks.", "inputSchema": RetryStepArguments.model_json_schema()},
    {"name": "checkpoint_saga", "description": "Persist a named workflow checkpoint and operator/agent-provided checkpoint data.", "inputSchema": CheckpointArguments.model_json_schema()},
    {"name": "commit_saga", "description": "Commit a completed saga after current tenant governance checks.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "rollback_saga", "description": "Compensate saga steps in reverse order; rollback remains available as the safety path.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "trigger_rollback", "description": "Immediately start compensation after a client-detected failure.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "get_saga", "description": "Inspect saga, workflow DAG, checkpoints, action versions, results, approvals, and recovery state.", "inputSchema": SagaArguments.model_json_schema()},
    {"name": "get_saga_timeline", "description": "Inspect a payload-safe timeline combining steps, workflow nodes, audit evidence, and integrity status.", "inputSchema": TimelineArguments.model_json_schema()},
    {"name": "get_audit_events", "description": "Read append-only audit events without exposing action inputs, results, or secret material.", "inputSchema": AuditArguments.model_json_schema()},
    {"name": "verify_audit_chain", "description": "Verify the per-saga SHA-256 audit hash chain.", "inputSchema": AuditArguments.model_json_schema()},
    {"name": "get_policy_status", "description": "Inspect the effective governance backend, revision, budgets, approval threshold, and rule ids for the caller tenant.", "inputSchema": PolicyStatusArguments.model_json_schema()},
    {"name": "get_policy_decisions", "description": "Read durable governance decisions and safety overrides for one tenant-owned saga.", "inputSchema": PolicyDecisionsArguments.model_json_schema()},
    {"name": "list_actions", "description": "List active action contracts, schemas, semantic effects, risk, hashes, and execution policies.", "inputSchema": ListActionsArguments.model_json_schema()},
    {"name": "get_action", "description": "Inspect one registered action contract by id and optional version.", "inputSchema": GetActionArguments.model_json_schema()},
]

SYSTEM_PROMPT = (
    "Wrap multi-step side effects in the Saga Coordinator. Inspect actions and governance before execution. "
    "Use `plan_saga_step` for governed workflows because policy can require approval based on action risk, tenant rules, "
    "or budgets. Obtain approvals when required, then call `run_ready_steps`; current policy is re-evaluated before side "
    "effects. Use `get_policy_status` and `get_policy_decisions` for governance evidence, and `get_saga_timeline` plus "
    "`verify_audit_chain` for operational review. If a saga enters RECOVERY_REQUIRED, reconcile the external outcome "
    "before forcing a retry; rollback remains available for compensation safety."
)
PROMPTS = [{"name": "saga-coordinator", "title": "Saga Coordinator", "description": SYSTEM_PROMPT, "arguments": []}]


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
                result = {"description": PROMPTS[0]["description"], "messages": [{"role": "user", "content": {"type": "text", "text": SYSTEM_PROMPT}}]}
            else:
                return self._error(request.id, -32601, f"Method not found: {request.method}")
            return {"jsonrpc": "2.0", "id": request.id, "result": result}
        except ValidationError as exc:
            return self._error(request_id, -32602, f"Invalid request: {exc}")
        except (SagaError, ActionRegistryError, PolicyError, KeyError, TypeError, ValueError) as exc:
            return self._error(request_id, -32602, str(exc))
        except Exception as exc:
            return self._error(request_id, -32603, f"Internal error: {exc}")

    def _call(self, raw_params: dict[str, Any], session_id: str) -> dict[str, Any]:
        params = ToolCallParams.model_validate(raw_params)
        args = ARGUMENT_MODELS[params.name].model_validate(params.arguments)
        if isinstance(args, BeginArguments):
            value = self.coordinator.begin(args.metadata, session_id=session_id)
        elif isinstance(args, PlanStepArguments):
            value = self.coordinator.plan_step(args.saga_id, args.action, args.input, session_id=session_id, key=args.key, depends_on=list(args.depends_on), approval_required=args.approval_required)
        elif isinstance(args, ExecuteArguments):
            value = self.coordinator.execute(args.saga_id, args.action, args.input, session_id=session_id)
        elif isinstance(args, RunReadyArguments):
            value = self.coordinator.run_ready_steps(args.saga_id, session_id=session_id, max_parallel=args.max_parallel, max_steps=args.max_steps)
        elif isinstance(args, ApprovalArguments):
            value = self.coordinator.approve_step(args.saga_id, args.node_id, session_id=session_id, approved=args.approved, reason=args.reason)
        elif isinstance(args, RetryStepArguments):
            value = self.coordinator.retry_step(args.saga_id, args.node_id, session_id=session_id, force=args.force)
        elif isinstance(args, CheckpointArguments):
            value = self.coordinator.checkpoint(args.saga_id, args.name, args.data, session_id=session_id)
        elif isinstance(args, AuditArguments):
            if params.name == "verify_audit_chain":
                value = self.coordinator.verify_audit_chain(args.saga_id, session_id=session_id)  # type: ignore[attr-defined]
            else:
                value = self.coordinator.get_audit_events(args.saga_id, session_id=session_id, limit=args.limit, event_types=set(args.event_types) if args.event_types else None)  # type: ignore[attr-defined]
        elif isinstance(args, TimelineArguments):
            value = self.coordinator.get_timeline(args.saga_id, session_id=session_id, limit=args.limit)  # type: ignore[attr-defined]
        elif isinstance(args, PolicyStatusArguments):
            value = self.coordinator.get_policy_status(args.tenant_id)  # type: ignore[attr-defined]
        elif isinstance(args, PolicyDecisionsArguments):
            value = self.coordinator.get_policy_decisions(args.saga_id, session_id=session_id, limit=args.limit)  # type: ignore[attr-defined]
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
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable MCP saga coordinator")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--actions", default=os.getenv("SAGA_ACTIONS_FILE"), help="Versioned action registry JSON (legacy action maps remain readable)")
    parser.add_argument("--allow-legacy-action-recovery", action="store_true", default=os.getenv("SAGA_ALLOW_LEGACY_ACTION_RECOVERY", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--file-root", default=os.getenv("SAGA_FILE_ROOT", "./saga-files"))
    parser.add_argument("--database", default=os.getenv("SAGA_DATABASE"))
    parser.add_argument("--postgres-dsn", default=os.getenv("SAGA_POSTGRES_DSN"))
    parser.add_argument("--postgres-pool-min", type=int, default=int(os.getenv("SAGA_POSTGRES_POOL_MIN", "1")))
    parser.add_argument("--postgres-pool-max", type=int, default=int(os.getenv("SAGA_POSTGRES_POOL_MAX", "10")))
    parser.add_argument("--worker-id", default=os.getenv("SAGA_WORKER_ID"))
    parser.add_argument("--lease-seconds", type=float, default=float(os.getenv("SAGA_LEASE_SECONDS", "30")))
    parser.add_argument("--recovery-limit", type=int, default=int(os.getenv("SAGA_RECOVERY_LIMIT", "100")))
    parser.add_argument("--transport", choices=("stdio", "streamable-http", "sse"), default=os.getenv("SAGA_TRANSPORT", "stdio"))
    parser.add_argument("--host", default=os.getenv("SAGA_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SAGA_PORT", "8000")))
    parser.add_argument("--allowed-host", action="append", default=_env_list("SAGA_ALLOWED_HOSTS"))
    parser.add_argument("--allowed-origin", action="append", default=_env_list("SAGA_ALLOWED_ORIGINS"))

    parser.add_argument("--auth-mode", choices=("none", "jwt", "static"), default=os.getenv("SAGA_AUTH_MODE", "none"))
    parser.add_argument("--auth-issuer", default=os.getenv("SAGA_AUTH_ISSUER"))
    parser.add_argument("--auth-resource-url", default=os.getenv("SAGA_AUTH_RESOURCE_URL"))
    parser.add_argument("--auth-audience", default=os.getenv("SAGA_AUTH_AUDIENCE"))
    parser.add_argument("--auth-jwks-url", default=os.getenv("SAGA_AUTH_JWKS_URL"))
    parser.add_argument("--auth-jwt-algorithm", action="append", default=_env_list("SAGA_AUTH_JWT_ALGORITHMS") or ["RS256"])
    parser.add_argument("--auth-static-tokens", default=os.getenv("SAGA_AUTH_STATIC_TOKENS"))
    parser.add_argument("--auth-required-scope", action="append", default=_env_list("SAGA_AUTH_REQUIRED_SCOPES"))
    parser.add_argument("--auth-tenant-claim", action="append", default=_env_list("SAGA_AUTH_TENANT_CLAIMS") or ["tenant_id", "tid", "org_id"])
    parser.add_argument("--auth-roles-claim", default=os.getenv("SAGA_AUTH_ROLES_CLAIM", "roles"))
    parser.add_argument("--auth-principal-type-claim", default=os.getenv("SAGA_AUTH_PRINCIPAL_TYPE_CLAIM", "principal_type"))
    parser.add_argument("--auth-allow-missing-tenant", action="store_true", default=os.getenv("SAGA_AUTH_ALLOW_MISSING_TENANT", "").lower() in {"1", "true", "yes"})

    parser.add_argument("--policy-mode", choices=("none", "json", "opa"), default=os.getenv("SAGA_POLICY_MODE", "none"), help="Governance backend: disabled, built-in JSON policy, or external OPA")
    parser.add_argument("--policy-file", default=os.getenv("SAGA_POLICY_FILE"), help="Tenant governance JSON file for --policy-mode json")
    parser.add_argument("--policy-opa-url", default=os.getenv("SAGA_POLICY_OPA_URL"), help="OPA base URL for --policy-mode opa")
    parser.add_argument("--policy-opa-decision-path", default=os.getenv("SAGA_POLICY_OPA_DECISION_PATH", "semantic_saga/decision"))
    parser.add_argument("--policy-opa-timeout", type=float, default=float(os.getenv("SAGA_POLICY_OPA_TIMEOUT", "2")))
    parser.add_argument("--policy-opa-token-env", default=os.getenv("SAGA_POLICY_OPA_TOKEN_ENV", "SAGA_OPA_TOKEN"), help="Environment variable holding the optional OPA bearer token")

    parser.add_argument("--otel-endpoint", default=os.getenv("SAGA_OTEL_ENDPOINT"))
    parser.add_argument("--otel-headers", default=os.getenv("SAGA_OTEL_HEADERS"))
    parser.add_argument("--otel-service-name", default=os.getenv("SAGA_OTEL_SERVICE_NAME", "semantic-saga-mcp"))
    parser.add_argument("--trust-identity-headers", action="store_true", default=os.getenv("SAGA_TRUST_IDENTITY_HEADERS", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--allow-unauthenticated-http", action="store_true", default=os.getenv("SAGA_ALLOW_UNAUTHENTICATED_HTTP", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("SAGA_DRY_RUN", "").lower() in {"1", "true", "yes"})
    args = parser.parse_args()

    if args.database and args.postgres_dsn:
        parser.error("choose --database (SQLite) or --postgres-dsn, not both")
    if args.postgres_pool_min < 1 or args.postgres_pool_max < args.postgres_pool_min:
        parser.error("PostgreSQL pool sizes must satisfy 1 <= min <= max")
    if args.lease_seconds <= 0 or args.recovery_limit < 1:
        parser.error("lease and recovery settings must be positive")
    if args.policy_opa_timeout <= 0:
        parser.error("--policy-opa-timeout must be positive")

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
    if args.policy_mode == "json" and not args.policy_file:
        parser.error("--policy-mode json requires --policy-file")
    if args.policy_mode == "opa" and not args.policy_opa_url:
        parser.error("--policy-mode opa requires --policy-opa-url")

    if args.transport == "streamable-http":
        if remote_streamable_http and not args.allowed_host:
            parser.error("--allowed-host (or SAGA_ALLOWED_HOSTS) is required when Streamable HTTP binds beyond localhost")
        if args.allowed_origin and not args.allowed_host:
            parser.error("--allowed-origin requires at least one --allowed-host")
        if remote_streamable_http and not native_auth and not args.trust_identity_headers and not args.allow_unauthenticated_http:
            parser.error("non-local Streamable HTTP requires native OAuth or trusted proxy identity; unauthenticated mode is for controlled private networks only")

    if args.postgres_dsn:
        store = PostgresSagaStore(args.postgres_dsn, min_pool_size=args.postgres_pool_min, max_pool_size=args.postgres_pool_max)
    elif args.database:
        store = SQLiteSagaStore(args.database)
    else:
        store = SagaStore()

    try:
        telemetry = configure_telemetry(service_name=args.otel_service_name, endpoint=args.otel_endpoint, headers=args.otel_headers)
        policy_engine = load_policy_engine(
            args.policy_mode,
            policy_file=args.policy_file,
            opa_url=args.policy_opa_url,
            opa_decision_path=args.policy_opa_decision_path,
            opa_timeout_seconds=args.policy_opa_timeout,
            opa_token_env=args.policy_opa_token_env,
        )
    except (RuntimeError, PolicyError) as exc:
        parser.error(str(exc))

    logger = lambda message: print(message, file=sys.stderr, flush=True)
    registry = load_action_registry(args.actions, dry_run=args.dry_run, log=logger, allow_legacy_recovery=args.allow_legacy_action_recovery)
    registry.register_runtime(
        "create_text_file",
        FileTransactionTool(Path(args.file_root), logger),
        version="1.0.0",
        semantic={"domain": "filesystem", "operation": "create", "resource": "text_file", "reversibility": "full", "risk": "low", "effects": {"creates": ["filesystem.text_file"]}},
        input_schema={"type": "object", "required": ["path", "content"], "properties": {"path": {"type": "string", "minLength": 1}, "content": {"type": "string"}, "simulate_error": {"type": "boolean"}}, "additionalProperties": False},
        output_schema={"type": "object", "required": ["path"], "properties": {"path": {"type": "string"}}, "additionalProperties": False},
    )
    coordinator = GovernedCoordinator(
        store,
        registry,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        telemetry=telemetry,
        policy_engine=policy_engine,
    )
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
        transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=True, allowed_hosts=args.allowed_host, allowed_origins=args.allowed_origin)

    auth_settings = None
    token_verifier = None
    if native_auth:
        from .auth import JwtTokenVerifier, StaticTokenVerifier
        auth_settings = AuthSettings(issuer_url=AnyHttpUrl(args.auth_issuer), resource_server_url=AnyHttpUrl(args.auth_resource_url), required_scopes=args.auth_required_scope)
        if args.auth_mode == "jwt":
            token_verifier = JwtTokenVerifier(issuer=args.auth_issuer, audience=args.auth_audience, jwks_url=args.auth_jwks_url, algorithms=args.auth_jwt_algorithm)
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
