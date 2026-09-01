from __future__ import annotations

import argparse
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import __version__
from .actions import FileTransactionTool
from .a2a_adapter import SemanticSagaA2AExecutor
from .auth import JwtTokenVerifier, StaticTokenVerifier
from .governance import GovernedCoordinator
from .observability import configure_telemetry
from .policy import PolicyError, load_policy_engine
from .registry import load_action_registry
from .store import PostgresSagaStore, SagaStore, SQLiteSagaStore


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item for item in value.replace(",", " ").split() if item)
    if isinstance(value, list):
        return tuple(item for item in value if isinstance(item, str) and item)
    return ()


def _tenant_from_claims(claims: Mapping[str, Any], tenant_claims: Iterable[str], allow_missing: bool) -> str:
    for name in tenant_claims:
        value = claims.get(name)
        if isinstance(value, str) and value:
            return value
    if allow_missing:
        return "default"
    raise RuntimeError("Authenticated A2A token is missing a configured tenant claim")


def _identity_from_token(
    token: Any,
    *,
    tenant_claims: Iterable[str],
    roles_claim: str,
    principal_type_claim: str,
    allow_missing_tenant: bool,
) -> dict[str, Any]:
    claims = token.claims if isinstance(token.claims, dict) else {}
    tenant_id = _tenant_from_claims(claims, tenant_claims, allow_missing_tenant)
    principal_id = token.subject or claims.get("sub") or token.client_id
    if not isinstance(principal_id, str) or not principal_id:
        raise RuntimeError("Authenticated A2A token does not identify a principal")
    principal_type = claims.get(principal_type_claim)
    if not isinstance(principal_type, str) or not principal_type:
        principal_type = "user" if token.subject or claims.get("sub") else "service"
    return {
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "principal_type": principal_type,
        "roles": _strings(claims.get(roles_claim)),
        "scopes": tuple(token.scopes),
        "authenticated": True,
    }


class A2AAuthenticationBackend:
    """Starlette authentication backend that reuses Semantic Saga token verifiers."""

    def __init__(
        self,
        verifier: Any | None,
        *,
        allow_unauthenticated: bool,
        tenant_claims: Iterable[str],
        roles_claim: str,
        principal_type_claim: str,
        allow_missing_tenant: bool,
    ) -> None:
        self.verifier = verifier
        self.allow_unauthenticated = allow_unauthenticated
        self.tenant_claims = tuple(tenant_claims)
        self.roles_claim = roles_claim
        self.principal_type_claim = principal_type_claim
        self.allow_missing_tenant = allow_missing_tenant

    async def authenticate(self, conn: Any) -> Any:
        from starlette.authentication import AuthCredentials, AuthenticationError, SimpleUser

        # A2A discovery is public. The card advertises the security required for
        # actual task interactions.
        if conn.url.path.endswith("/.well-known/agent-card.json"):
            return None
        authorization = conn.headers.get("authorization", "")
        if authorization.lower().startswith("bearer ") and self.verifier is not None:
            raw = authorization.split(None, 1)[1].strip()
            token = await self.verifier.verify_token(raw)
            if token is None:
                raise AuthenticationError("Invalid bearer token")
            try:
                identity = _identity_from_token(
                    token,
                    tenant_claims=self.tenant_claims,
                    roles_claim=self.roles_claim,
                    principal_type_claim=self.principal_type_claim,
                    allow_missing_tenant=self.allow_missing_tenant,
                )
            except RuntimeError as exc:
                raise AuthenticationError(str(exc)) from exc
            conn.scope["semantic_saga_identity"] = identity
            return AuthCredentials(["authenticated"]), SimpleUser(identity["principal_id"])
        if self.allow_unauthenticated:
            identity = {
                "tenant_id": "local-a2a",
                "principal_id": "a2a-local",
                "principal_type": "development",
                "roles": ("admin",),
                "scopes": (),
                "authenticated": False,
            }
            conn.scope["semantic_saga_identity"] = identity
            return AuthCredentials(["authenticated"]), SimpleUser("a2a-local")
        raise AuthenticationError("Bearer authentication is required")


class SemanticSagaCallContextBuilder:
    """Populate A2A task ownership and executor state from authenticated identity."""

    def __init__(self) -> None:
        from a2a.server.routes import DefaultServerCallContextBuilder

        self._default = DefaultServerCallContextBuilder()

    def build(self, request: Any) -> Any:
        context = self._default.build(request)
        identity = request.scope.get("semantic_saga_identity")
        if isinstance(identity, Mapping):
            context.tenant = str(identity["tenant_id"])
            context.state["semantic_saga_identity"] = dict(identity)
        return context


def _task_owner(context: Any) -> str:
    # A2A protocol task visibility follows Semantic Saga's tenant ownership,
    # enabling authorized peer agents in one organization to resume each other's
    # durable tasks while preventing cross-tenant task discovery.
    return context.tenant or "anonymous"


def _database_task_url(*, sqlite_path: str | None, postgres_dsn: str | None) -> str | None:
    if sqlite_path:
        absolute = Path(sqlite_path).expanduser().resolve()
        return "sqlite+aiosqlite:///" + str(absolute)
    if postgres_dsn:
        if postgres_dsn.startswith("postgresql://"):
            return "postgresql+asyncpg://" + postgres_dsn[len("postgresql://"):]
        if postgres_dsn.startswith("postgres://"):
            return "postgresql+asyncpg://" + postgres_dsn[len("postgres://"):]
        if postgres_dsn.startswith("postgresql+asyncpg://"):
            return postgres_dsn
        raise RuntimeError("A2A PostgreSQL DSN must use postgresql://, postgres://, or postgresql+asyncpg://")
    return None


def build_task_store(*, sqlite_path: str | None, postgres_dsn: str | None) -> tuple[Any, Any | None]:
    from a2a.server.tasks import DatabaseTaskStore, InMemoryTaskStore

    url = _database_task_url(sqlite_path=sqlite_path, postgres_dsn=postgres_dsn)
    if url is None:
        return InMemoryTaskStore(owner_resolver=_task_owner), None
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url, pool_pre_ping=True)
    return DatabaseTaskStore(engine=engine, table_name="a2a_tasks", owner_resolver=_task_owner), engine


def build_agent_card(public_url: str, rpc_path: str, *, auth_required: bool) -> Any:
    from a2a.types import (
        AgentCapabilities,
        AgentCard,
        AgentInterface,
        AgentSkill,
        HTTPAuthSecurityScheme,
        SecurityRequirement,
        SecurityScheme,
        StringList,
    )

    endpoint = public_url.rstrip("/") + rpc_path
    security_schemes: dict[str, Any] = {}
    security_requirements: list[Any] = []
    if auth_required:
        security_schemes["bearer"] = SecurityScheme(
            http_auth_security_scheme=HTTPAuthSecurityScheme(
                description="OAuth/OIDC bearer access token validated by Semantic Saga",
                scheme="Bearer",
                bearer_format="JWT",
            )
        )
        security_requirements.append(SecurityRequirement(schemes={"bearer": StringList(list=[])}))
    return AgentCard(
        name="Semantic Saga Transaction Coordinator",
        description=(
            "Governed, durable transactional orchestration for agent-to-agent side effects, "
            "approvals, recovery, compensation, policy inspection, and audit. Commands are "
            "strict structured application/json Part.data objects."
        ),
        version=__version__,
        capabilities=AgentCapabilities(streaming=False, push_notifications=False),
        supported_interfaces=[
            AgentInterface(url=endpoint, protocol_binding="JSONRPC", protocol_version="1.0")
        ],
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        security_schemes=security_schemes,
        security_requirements=security_requirements,
        skills=[
            AgentSkill(
                id="transactional-orchestration",
                name="Transactional orchestration",
                description="Begin, plan, run, commit, or roll back durable governed sagas.",
                tags=["saga", "transactions", "orchestration", "compensation"],
                examples=['{"operation":"begin","metadata":{"workflow":"onboarding"}}'],
                input_modes=["application/json"],
                output_modes=["application/json"],
                security_requirements=security_requirements,
            ),
            AgentSkill(
                id="governed-side-effects",
                name="Governed side effects",
                description="Execute, approve, retry, and checkpoint versioned side effects under tenant policy.",
                tags=["governance", "approval", "policy", "side-effects"],
                examples=['{"operation":"plan","saga_id":"...","action":"create_repository","input":{}}'],
                input_modes=["application/json"],
                output_modes=["application/json"],
                security_requirements=security_requirements,
            ),
            AgentSkill(
                id="transaction-inspection",
                name="Transaction inspection",
                description="Inspect saga state, timelines, audit integrity, action contracts, and policy decisions.",
                tags=["audit", "observability", "policy", "recovery"],
                examples=['{"operation":"timeline","saga_id":"..."}'],
                input_modes=["application/json"],
                output_modes=["application/json"],
                security_requirements=security_requirements,
            ),
        ],
    )


def build_app(
    coordinator: Any,
    *,
    task_store: Any,
    task_engine: Any | None,
    public_url: str,
    rpc_path: str,
    verifier: Any | None,
    auth_required: bool,
    allow_unauthenticated: bool,
    tenant_claims: Iterable[str],
    roles_claim: str,
    principal_type_claim: str,
    allow_missing_tenant: bool,
) -> Any:
    from a2a.server.request_handlers import DefaultRequestHandler
    from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.authentication import AuthenticationMiddleware
    from starlette.responses import JSONResponse

    card = build_agent_card(public_url, rpc_path, auth_required=auth_required)
    handler = DefaultRequestHandler(
        agent_executor=SemanticSagaA2AExecutor(coordinator),
        task_store=task_store,
        agent_card=card,
    )
    routes = [
        *create_agent_card_routes(card),
        *create_jsonrpc_routes(
            handler,
            rpc_url=rpc_path,
            context_builder=SemanticSagaCallContextBuilder(),
            enable_v0_3_compat=False,
        ),
    ]

    backend = A2AAuthenticationBackend(
        verifier,
        allow_unauthenticated=allow_unauthenticated,
        tenant_claims=tenant_claims,
        roles_claim=roles_claim,
        principal_type_claim=principal_type_claim,
        allow_missing_tenant=allow_missing_tenant,
    )

    def auth_error(_request: Any, exc: Exception) -> Any:
        return JSONResponse({"error": str(exc)}, status_code=401)

    @asynccontextmanager
    async def lifespan(_app: Any):
        initialize = getattr(task_store, "initialize", None)
        if initialize is not None:
            await initialize()
        try:
            yield
        finally:
            if task_engine is not None:
                await task_engine.dispose()

    return Starlette(
        routes=routes,
        middleware=[Middleware(AuthenticationMiddleware, backend=backend, on_error=auth_error)],
        lifespan=lifespan,
    )


def _env_list(name: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Semantic Saga A2A 1.0 interoperability server")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--actions", default=os.getenv("SAGA_ACTIONS_FILE"))
    parser.add_argument("--allow-legacy-action-recovery", action="store_true", default=os.getenv("SAGA_ALLOW_LEGACY_ACTION_RECOVERY", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--file-root", default=os.getenv("SAGA_FILE_ROOT", "./saga-files"))
    parser.add_argument("--database", default=os.getenv("SAGA_DATABASE"), help="SQLite saga DB; A2A tasks use table a2a_tasks in the same file")
    parser.add_argument("--postgres-dsn", default=os.getenv("SAGA_POSTGRES_DSN"), help="PostgreSQL saga DB; A2A tasks use table a2a_tasks in the same database")
    parser.add_argument("--postgres-pool-min", type=int, default=int(os.getenv("SAGA_POSTGRES_POOL_MIN", "1")))
    parser.add_argument("--postgres-pool-max", type=int, default=int(os.getenv("SAGA_POSTGRES_POOL_MAX", "10")))
    parser.add_argument("--worker-id", default=os.getenv("SAGA_WORKER_ID"))
    parser.add_argument("--lease-seconds", type=float, default=float(os.getenv("SAGA_LEASE_SECONDS", "30")))
    parser.add_argument("--recovery-limit", type=int, default=int(os.getenv("SAGA_RECOVERY_LIMIT", "100")))
    parser.add_argument("--host", default=os.getenv("SAGA_A2A_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SAGA_A2A_PORT", "8100")))
    parser.add_argument("--a2a-public-url", default=os.getenv("SAGA_A2A_PUBLIC_URL"))
    parser.add_argument("--a2a-rpc-path", default=os.getenv("SAGA_A2A_RPC_PATH", "/a2a"))

    parser.add_argument("--auth-mode", choices=("none", "jwt", "static"), default=os.getenv("SAGA_AUTH_MODE", "none"))
    parser.add_argument("--auth-issuer", default=os.getenv("SAGA_AUTH_ISSUER"))
    parser.add_argument("--auth-audience", default=os.getenv("SAGA_AUTH_AUDIENCE"))
    parser.add_argument("--auth-jwks-url", default=os.getenv("SAGA_AUTH_JWKS_URL"))
    parser.add_argument("--auth-jwt-algorithm", action="append", default=_env_list("SAGA_AUTH_JWT_ALGORITHMS") or ["RS256"])
    parser.add_argument("--auth-static-tokens", default=os.getenv("SAGA_AUTH_STATIC_TOKENS"))
    parser.add_argument("--auth-tenant-claim", action="append", default=_env_list("SAGA_AUTH_TENANT_CLAIMS") or ["tenant_id", "tid", "org_id"])
    parser.add_argument("--auth-roles-claim", default=os.getenv("SAGA_AUTH_ROLES_CLAIM", "roles"))
    parser.add_argument("--auth-principal-type-claim", default=os.getenv("SAGA_AUTH_PRINCIPAL_TYPE_CLAIM", "principal_type"))
    parser.add_argument("--auth-allow-missing-tenant", action="store_true", default=os.getenv("SAGA_AUTH_ALLOW_MISSING_TENANT", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--allow-unauthenticated-a2a", action="store_true", default=os.getenv("SAGA_ALLOW_UNAUTHENTICATED_A2A", "").lower() in {"1", "true", "yes"})

    parser.add_argument("--policy-mode", choices=("none", "json", "opa"), default=os.getenv("SAGA_POLICY_MODE", "none"))
    parser.add_argument("--policy-file", default=os.getenv("SAGA_POLICY_FILE"))
    parser.add_argument("--policy-opa-url", default=os.getenv("SAGA_POLICY_OPA_URL"))
    parser.add_argument("--policy-opa-decision-path", default=os.getenv("SAGA_POLICY_OPA_DECISION_PATH", "semantic_saga/decision"))
    parser.add_argument("--policy-opa-timeout", type=float, default=float(os.getenv("SAGA_POLICY_OPA_TIMEOUT", "2")))
    parser.add_argument("--policy-opa-token-env", default=os.getenv("SAGA_POLICY_OPA_TOKEN_ENV", "SAGA_OPA_TOKEN"))
    parser.add_argument("--otel-endpoint", default=os.getenv("SAGA_OTEL_ENDPOINT"))
    parser.add_argument("--otel-headers", default=os.getenv("SAGA_OTEL_HEADERS"))
    parser.add_argument("--otel-service-name", default=os.getenv("SAGA_OTEL_SERVICE_NAME", "semantic-saga-a2a"))
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("SAGA_DRY_RUN", "").lower() in {"1", "true", "yes"})
    args = parser.parse_args()

    if args.database and args.postgres_dsn:
        parser.error("choose --database or --postgres-dsn, not both")
    if args.postgres_pool_min < 1 or args.postgres_pool_max < args.postgres_pool_min:
        parser.error("PostgreSQL pool sizes must satisfy 1 <= min <= max")
    if args.lease_seconds <= 0 or args.recovery_limit < 1 or args.policy_opa_timeout <= 0:
        parser.error("lease, recovery, and policy timeout settings must be positive")
    if not args.a2a_rpc_path.startswith("/") or args.a2a_rpc_path == "/":
        parser.error("--a2a-rpc-path must be a non-root absolute path")
    if args.policy_mode == "json" and not args.policy_file:
        parser.error("--policy-mode json requires --policy-file")
    if args.policy_mode == "opa" and not args.policy_opa_url:
        parser.error("--policy-mode opa requires --policy-opa-url")
    if args.auth_mode == "jwt" and (not args.auth_issuer or not args.auth_audience or not args.auth_jwks_url):
        parser.error("JWT auth requires --auth-issuer, --auth-audience, and --auth-jwks-url")
    if args.auth_mode == "static" and not args.auth_static_tokens:
        parser.error("static auth requires --auth-static-tokens")

    local_hosts = {"127.0.0.1", "localhost", "::1"}
    remote = args.host not in local_hosts
    auth_required = args.auth_mode != "none"
    if remote and not auth_required and not args.allow_unauthenticated_a2a:
        parser.error("non-local A2A binding requires bearer authentication; unauthenticated mode is for controlled private networks only")
    allow_unauthenticated = not auth_required and (not remote or args.allow_unauthenticated_a2a)
    public_url = args.a2a_public_url or f"http://{args.host}:{args.port}"

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
        task_store, task_engine = build_task_store(sqlite_path=args.database, postgres_dsn=args.postgres_dsn)
    except (RuntimeError, PolicyError, ImportError) as exc:
        parser.error(str(exc))

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
            "domain": "filesystem", "operation": "create", "resource": "text_file",
            "reversibility": "full", "risk": "low", "effects": {"creates": ["filesystem.text_file"]},
        },
        input_schema={
            "type": "object", "required": ["path", "content"],
            "properties": {"path": {"type": "string", "minLength": 1}, "content": {"type": "string"}, "simulate_error": {"type": "boolean"}},
            "additionalProperties": False,
        },
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
        logger(f"[recovery] A2A worker {coordinator.worker_id} recovered {len(recovered)} saga(s)")

    verifier = None
    if args.auth_mode == "jwt":
        verifier = JwtTokenVerifier(
            issuer=args.auth_issuer,
            audience=args.auth_audience,
            jwks_url=args.auth_jwks_url,
            algorithms=args.auth_jwt_algorithm,
        )
    elif args.auth_mode == "static":
        verifier = StaticTokenVerifier.from_file(args.auth_static_tokens)

    app = build_app(
        coordinator,
        task_store=task_store,
        task_engine=task_engine,
        public_url=public_url,
        rpc_path=args.a2a_rpc_path,
        verifier=verifier,
        auth_required=auth_required,
        allow_unauthenticated=allow_unauthenticated,
        tenant_claims=args.auth_tenant_claim,
        roles_claim=args.auth_roles_claim,
        principal_type_claim=args.auth_principal_type_claim,
        allow_missing_tenant=args.auth_allow_missing_tenant,
    )
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
