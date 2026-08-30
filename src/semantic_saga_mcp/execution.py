from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any, Iterable

from mcp.server.auth.middleware.auth_context import get_access_token

from .auth import IdentityError


@dataclass(frozen=True)
class ExecutionContext:
    """Authenticated, transport-independent identity for one MCP request."""

    owner_id: str
    transport: str
    identity_source: str
    tenant_id: str
    principal_id: str
    principal_type: str
    roles: tuple[str, ...]
    scopes: tuple[str, ...]
    authenticated: bool

    def audit_metadata(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "principal_id": self.principal_id,
            "principal_type": self.principal_type,
            "roles": list(self.roles),
            "identity_source": self.identity_source,
        }


class ExecutionContextResolver:
    """Resolve OAuth, trusted-proxy, or local process identity.

    Durable HTTP saga ownership is tenant-scoped, not connection- or token-scoped.
    This permits multiple authorized agents in one organization to continue the
    same explicit saga while preventing cross-tenant access.
    """

    TENANT_HEADER = "x-semantic-saga-tenant"
    PRINCIPAL_HEADER = "x-semantic-saga-principal"
    PRINCIPAL_TYPE_HEADER = "x-semantic-saga-principal-type"
    ROLES_HEADER = "x-semantic-saga-roles"

    def __init__(
        self,
        *,
        trust_proxy_headers: bool = False,
        require_proxy_identity: bool = False,
        allow_anonymous_http: bool = False,
        tenant_claims: Iterable[str] = ("tenant_id", "tid", "org_id"),
        roles_claim: str = "roles",
        principal_type_claim: str = "principal_type",
        allow_missing_tenant: bool = False,
        local_owner_id: str | None = None,
    ) -> None:
        if require_proxy_identity and not trust_proxy_headers:
            raise ValueError("require_proxy_identity needs trust_proxy_headers")
        self.trust_proxy_headers = trust_proxy_headers
        self.require_proxy_identity = require_proxy_identity
        self.allow_anonymous_http = allow_anonymous_http
        self.tenant_claims = tuple(item for item in tenant_claims if item)
        self.roles_claim = roles_claim
        self.principal_type_claim = principal_type_claim
        self.allow_missing_tenant = allow_missing_tenant
        self.local_owner_id = local_owner_id or f"stdio:{uuid.uuid4()}"

    @staticmethod
    def _digest(namespace: str, value: str) -> str:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
        return f"{namespace}:{digest}"

    @staticmethod
    def _strings(value: Any) -> tuple[str, ...]:
        if isinstance(value, str):
            return tuple(item for item in value.replace(",", " ").split() if item)
        if isinstance(value, list):
            return tuple(item for item in value if isinstance(item, str) and item)
        return ()

    def _tenant_from_claims(self, claims: dict[str, Any]) -> str:
        for claim in self.tenant_claims:
            value = claims.get(claim)
            if isinstance(value, str) and value:
                return value
        if self.allow_missing_tenant:
            return "default"
        raise IdentityError(
            "Authenticated token is missing a tenant claim; expected one of "
            + ", ".join(self.tenant_claims)
        )

    def _from_oauth(self) -> ExecutionContext | None:
        token = get_access_token()
        if token is None:
            return None
        claims = token.claims if isinstance(token.claims, dict) else {}
        tenant = self._tenant_from_claims(claims)
        principal = token.subject or claims.get("sub") or token.client_id
        if not isinstance(principal, str) or not principal:
            raise IdentityError("Authenticated token does not identify a principal")
        principal_type = claims.get(self.principal_type_claim)
        if not isinstance(principal_type, str) or not principal_type:
            principal_type = "user" if token.subject or claims.get("sub") else "service"
        roles = self._strings(claims.get(self.roles_claim))
        return ExecutionContext(
            owner_id=self._digest("tenant", tenant),
            transport="streamable-http",
            identity_source="oauth-access-token",
            tenant_id=tenant,
            principal_id=principal,
            principal_type=principal_type,
            roles=roles,
            scopes=tuple(token.scopes),
            authenticated=True,
        )

    def resolve(self, request_context: Any) -> ExecutionContext:
        request = getattr(request_context, "request", None)
        headers = getattr(request, "headers", None)
        if headers is None:
            return ExecutionContext(
                owner_id=self.local_owner_id,
                transport="stdio",
                identity_source="local-process",
                tenant_id="local",
                principal_id=self.local_owner_id,
                principal_type="process",
                roles=("admin",),
                scopes=(),
                authenticated=True,
            )

        oauth = self._from_oauth()
        if oauth is not None:
            return oauth

        if self.trust_proxy_headers:
            principal = headers.get(self.PRINCIPAL_HEADER)
            if principal:
                tenant = headers.get(self.TENANT_HEADER)
                if not tenant:
                    if not self.allow_missing_tenant:
                        raise IdentityError(
                            f"Authenticated proxy identity is missing {self.TENANT_HEADER}"
                        )
                    tenant = "default"
                principal_type = headers.get(self.PRINCIPAL_TYPE_HEADER) or "user"
                roles = self._strings(headers.get(self.ROLES_HEADER))
                return ExecutionContext(
                    owner_id=self._digest("tenant", tenant),
                    transport="streamable-http",
                    identity_source="trusted-proxy-header",
                    tenant_id=tenant,
                    principal_id=principal,
                    principal_type=principal_type,
                    roles=roles,
                    scopes=(),
                    authenticated=True,
                )
            if self.require_proxy_identity:
                raise IdentityError(
                    "Authenticated proxy identity is required; missing "
                    f"{self.PRINCIPAL_HEADER}"
                )

        if self.allow_anonymous_http:
            return ExecutionContext(
                owner_id=self._digest("tenant", "local-http"),
                transport="streamable-http",
                identity_source="anonymous-development",
                tenant_id="local-http",
                principal_id="anonymous",
                principal_type="development",
                roles=("admin",),
                scopes=(),
                authenticated=False,
            )

        raise IdentityError("Authenticated HTTP identity is required")
