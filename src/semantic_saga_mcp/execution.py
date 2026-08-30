from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecutionContext:
    """Transport-independent ownership context for one MCP request.

    Phase 1 deliberately separates saga ownership from a transport connection.
    Authentication and authorization remain deployment concerns until the
    enterprise identity layer lands in Phase 2.
    """

    owner_id: str
    transport: str
    identity_source: str
    tenant_id: str | None = None
    principal_id: str | None = None


class ExecutionContextResolver:
    """Resolve a stable saga ownership scope without persisting credentials.

    HTTP authorization values are only hashed to create a stable ownership
    scope. This class does *not* authenticate or validate a credential. Remote
    deployments must still authenticate at a trusted gateway until Phase 2 adds
    native OAuth/OIDC support.

    Proxy-provided tenant/principal headers are ignored unless explicitly
    enabled by the server operator.
    """

    TENANT_HEADER = "x-semantic-saga-tenant"
    PRINCIPAL_HEADER = "x-semantic-saga-principal"

    def __init__(self, *, trust_proxy_headers: bool = False, local_owner_id: str | None = None) -> None:
        self.trust_proxy_headers = trust_proxy_headers
        self.local_owner_id = local_owner_id or f"stdio:{uuid.uuid4()}"

    @staticmethod
    def _digest(namespace: str, value: str) -> str:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
        return f"{namespace}:{digest}"

    def resolve(self, request_context: Any) -> ExecutionContext:
        request = getattr(request_context, "request", None)
        headers = getattr(request, "headers", None)
        if headers is None:
            return ExecutionContext(
                owner_id=self.local_owner_id,
                transport="stdio",
                identity_source="local-process",
            )

        if self.trust_proxy_headers:
            principal = headers.get(self.PRINCIPAL_HEADER)
            if principal:
                tenant = headers.get(self.TENANT_HEADER)
                owner_id = self._digest("principal", f"{tenant or '-'}\0{principal}")
                return ExecutionContext(
                    owner_id=owner_id,
                    transport="streamable-http",
                    identity_source="trusted-proxy-header",
                    tenant_id=tenant,
                    principal_id=principal,
                )

        authorization = headers.get("authorization")
        if authorization:
            return ExecutionContext(
                owner_id=self._digest("authorization", authorization),
                transport="streamable-http",
                identity_source="authorization-digest",
            )

        return ExecutionContext(
            owner_id="http:anonymous",
            transport="streamable-http",
            identity_source="anonymous",
        )
