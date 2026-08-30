# Phase 2 — Enterprise identity and RBAC

This phase upgrades Semantic Saga from transport-level ownership to an organization-aware identity and authorization model.

Implemented capabilities:

- OAuth 2.1 resource-server integration through the MCP Python SDK.
- Signed JWT access-token verification against configured JWKS, issuer, audience, and expiration.
- RFC 9728 protected-resource discovery through the MCP SDK.
- Tenant-scoped saga ownership so authorized users and agents in one organization can hand off the same saga.
- User and service-principal identity.
- `viewer`, `operator`, and `admin` RBAC.
- Scope-based machine authorization with `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`.
- Cross-tenant lookup isolation.
- Reserved creator identity metadata under `metadata._identity`.
- Configurable tenant, roles, and principal-type claims.
- Static bearer-token verifier only for local/demo/CI use.
- Phase 1 trusted reverse-proxy headers retained as a mutually exclusive migration mode.
- Loopback unauthenticated HTTP retained for local development; non-local HTTP remains fail-closed.
- End-to-end CI coverage for bearer rejection, protected-resource metadata, same-tenant viewer handoff, RBAC denial, and cross-tenant denial.

See `docs/enterprise_identity.md` for deployment and claim-model details.
