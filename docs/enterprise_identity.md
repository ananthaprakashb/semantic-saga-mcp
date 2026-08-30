# Enterprise identity and multi-tenancy

Semantic Saga 0.3 treats remote Streamable HTTP as an OAuth 2.1 resource server. The MCP process does not implement login, consent, users, passwords, or token issuance. An external authorization server / identity provider issues access tokens; Semantic Saga verifies those tokens and authorizes each tool call.

The implementation follows the MCP resource-server model and publishes RFC 9728 protected-resource metadata through the MCP Python SDK.

## Identity model

Every remote execution resolves to:

- `tenant_id` — organization boundary and durable saga ownership scope.
- `principal_id` — user or workload identity responsible for the request.
- `principal_type` — normally `user` or `service`.
- `roles` — Semantic Saga RBAC roles from the configured token claim.
- `scopes` — OAuth scopes on the access token.
- `identity_source` — currently `oauth-access-token`, `trusted-proxy-header`, `local-process`, or the explicit anonymous development mode.

HTTP sagas are owned by the tenant rather than by an MCP connection or one individual principal. This permits a human, agent, or service principal in the same organization to continue an explicit saga when RBAC allows it. A different tenant receives `Saga not found` for the same saga ID.

The identity that creates a saga is recorded under the reserved `metadata._identity` object. Caller-provided `_identity` metadata is overwritten by the server so it cannot spoof creator attribution.

## RBAC

Default roles are intentionally small:

| Role | Read saga | Begin / execute | Commit / rollback |
| --- | --- | --- | --- |
| `viewer` | yes | no | no |
| `operator` | yes | yes | yes |
| `admin` | yes | yes | yes |

Machine identities can use OAuth scopes instead of roles:

- `semantic-saga:read` — read saga state.
- `semantic-saga:execute` — read and mutate sagas.
- `semantic-saga:admin` — all current saga tools.

You may also configure one or more base scopes with `--auth-required-scope`. Those are enforced by the MCP SDK before a request reaches Semantic Saga RBAC.

## JWT access tokens

Production mode verifies signed JWT access tokens against an operator-supplied JWKS endpoint and validates the token issuer, audience, expiration, and signature.

Example:

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --allowed-host mcp.example.com \
  --allowed-host 'mcp.example.com:*' \
  --auth-mode jwt \
  --auth-issuer https://idp.example.com/ \
  --auth-resource-url https://mcp.example.com/mcp \
  --auth-audience https://mcp.example.com \
  --auth-jwks-url https://idp.example.com/.well-known/jwks.json \
  --auth-required-scope semantic-saga \
  --database ./semantic-saga.db
```

The public `--auth-resource-url` must describe the MCP resource as clients see it, not an internal container or reverse-proxy address.

### Claims

A JWT should identify:

- issuer (`iss`), audience (`aud`), and expiration (`exp`);
- principal through `sub`, `client_id`, `azp`, or `appid`;
- tenant through one of `tenant_id`, `tid`, or `org_id` by default;
- roles through `roles` by default.

Tenant claim fallbacks can be changed by repeating `--auth-tenant-claim`. The roles claim can be changed with `--auth-roles-claim`, and the principal-type claim with `--auth-principal-type-claim`.

A missing tenant is rejected by default because silently placing unrelated identities into one tenant defeats isolation. Single-tenant installations that cannot add a tenant claim can opt into `--auth-allow-missing-tenant`, which maps authenticated callers to the tenant `default`.

## Service principals

Client-credentials and workload identities are first-class callers. When a token does not have a user subject, Semantic Saga falls back to its OAuth client identity and classifies it as a service principal unless a configured principal-type claim says otherwise.

For example, an automation token can carry:

```json
{
  "aud": "https://mcp.example.com",
  "tenant_id": "acme",
  "client_id": "deployment-agent",
  "roles": ["operator"],
  "scope": "semantic-saga"
}
```

## Development static-token mode

`--auth-mode static` exists only for tests, demos, and local integration. The JSON file is operator-owned and maps bearer values to verified identity records:

```json
{
  "replace-this-development-token": {
    "client_id": "local-agent",
    "subject": "alice",
    "scopes": ["semantic-saga"],
    "claims": {
      "tenant_id": "acme",
      "roles": ["operator"]
    }
  }
}
```

Run it with the same `--auth-issuer`, `--auth-resource-url`, and optional `--auth-required-scope` settings used by native auth. Do not use this file as a production credential database or commit real bearer tokens.

## Trusted reverse-proxy migration mode

The Phase 1 trusted-header integration remains available for deployments that authenticate outside the MCP server. It is mutually exclusive with native OAuth mode.

The proxy must strip caller-supplied Semantic Saga identity headers and inject validated values:

- `X-Semantic-Saga-Tenant`
- `X-Semantic-Saga-Principal`
- `X-Semantic-Saga-Principal-Type` (optional)
- `X-Semantic-Saga-Roles`

Start Semantic Saga with `--trust-identity-headers`. The backend must not be directly reachable around that proxy.

## Local transports

`stdio` has no HTTP bearer-token layer. Its security boundary remains the process that launches the server, and the local process receives the `admin` role.

Unauthenticated Streamable HTTP is allowed automatically on loopback for development. Non-local HTTP continues to fail closed unless native OAuth, trusted proxy identity, or the explicit private-network `--allow-unauthenticated-http` escape hatch is configured.

## Security properties

- Raw bearer tokens are never used as saga ownership keys.
- Native OAuth identity comes only from a token that the MCP SDK's auth middleware accepted.
- Tenant ownership is hashed before it is stored in the existing ownership column.
- Cross-tenant saga lookup intentionally behaves as not found.
- Identity headers are ignored unless trusted-proxy mode is explicitly enabled.
- Caller metadata cannot override the server-generated `_identity` creator record.
- JWT issuer, audience, signature, and expiration are verified before Semantic Saga accepts the principal.

Phase 3 will normalize tenant/principal/audit fields into the distributed persistence model; Phase 2 keeps the existing store schema compatible while establishing the identity and authorization contract.
