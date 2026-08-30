# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes allow-listed side effects, journals their state, and automatically invokes compensating actions in reverse order when a step fails or an authorized MCP client requests rollback.

Version 0.3 builds an enterprise identity layer on the MCP `2026-07-28` stateless foundation: native OAuth bearer-token verification for Streamable HTTP, tenant-scoped saga ownership, user and service principals, RBAC, and cross-tenant isolation. The old dedicated HTTP+SSE transport remains available only as a deprecated migration path.

## Guarantees

- **Write-ahead intent:** a step is stored as `EXECUTING` before its forward request. After a process crash, such an uncertain step is eligible for compensation.
- **Automatic rollback:** a failed step changes the saga to failed and rolls back it and previously completed steps. The failed request is included because a network error may occur after a remote mutation.
- **Reverse-order compensation:** completed mutations unwind from newest to oldest.
- **Idempotency:** forward and compensation requests receive stable `Idempotency-Key` headers. Endpoints **must honor these keys** because networks cannot provide exactly-once delivery.
- **Pluggable storage:** the dependency-free in-memory store is the default. SQLite provides durable local recovery, and the storage protocol keeps the coordinator independent of SQLite, Redis, or PostgreSQL.
- **Transport-independent workflows:** `saga_id` is the durable workflow handle; remote continuation does not depend on a sticky MCP connection.
- **Tenant isolation:** authenticated HTTP sagas are owned by the organization/tenant. Authorized users and service principals in the same tenant can continue a saga; other tenants see it as not found.
- **Native OAuth resource-server mode:** production HTTP deployments can validate signed JWT access tokens using configured issuer, audience, JWKS, signing algorithms, and expiration checks.
- **RBAC and scopes:** `viewer`, `operator`, and `admin` roles plus `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin` scopes restrict tool execution.
- **Creator attribution:** the server records the creating principal under reserved `metadata._identity`; caller metadata cannot spoof that field.
- **Strict schema enforcement:** Pydantic strict models reject missing, mistyped, or unexpected tool arguments before coordinator code can run.
- **Safe action surface:** agents select administrator-configured actions; they cannot supply arbitrary URLs or credentials.
- **Fail-closed remote binding:** non-local Streamable HTTP requires an explicit Host allowlist and authenticated identity unless the operator deliberately enables the private-network escape hatch.

This is a coordination framework, not an ACID transaction spanning independent systems. A compensation can itself fail. That state is reported as `ROLLBACK_FAILED` for operator or client retry rather than being hidden.

## Quick start

Python 3.10 or newer is required.

```bash
python -m pip install -e .
semantic-saga-mcp
```

Example local MCP client configuration:

```json
{
  "mcpServers": {
    "semantic-saga": {
      "command": "semantic-saga-mcp",
      "args": ["--actions", "/absolute/path/actions.json", "--database", "/absolute/path/sagas.db"]
    }
  }
}
```

Environment variables `SAGA_ACTIONS_FILE` and `SAGA_DATABASE` are alternatives to CLI flags.

### Built-in file transaction

The server always registers a `create_text_file` action backed by `FileTransactionTool`. Invoke it through `execute_saga_step` with `input` containing a relative `.txt` `path` and string `content`. Files are confined to `./saga-files` by default; use `--file-root` or `SAGA_FILE_ROOT` to choose another root. Compensation deletes only that step's file, and existing files are never overwritten.

Run the complete local demonstration with:

```bash
python examples/file_transaction_demo.py
```

The demo creates `demo-1.txt`, `demo-2.txt`, and `demo-3.txt`, deliberately fails the fourth action, and prints the reverse-order deletion of the first three files as automatic rollback runs.

### Claude Desktop

A ready-to-customize Claude Desktop configuration is provided at [`docs/claude_desktop_config.json`](docs/claude_desktop_config.json). It uses the local stdio transport and can enable durable SQLite recovery without any remote authentication setup.

### Dry run

Pass `--dry-run` (or set `SAGA_DRY_RUN=true`) to validate a failure and rollback flow without making any HTTP requests. Each forward action is rendered and logged to stderr, then deliberately fails so the coordinator enters rollback. The expected compensation request is rendered and logged rather than sent. Sensitive authentication headers are redacted from previews.

### Storage and crash recovery

Without `--database`, the server uses `SagaStore`, a thread-safe in-memory adapter. This is convenient for development, but its journal disappears when the process exits. Enable the included durable SQLite adapter with `--database ./semantic-saga.db`. On startup, the server finds interrupted rollbacks and uncertain `EXECUTING` steps in durable storage and resumes compensation automatically.

Developers can plug in Redis or PostgreSQL without changing the coordinator by implementing `SagaStoreProtocol` from `semantic_saga_mcp.store`. Mutations in a durable implementation must be committed before returning; `create_step` must also allocate its per-saga sequence atomically.

## Remote Streamable HTTP

For local development, Streamable HTTP can run on loopback without OAuth:

```bash
semantic-saga-mcp --transport streamable-http \
  --host 127.0.0.1 --port 8000 \
  --actions ./examples/actions.json \
  --database ./semantic-saga.db
```

Clients connect to `http://127.0.0.1:8000/mcp`. The official MCP SDK serves the modern `2026-07-28` protocol. A completely new connection can continue the same durable saga using its explicit `saga_id`.

### Production OAuth/JWT deployment

For a public or organization-wide endpoint, Semantic Saga acts as an OAuth resource server. Your existing identity provider issues access tokens; Semantic Saga validates them and publishes MCP/RFC 9728 protected-resource metadata.

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --allowed-host mcp.example.com \
  --allowed-host 'mcp.example.com:*' \
  --allowed-origin https://agents.example.com \
  --auth-mode jwt \
  --auth-issuer https://idp.example.com/ \
  --auth-resource-url https://mcp.example.com/mcp \
  --auth-audience https://mcp.example.com \
  --auth-jwks-url https://idp.example.com/.well-known/jwks.json \
  --auth-required-scope semantic-saga \
  --actions ./examples/actions.json \
  --database ./semantic-saga.db
```

JWT verification checks the configured issuer exactly, the intended audience, token expiration, signature, and permitted signing algorithm. The public `--auth-resource-url` should be the MCP URL that clients use, not an internal container or proxy address.

By default, Semantic Saga looks for a tenant in `tenant_id`, then `tid`, then `org_id`; roles come from `roles`. Those claim names are configurable. Missing tenant identity is rejected by default rather than silently combining unrelated callers into one tenant.

See [`docs/enterprise_identity.md`](docs/enterprise_identity.md) for the complete identity model, service-principal examples, claim configuration, and deployment guidance.

### Authorization model

| Identity | Read saga | Begin / execute | Commit / rollback |
| --- | --- | --- | --- |
| `viewer` | yes | no | no |
| `operator` | yes | yes | yes |
| `admin` | yes | yes | yes |

OAuth scopes can grant equivalent machine permissions:

- `semantic-saga:read` — inspect saga state.
- `semantic-saga:execute` — read and mutate sagas.
- `semantic-saga:admin` — all current saga tools.

A separate base scope can also be required by the MCP authentication middleware with `--auth-required-scope`.

### User and service-principal handoff

Remote saga ownership is tenant-scoped rather than principal-scoped. For example:

```text
alice@acme          deployment-agent@acme          mallory@other
    │                         │                          │
    ├─ begin_saga()           │                          │
    │      saga-123           │                          │
    │                         ├─ get_saga(saga-123) ✓   │
    │                         └─ continue if authorized │
    │                                                    └─ get_saga(saga-123) → not found
```

The creating identity remains visible in `metadata._identity`, including its tenant, principal, principal type, roles, and identity source.

### Static auth for local demos and CI

`--auth-mode static` loads an operator-owned JSON mapping of bearer tokens to verified identity records. It exists only for tests and controlled demos; it is not a production credential database.

A safe placeholder file is provided at [`examples/static_tokens.example.json`](examples/static_tokens.example.json).

### Trusted reverse-proxy migration mode

Deployments that already authenticate at a gateway can continue to use `--trust-identity-headers` instead of native OAuth. This mode is mutually exclusive with `--auth-mode jwt|static`.

The proxy must strip caller-supplied identity headers, inject validated `X-Semantic-Saga-Tenant`, `X-Semantic-Saga-Principal`, optional `X-Semantic-Saga-Principal-Type`, and `X-Semantic-Saga-Roles`, and prevent clients from bypassing the proxy.

### Private-network escape hatch

`--allow-unauthenticated-http` (or `SAGA_ALLOW_UNAUTHENTICATED_HTTP=true`) bypasses the non-local identity requirement. It exists for controlled private-network migration only and should not be used for an internet-facing action server.

### Deprecated dedicated SSE transport

The previous dedicated SSE transport remains temporarily available for migration:

```bash
semantic-saga-mcp --transport sse --host 127.0.0.1 --port 8000
```

It uses `GET /sse` plus a session-specific `POST /messages?session_id=...` endpoint and retains the older `2025-06-18` behavior. New deployments should use Streamable HTTP.

## Configure actions

Action configuration is controlled by the server operator. Each action pairs one forward HTTP request with one rollback request:

```json
{
  "charge_card": {
    "forward": {
      "url": "https://payments.internal/charges",
      "method": "POST",
      "headers": {"Authorization": "Bearer configured-secret"},
      "body": {"amount": "${input.amount}", "account": "${input.account}"},
      "timeout_seconds": 15
    },
    "rollback": {
      "url": "https://payments.internal/refunds",
      "method": "POST",
      "headers": {"Authorization": "Bearer configured-secret"},
      "body": {"charge_id": "${result.charge_id}"}
    }
  }
}
```

An entire string may be a typed template value. Supported roots are `input`, `result`, `saga`, and `step`, for example `${input.amount}`, `${result.charge_id}`, `${saga.id}`, and `${step.id}`. Do not commit secrets in an action file; generate protected runtime configuration instead.

## MCP tools

| Tool | Purpose |
| --- | --- |
| `begin_saga` | Creates an `ACTIVE` saga and returns its ID. |
| `execute_saga_step` | Runs a configured action. A failure automatically starts rollback. |
| `commit_saga` | Finalizes a successful saga and prevents later rollback. |
| `rollback_saga` | Explicitly compensates eligible steps in reverse order. |
| `trigger_rollback` | Immediately compensates eligible steps after an error. |
| `get_saga` | Returns the durable saga and step journal. |

## MCP prompt

The `saga-coordinator` prompt tells an LLM to wrap multi-step infrastructure changes in the Saga Coordinator and to invoke `trigger_rollback` immediately after an error.

A typical client flow is:

1. Call `begin_saga` and retain `id`.
2. Call `execute_saga_step` for each mutation with that `saga_id`.
3. Call `commit_saga` only when the whole workflow is accepted.
4. Call `rollback_saga` on a client-side validation error or hallucination. Server-side action errors trigger rollback automatically.

## Development

```bash
python -m unittest discover -s tests -v
```

Pull requests run CI across Python 3.10–3.13, build and validate the package, exercise a two-client stateless Streamable HTTP continuation, and run an authenticated HTTP scenario that verifies 401 gating, protected-resource discovery, same-tenant handoff, viewer mutation denial, and cross-tenant isolation.

The stdio transport writes only MCP protocol messages to stdout. Keep application diagnostics on stderr so clients can parse the protocol stream.

## Publishing to PyPI

Package metadata, the MIT license, typed-package marker, console entry point, PR CI, and a trusted-publishing GitHub Actions workflow are included. Before publishing a release:

1. Update `__version__` in `src/semantic_saga_mcp/__init__.py` and commit it.
2. Run the tests and build validation locally:

   ```bash
   python -m pip install --upgrade build twine
   python -m unittest discover -s tests -v
   python -m build
   python -m twine check dist/*
   ```

3. Confirm both the wheel and source distribution contain `LICENSE`, `README.md`, and `semantic_saga_mcp/py.typed`.
4. Configure a PyPI Trusted Publisher for this repository with workflow `publish.yml` and environment `pypi`.
5. Publish a GitHub release. The workflow builds once, validates the distributions, and publishes that exact artifact to PyPI using OpenID Connect rather than a long-lived API token.

PyPI does not allow a released filename/version to be replaced. Increment `__version__` for every release, including corrections to a failed or incomplete publication.
