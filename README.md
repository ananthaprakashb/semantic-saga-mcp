# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes allow-listed side effects, journals intent before execution, and automatically invokes compensating actions in reverse order when a workflow fails or an authorized client requests rollback.

Version 0.4 adds **distributed durability** on top of the MCP `2026-07-28` and enterprise-identity foundation: PostgreSQL storage, atomic step ordering, renewable saga leases, fencing tokens, and multi-instance recovery claiming.

## Why Semantic Saga

Agentic workflows routinely cross systems that do not share one ACID transaction: GitHub, cloud APIs, ticketing systems, payments, SaaS applications, internal services, and other MCP tools. Semantic Saga provides a deterministic coordination layer around those side effects.

Core guarantees:

- **Write-ahead intent:** a step is stored as `EXECUTING` before its forward side effect.
- **Automatic rollback:** failed workflows compensate eligible steps in reverse order.
- **Uncertain-outcome recovery:** if a process disappears after a request may have reached a remote service, the durable `EXECUTING` record remains recoverable.
- **Stable idempotency keys:** forward and compensation calls reuse deterministic keys. Downstream endpoints must honor them.
- **Transport-independent workflows:** `saga_id` is the durable workflow handle; state is not tied to one HTTP connection.
- **Tenant isolation:** authenticated HTTP sagas are scoped to the organization/tenant.
- **OAuth resource-server mode:** signed JWT access tokens can be validated against issuer, audience, JWKS, expiry, and allowed algorithms.
- **RBAC/scopes:** `viewer`, `operator`, `admin`, plus `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`.
- **Safe action surface:** agents choose only administrator-configured actions; they cannot provide arbitrary URLs or credentials.
- **Distributed mutation leases:** only the current saga lease owner may mutate an active/recovering saga.
- **Fencing tokens:** stale workers cannot write completion state after another worker takes ownership.
- **Atomic step order:** PostgreSQL allocates each saga's compensation sequence transactionally.
- **Single-owner recovery:** PostgreSQL uses `FOR UPDATE SKIP LOCKED` so multiple instances do not duplicate recovery work.

Semantic Saga coordinates independent systems; it cannot make them a single ACID database transaction. A compensation may fail, in which case the saga remains `ROLLBACK_FAILED` for later retry/operator handling.

## Quick start

Python 3.10 or newer is required.

```bash
python -m pip install -e .
semantic-saga-mcp
```

The default transport is local stdio and the default store is in-memory.

Example MCP client configuration:

```json
{
  "mcpServers": {
    "semantic-saga": {
      "command": "semantic-saga-mcp",
      "args": [
        "--database", "/absolute/path/sagas.db",
        "--actions", "/absolute/path/actions.json"
      ]
    }
  }
}
```

A ready-to-customize Claude Desktop example is available in [`docs/claude_desktop_config.json`](docs/claude_desktop_config.json).

## Storage choices

### In-memory

Use the default `SagaStore` for tests and ephemeral development. State disappears when the process exits.

### SQLite — durable single node

```bash
semantic-saga-mcp --database ./semantic-saga.db
```

SQLite uses WAL mode and supports crash recovery. Version 0.4 also applies the same lease/fencing coordinator contract locally, but SQLite remains intended for one Semantic Saga deployment rather than horizontally scaled production.

### PostgreSQL — distributed production

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --postgres-dsn 'postgresql://semantic_saga:secret@db.internal/semantic_saga' \
  --worker-id "$HOSTNAME" \
  --lease-seconds 30 \
  --postgres-pool-min 2 \
  --postgres-pool-max 20
```

`--database` and `--postgres-dsn` are mutually exclusive. PostgreSQL is the recommended store when several Semantic Saga replicas share one journal.

Environment equivalents include:

- `SAGA_POSTGRES_DSN`
- `SAGA_POSTGRES_POOL_MIN`
- `SAGA_POSTGRES_POOL_MAX`
- `SAGA_WORKER_ID`
- `SAGA_LEASE_SECONDS`
- `SAGA_RECOVERY_LIMIT`

If `SAGA_WORKER_ID` is not configured, a unique process identity is generated automatically.

See [`docs/distributed_durability.md`](docs/distributed_durability.md) for the lease, fencing, uncertain-outcome, and recovery model.

## How distributed safety works

A mutating operation first acquires a renewable saga lease. Lease acquisition increments a monotonically increasing fencing token. The coordinator writes the step intent with that token before calling the external system and keeps the lease alive while the call is in flight.

If a worker loses its lease, a newer owner receives a larger fencing token. The stale worker can no longer update the saga or step journal. If its external request may already have succeeded, the step intentionally remains `EXECUTING`; a later recovery owner treats the outcome as uncertain and compensates it using the same stable idempotency key.

Startup recovery claims eligible PostgreSQL sagas using row locking plus `SKIP LOCKED`. This lets many replicas perform recovery concurrently without all choosing the same saga.

## Enterprise identity

For organization-wide HTTP deployment, Semantic Saga acts as an OAuth resource server. Your IdP issues tokens; Semantic Saga validates them.

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --allowed-host mcp.example.com \
  --auth-mode jwt \
  --auth-issuer https://idp.example.com/ \
  --auth-resource-url https://mcp.example.com/mcp \
  --auth-audience https://mcp.example.com \
  --auth-jwks-url https://idp.example.com/.well-known/jwks.json \
  --auth-required-scope semantic-saga \
  --postgres-dsn "$SAGA_POSTGRES_DSN"
```

By default tenant identity is read from `tenant_id`, then `tid`, then `org_id`; roles are read from `roles`. Claim names are configurable. Authenticated callers without a tenant are rejected unless an operator explicitly enables the missing-tenant compatibility option.

Remote saga ownership is tenant-scoped. A user and a service principal from the same tenant can continue the same explicit saga; another tenant sees the saga as not found. `tenant_id` and `creator_principal_id` are persisted as first-class saga columns in addition to reserved audit metadata.

See [`docs/enterprise_identity.md`](docs/enterprise_identity.md).

### Authorization

| Identity | Read | Begin/execute | Commit/rollback |
| --- | --- | --- | --- |
| `viewer` | yes | no | no |
| `operator` | yes | yes | yes |
| `admin` | yes | yes | yes |

Equivalent machine scopes are `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`.

Trusted reverse-proxy identity headers remain available as a migration mode. Static bearer tokens are intended only for local demos and CI. Non-local unauthenticated HTTP requires the explicit private-network override and should not be internet-facing.

## Streamable HTTP

For local development without OAuth:

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 127.0.0.1 --port 8000 \
  --database ./semantic-saga.db
```

Clients connect to `http://127.0.0.1:8000/mcp`.

The older dedicated `GET /sse` plus `POST /messages` transport remains temporarily available with `--transport sse` for migration only. New deployments should use Streamable HTTP.

## Configure actions

Action definitions are controlled by the server operator. An HTTP action pairs a forward request with a compensating request:

```json
{
  "charge_card": {
    "forward": {
      "url": "https://payments.internal/charges",
      "method": "POST",
      "headers": {"Authorization": "Bearer configured-secret"},
      "body": {
        "amount": "${input.amount}",
        "account": "${input.account}"
      }
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

Supported template roots are `input`, `result`, `saga`, and `step`. Do not commit secrets in action files; supply protected runtime configuration or secret-injected files through your deployment platform.

### Dry run

`--dry-run` (or `SAGA_DRY_RUN=true`) renders forward/compensation requests without sending them. Sensitive authentication headers are redacted.

### Built-in file transaction

`create_text_file` is always available. It creates a relative `.txt` file under `./saga-files` (configurable with `--file-root`) and compensates by deleting only the file created by that step. Existing files are never overwritten.

Run the local demo:

```bash
python examples/file_transaction_demo.py
```

## MCP tools

| Tool | Purpose |
| --- | --- |
| `begin_saga` | Create an `ACTIVE` saga and return its durable ID. |
| `execute_saga_step` | Journal intent and execute a configured side effect. |
| `commit_saga` | Finalize a successful saga. |
| `rollback_saga` | Compensate eligible steps in reverse order. |
| `trigger_rollback` | Immediately initiate compensation after a client-detected error. |
| `get_saga` | Inspect durable saga and step state. |

A typical flow is:

1. `begin_saga`
2. one or more `execute_saga_step` calls
3. `commit_saga` when the complete workflow is accepted
4. `rollback_saga` or `trigger_rollback` when the workflow must unwind

Server-side action failures automatically attempt rollback.

## Development and CI

```bash
python -m unittest discover -s tests -v
```

Pull requests run:

- Python 3.10–3.13 unit tests;
- MCP 2026 Streamable HTTP continuation smoke tests;
- OAuth tenant/RBAC integration tests;
- signed JWT verification tests;
- package build/Twine validation; and
- PostgreSQL 16 concurrency, lease/fencing, recovery-claim, and server-startup tests.

The stdio transport writes only MCP protocol messages to stdout; diagnostics belong on stderr.

## Publishing

The repository includes a PyPI trusted-publishing workflow. Before releasing:

1. update `__version__`;
2. run tests and package validation;
3. confirm wheel/source distribution contents; and
4. publish a GitHub release after CI succeeds.

PyPI versions are immutable; increment the version for every release.

## Phase notes

- [`docs/phase2_summary.md`](docs/phase2_summary.md) — OAuth identity, tenants, and RBAC.
- [`docs/phase3_summary.md`](docs/phase3_summary.md) — PostgreSQL distributed durability.
