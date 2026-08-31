# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes allow-listed side effects, journals intent before execution, and automatically invokes compensating actions in reverse order when a workflow fails or an authorized client requests rollback.

Version 0.5 adds the **Enterprise Action Registry** on top of MCP `2026-07-28`, enterprise identity, and PostgreSQL distributed durability. Every new side effect is now bound to an immutable action version, SHA-256 definition hash, semantic metadata, JSON Schemas, and a persisted recovery snapshot before the side effect begins.

## Why Semantic Saga

Agentic workflows routinely cross systems that do not share one ACID transaction: GitHub, cloud APIs, ticketing systems, payments, SaaS applications, internal services, and other MCP tools. Semantic Saga provides a deterministic coordination layer around those side effects.

Core guarantees:

- **Write-ahead intent:** a step is stored as `EXECUTING` before its forward side effect.
- **Immutable action contract:** every new step persists the exact action version, definition hash, and non-secret contract snapshot before execution.
- **Version-correct recovery:** HTTP compensation is reconstructed from the historical snapshot rather than the currently active definition.
- **Runtime drift protection:** built-in/runtime compensation requires an exact implementation version/hash match and fails closed if code changed.
- **Schema enforcement:** Draft 2020-12 input schemas run before side effects; output-schema failures retain the returned receipt and trigger rollback.
- **Secret references:** credentials can be resolved at execution time without putting secret values in action files, saga journals, or dry-run output.
- **Semantic action metadata:** operators can declare domain, operation, resource, reversibility, risk, effects, and compensation guarantees.
- **Automatic rollback:** failed workflows compensate eligible steps in reverse order.
- **Uncertain-outcome recovery:** if a process disappears after a request may have reached a remote service, the durable `EXECUTING` record remains recoverable.
- **Stable idempotency keys:** forward and compensation calls reuse deterministic keys. Downstream endpoints must honor them.
- **Transport-independent workflows:** `saga_id` is the durable workflow handle; state is not tied to one HTTP connection.
- **Tenant isolation:** authenticated HTTP sagas are scoped to the organization/tenant.
- **OAuth resource-server mode:** signed JWT access tokens can be validated against issuer, audience, JWKS, expiry, and allowed algorithms.
- **RBAC/scopes:** `viewer`, `operator`, `admin`, plus `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`.
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
        "--actions", "/absolute/path/action_registry.json"
      ]
    }
  }
}
```

A ready-to-customize Claude Desktop example is available in [`docs/claude_desktop_config.json`](docs/claude_desktop_config.json).

## Enterprise Action Registry

Actions are operator-owned, immutable contracts. The recommended `schema_version: 1` format is:

```json
{
  "schema_version": 1,
  "actions": [
    {
      "id": "create_repository",
      "version": "1.0.0",
      "kind": "http",
      "active": true,
      "semantic": {
        "domain": "source_control",
        "operation": "create",
        "resource": "repository",
        "reversibility": "full",
        "risk": "medium",
        "effects": {"creates": ["source_control.repository"]}
      },
      "input_schema": {
        "type": "object",
        "required": ["name"],
        "properties": {"name": {"type": "string", "minLength": 1}},
        "additionalProperties": false
      },
      "output_schema": {
        "type": "object",
        "required": ["id"],
        "properties": {"id": {"type": "string"}}
      },
      "forward": {
        "url": "https://scm.internal/repos",
        "method": "POST",
        "headers": {
          "Authorization": {
            "secret_ref": "env://SCM_SERVICE_TOKEN",
            "prefix": "Bearer "
          }
        },
        "body": {"name": "${input.name}"}
      },
      "compensation": {
        "url": "https://scm.internal/repos/delete",
        "method": "POST",
        "headers": {
          "Authorization": {
            "secret_ref": "env://SCM_SERVICE_TOKEN",
            "prefix": "Bearer "
          }
        },
        "body": {"id": "${result.id}"}
      }
    }
  ]
}
```

See [`examples/action_registry.json`](examples/action_registry.json), [`docs/action_registry.schema.json`](docs/action_registry.schema.json), and [`docs/action_registry.md`](docs/action_registry.md).

### Immutable versions and recovery

When `execute_saga_step` starts, Semantic Saga validates the input and persists:

```text
action
action_version
action_definition_hash
action_definition
```

Only then is the forward side effect invoked.

If `create_repository@1.0.0` later becomes `2.0.0`, an old step created under `1.0.0` still compensates with the persisted `1.0.0` HTTP contract. The engine verifies the stored SHA-256 hash before using it. A tampered or inconsistent snapshot fails visibly instead of falling back to the current action.

Runtime/built-in actions cannot be reconstructed from JSON alone. Their persisted definition includes an implementation identity/hash, and recovery requires the currently registered implementation to match exactly. When changing a built-in action, its action version must be bumped.

### Input and output schemas

Input JSON Schema validation happens before a step is created, so invalid agent input causes no external side effect and leaves the saga active.

Output validation happens after the remote action returns. Because the side effect may already exist, an invalid output is treated as a workflow failure: the returned receipt is retained in the journal and compensation is attempted with it.

### Secret references

Header values can reference credentials without embedding them:

```json
{
  "Authorization": {
    "secret_ref": "env://PAYMENTS_TOKEN",
    "prefix": "Bearer "
  }
}
```

The built-in provider supports `env://NAME`. Only the reference is part of the action snapshot; the secret value is resolved at request time and is never stored in the saga journal. Dry-run previews redact secret-referenced headers without resolving or printing the credential.

The Python `SecretProvider` protocol can be implemented for Vault, AWS Secrets Manager, Azure Key Vault, GCP Secret Manager, or another organization-owned service.

### Upgrade behavior for pre-0.5 sagas

Steps created before 0.5 have no immutable action snapshot. By default, Semantic Saga refuses to compensate such rows because it cannot prove that the currently configured definition matches the historical one.

Preferred migration: recover pending old sagas using the pre-upgrade release before moving to 0.5. After independent verification, an operator can explicitly enable the compatibility escape hatch:

```bash
semantic-saga-mcp --allow-legacy-action-recovery
```

or set `SAGA_ALLOW_LEGACY_ACTION_RECOVERY=true`.

Legacy `actions.json` maps are still accepted for configuration compatibility; newly executed legacy-map actions are journaled as immutable `legacy-v1` contracts.

## Storage choices

### In-memory

Use the default `SagaStore` for tests and ephemeral development. State disappears when the process exits.

### SQLite — durable single node

```bash
semantic-saga-mcp --database ./semantic-saga.db
```

SQLite uses WAL mode and supports crash recovery. It applies the same lease/fencing coordinator contract locally, but remains intended for one Semantic Saga deployment rather than horizontally scaled production. Existing SQLite databases are migrated in place with nullable action version/hash/snapshot columns.

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

`--database` and `--postgres-dsn` are mutually exclusive. PostgreSQL is the recommended store when several Semantic Saga replicas share one journal. Phase 4 adds first-class persisted action version/hash/snapshot fields without changing the Phase 3 lease/fencing model.

Environment equivalents include `SAGA_POSTGRES_DSN`, `SAGA_POSTGRES_POOL_MIN`, `SAGA_POSTGRES_POOL_MAX`, `SAGA_WORKER_ID`, `SAGA_LEASE_SECONDS`, and `SAGA_RECOVERY_LIMIT`.

See [`docs/distributed_durability.md`](docs/distributed_durability.md).

## How distributed safety works

A mutating operation first acquires a renewable saga lease. Lease acquisition increments a monotonically increasing fencing token. The coordinator writes the step intent—including its immutable action contract—with that token before calling the external system and keeps the lease alive while the call is in flight.

If a worker loses its lease, a newer owner receives a larger fencing token. The stale worker can no longer update the saga or step journal. If its external request may already have succeeded, the step intentionally remains `EXECUTING`; a later recovery owner treats the outcome as uncertain and compensates it using the same stable idempotency key and the persisted action contract.

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
  --postgres-dsn "$SAGA_POSTGRES_DSN" \
  --actions ./examples/action_registry.json
```

By default tenant identity is read from `tenant_id`, then `tid`, then `org_id`; roles are read from `roles`. Claim names are configurable. Authenticated callers without a tenant are rejected unless an operator explicitly enables the missing-tenant compatibility option.

Remote saga ownership is tenant-scoped. A user and a service principal from the same tenant can continue the same explicit saga; another tenant sees the saga as not found. `tenant_id` and `creator_principal_id` are persisted as first-class saga columns in addition to reserved audit metadata.

See [`docs/enterprise_identity.md`](docs/enterprise_identity.md).

### Authorization

| Identity | Read saga/registry | Begin/execute | Commit/rollback |
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

## Templates and dry run

Supported template roots in HTTP action requests are `input`, `result`, `saga`, and `step`.

`--dry-run` (or `SAGA_DRY_RUN=true`) renders forward/compensation requests without sending them. Sensitive headers and secret references are redacted.

## Built-in file transaction

`create_text_file@1.0.0` is always registered as a runtime action. It creates a relative `.txt` file under `./saga-files` (configurable with `--file-root`) and compensates by deleting only the file created by that step. Existing files are never overwritten.

Run the local demo:

```bash
python examples/file_transaction_demo.py
```

## MCP tools

| Tool | Purpose |
| --- | --- |
| `list_actions` | List active action ids, versions, hashes, schemas, and semantic metadata. |
| `get_action` | Inspect one action contract by id and optional version. |
| `begin_saga` | Create an `ACTIVE` saga and return its durable ID. |
| `execute_saga_step` | Validate input, snapshot the active action contract, journal intent, and execute the side effect. |
| `commit_saga` | Finalize a successful saga. |
| `rollback_saga` | Compensate eligible steps in reverse order under their persisted contracts. |
| `trigger_rollback` | Immediately initiate compensation after a client-detected error. |
| `get_saga` | Inspect durable saga and step state, including action versions/hashes. |

A typical agent flow is:

1. `list_actions` / `get_action` to understand available operations and risk;
2. `begin_saga`;
3. one or more `execute_saga_step` calls;
4. `commit_saga` when the complete workflow is accepted; or
5. `rollback_saga` / `trigger_rollback` when the workflow must unwind.

Server-side action failures automatically attempt rollback.

## Development and CI

```bash
python -m unittest discover -s tests -v
```

Pull requests validate:

- Python 3.10–3.13 unit tests;
- immutable action snapshots, version-correct historical recovery, tamper detection, schema failures, and secret non-persistence;
- MCP registry discovery and authorization;
- MCP 2026 Streamable HTTP continuation;
- OAuth tenant/RBAC integration;
- signed JWT verification;
- package build/Twine validation; and
- PostgreSQL 16 concurrency, lease/fencing, recovery-claim, action-contract persistence, and server-startup tests.

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
- [`docs/phase4_summary.md`](docs/phase4_summary.md) — immutable enterprise action registry, schemas, semantics, and secret references.
