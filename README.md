# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes allow-listed side effects, journals intent before execution, and invokes compensating actions when work must unwind.

Version 0.7 adds **OpenTelemetry observability and tamper-evident audit** on top of the Phase 5 Semantic Saga Engine, MCP `2026-07-28`, enterprise identity, PostgreSQL distributed durability, and the immutable Enterprise Action Registry. Organizations can now correlate MCP requests with saga/action traces, export operational metrics, inspect a payload-safe execution timeline, and verify a durable per-saga audit hash chain.

## Core guarantees

- **Write-ahead intent:** every concrete side effect is stored as `EXECUTING` before invocation.
- **Immutable action contracts:** each step persists the exact action version, SHA-256 definition hash, and non-secret contract snapshot.
- **Version-correct recovery:** historical HTTP compensation is reconstructed from the historical snapshot, not the current action definition.
- **Stable idempotency:** forward retries reuse one persisted step id and therefore one `Idempotency-Key`.
- **Durable workflow DAGs:** planned nodes, dependencies, approvals, checkpoints, attempts, and recovery state survive restart.
- **Dependency-safe execution:** downstream nodes do not run until every dependency is `COMPLETED`.
- **Parallel ready nodes:** independent nodes in one dependency wave execute concurrently under the current fenced saga lease.
- **Policy-driven retries:** forward and compensation retry limits, exponential backoff, deterministic jitter, approval requirements, and failure mode can be versioned with an action.
- **Human approval gates:** approval decisions carry the authenticated principal, timestamp, and optional reason.
- **Operator recovery:** `failure_mode: "pause"` moves a saga to `RECOVERY_REQUIRED` rather than automatically rolling it back.
- **Uncertain-outcome protection:** a paused node found `EXECUTING` after worker restart requires reconciliation before force retry.
- **Schema enforcement:** Draft 2020-12 input validation occurs before side effects; invalid outputs retain the returned receipt for compensation.
- **Secret references:** credential values are resolved at execution time and are not persisted in action snapshots or dry-run output.
- **Tenant isolation + OAuth/RBAC:** remote MCP workflows remain organization scoped and authorization controlled.
- **Distributed safety:** renewable saga leases, fencing tokens, atomic step sequencing, and PostgreSQL `SKIP LOCKED` recovery prevent stale or duplicate workers from owning the same saga.
- **Distributed tracing:** MCP SEP-414/W3C trace context is correlated through MCP, saga/workflow, and downstream HTTP action spans.
- **Low-cardinality metrics:** action/compensation attempts and durations, approval decisions, recovery operations, and saga lifecycle operations can be exported with OpenTelemetry.
- **Durable audit evidence:** control-plane events are append-only through the application API and hash-chained per saga in memory, SQLite, or PostgreSQL.
- **Payload-safe diagnostics:** audit/timeline output omits action input/result payloads, credential material, HTTP bodies, resolved secrets, and checkpoint payloads.

Semantic Saga coordinates systems that do not share an ACID transaction. A compensation can still fail; those failures are surfaced as `ROLLBACK_FAILED` for later operator handling.

## Quick start

Python 3.10 or newer is required.

```bash
python -m pip install -e .
semantic-saga-mcp
```

The default transport is stdio and the default store is in-memory. For durable local development:

```bash
semantic-saga-mcp \
  --database ./semantic-saga.db \
  --actions ./examples/action_registry.json
```

For horizontally scaled production use PostgreSQL and Streamable HTTP:

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --allowed-host mcp.example.com \
  --postgres-dsn "$SAGA_POSTGRES_DSN" \
  --worker-id "$HOSTNAME" \
  --actions ./examples/action_registry.json
```

See [`docs/enterprise_identity.md`](docs/enterprise_identity.md) for OAuth/OIDC deployment, [`docs/distributed_durability.md`](docs/distributed_durability.md) for PostgreSQL leases/fencing, and [`docs/observability_audit.md`](docs/observability_audit.md) for tracing, metrics, and audit.

## Observability and audit

The base package includes the OpenTelemetry API and works without a collector. To export OTLP/HTTP traces and metrics, install the optional observability extra:

```bash
python -m pip install 'semantic-saga-mcp[otel]'
```

Then configure an OpenTelemetry Collector base endpoint:

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --otel-endpoint http://otel-collector:4318
```

Environment equivalents include:

```text
SAGA_OTEL_ENDPOINT
SAGA_OTEL_HEADERS
SAGA_OTEL_SERVICE_NAME
OTEL_EXPORTER_OTLP_ENDPOINT
OTEL_EXPORTER_OTLP_HEADERS
```

Semantic Saga accepts W3C `traceparent`, `tracestate`, and `baggage` through MCP request `_meta` as standardized by MCP SEP-414. Streamable HTTP headers are a fallback when the corresponding `_meta` value is absent. Active trace context is injected into downstream HTTP actions.

The durable audit journal is stored alongside saga state. Audit events include control-plane identifiers/statuses, actor identity, action/version, retry/approval metadata, timestamps, and trace/span correlation when available. They intentionally exclude action input/result objects and secret-bearing request data.

Each saga has a SHA-256 chain:

```text
previous_hash -> event_hash -> next previous_hash -> ...
```

`verify_audit_chain` recomputes the complete chain. This is **tamper-evident**, not a substitute for independently anchored/WORM evidence: a database administrator with unrestricted rewrite access could rewrite the data and recompute hashes.

See [`docs/observability_audit.md`](docs/observability_audit.md).

## Semantic Saga Engine

The orchestration layer is opt-in. Existing integrations may continue using `execute_saga_step` directly. For dependency-aware workflows:

1. `begin_saga`
2. `plan_saga_step` for each node
3. reference earlier node ids through `depends_on`
4. `approve_saga_step` where approval is required
5. `run_ready_steps`
6. optionally call `checkpoint_saga`
7. `commit_saga` after every planned node is `COMPLETED`

Example shape:

```text
          create-account
             /      \
            /        \
   github-access    slack-access
            \        /
             \      /
             aws-role
```

The first node runs, then `github-access` and `slack-access` can execute concurrently, and `aws-role` becomes ready only after both complete.

Planned nodes use these durable states:

```text
WAITING_DEPENDENCY
WAITING_APPROVAL
READY
EXECUTING
COMPLETED
FAILED
REJECTED
BLOCKED
COMPENSATED
COMPENSATION_FAILED
```

See [`docs/workflow_engine.md`](docs/workflow_engine.md).

## Retry, approval, and failure policy

An action can include an immutable execution policy:

```json
{
  "execution_policy": {
    "forward": {
      "max_attempts": 3,
      "initial_backoff_seconds": 0.5,
      "backoff_multiplier": 2,
      "max_backoff_seconds": 5,
      "jitter": 0.2
    },
    "compensation": {
      "max_attempts": 5,
      "initial_backoff_seconds": 1,
      "backoff_multiplier": 2,
      "max_backoff_seconds": 15,
      "jitter": 0.2
    },
    "failure_mode": "pause",
    "approval_required": true
  }
}
```

`failure_mode: "rollback"` preserves classic Saga behavior. `failure_mode: "pause"` leaves the workflow in `RECOVERY_REQUIRED` so an operator can reconcile external state, retry the planned node, or roll the saga back.

Default policy remains backward compatible: one forward attempt, the coordinator's existing compensation retry count, automatic rollback, and no approval. Default policy is not inserted into the immutable snapshot unless explicitly configured, preserving Phase 4 hashes for existing definitions.

## Enterprise Action Registry

Actions are operator-owned, immutable contracts. The recommended `schema_version: 1` format includes semantic metadata, JSON Schemas, execution policy, forward request, compensation request, and secret references.

See:

- [`examples/action_registry.json`](examples/action_registry.json)
- [`docs/action_registry.schema.json`](docs/action_registry.schema.json)
- [`docs/action_registry.md`](docs/action_registry.md)

Before a concrete side effect begins Semantic Saga persists:

```text
action
action_version
action_definition_hash
action_definition
```

If `create_repository@1.0.0` later becomes `2.0.0`, an old step still compensates with its persisted `1.0.0` contract. Runtime/built-in actions additionally require an exact implementation hash match.

## Secret references

Do not store credential values in action manifests. Header values can reference secret material:

```json
{
  "Authorization": {
    "secret_ref": "env://PAYMENTS_TOKEN",
    "prefix": "Bearer "
  }
}
```

The built-in provider supports `env://NAME`. The Python `SecretProvider` protocol can be implemented for Vault, AWS Secrets Manager, Azure Key Vault, GCP Secret Manager, or another organization-owned secret system.

## Storage

### In-memory

Use `SagaStore` for tests and ephemeral development. Audit rows are also in-memory and disappear with the process.

### SQLite

```bash
semantic-saga-mcp --database ./semantic-saga.db
```

SQLite uses WAL mode and is intended for one Semantic Saga deployment. Workflow DAG state is persisted inside the saga metadata JSON, concrete action executions use the durable steps table, and Phase 6 audit events use an `audit_events` table in the same database.

### PostgreSQL

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

PostgreSQL is recommended when several Semantic Saga replicas share one journal. The saga lease remains the ownership boundary: one replica owns a saga, and that owner may execute several independent ready nodes concurrently using the existing PostgreSQL connection pool and fencing token. Audit events use the same pool; PostgreSQL advisory locking serializes hash-chain appends per saga.

## Enterprise identity

For organization-wide HTTP deployment, Semantic Saga can validate OAuth/OIDC JWT access tokens using issuer, audience, JWKS, expiry, and allowed algorithms. Remote saga ownership is tenant scoped, so principals from one tenant cannot read or mutate another tenant's saga.

Roles/scopes:

| Identity | Read saga/registry/audit | Orchestrate / execute | Commit / rollback |
| --- | --- | --- | --- |
| `viewer` | yes | no | no |
| `operator` | yes | yes | yes |
| `admin` | yes | yes | yes |

Equivalent scopes are `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`.

## MCP tools

| Tool | Purpose |
| --- | --- |
| `list_actions` | List active action contracts, schemas, semantic metadata, and execution policy. |
| `get_action` | Inspect one action id/version. |
| `begin_saga` | Create an `ACTIVE` saga. |
| `execute_saga_step` | Immediate compatibility path for one action. |
| `plan_saga_step` | Persist a version-pinned workflow node with dependencies/approval. |
| `run_ready_steps` | Execute ready dependency waves with bounded parallelism. |
| `approve_saga_step` | Approve or reject a gated node. |
| `retry_saga_step` | Reset a failed/rejected/blocked node; uncertain outcomes require `force`. |
| `checkpoint_saga` | Persist a named workflow milestone. |
| `commit_saga` | Commit only after all planned nodes complete. |
| `rollback_saga` | Compensate eligible concrete steps in reverse order. |
| `trigger_rollback` | Immediately start compensation after a client-detected failure. |
| `get_saga` | Inspect full saga, workflow DAG, approvals, checkpoints, steps, and recovery state. |
| `get_saga_timeline` | Inspect a payload-safe execution timeline plus audit integrity. |
| `get_audit_events` | Read ordered append-only audit evidence, optionally filtered by event type. |
| `verify_audit_chain` | Recompute the complete per-saga SHA-256 hash chain. |

## Built-in file transaction

`create_text_file@1.0.0` creates a relative `.txt` file below the configured file root and compensates only files created by that step. Existing files are never overwritten or deleted.

## Upgrade behavior

Steps created before 0.5 have no immutable action snapshot and are refused for compensation by default. Recover pending old sagas before upgrading when possible. The explicit migration escape hatch remains:

```bash
semantic-saga-mcp --allow-legacy-action-recovery
```

Legacy action-map JSON remains readable; newly executed legacy actions are journaled as immutable `legacy-v1` contracts.

Phase 6 creates the audit table automatically when an instrumented server starts. Existing saga and step rows require no rewrite.

## Development and CI

```bash
python -m unittest discover -s tests -v
```

Pull requests validate Python 3.10–3.13, MCP 2026 Streamable HTTP, OAuth tenant/RBAC and JWT behavior, package/Twine checks, action-registry recovery invariants, DAG/retry/approval/recovery behavior, audit integrity/payload safety, an optional OpenTelemetry SDK/exporter stack, W3C trace propagation, and PostgreSQL 16 concurrency/fencing/recovery/audit/server-startup guarantees.

## Publishing

The repository uses PyPI trusted publishing. Before release, update `__version__`, run CI/package validation, inspect distribution contents, and publish a GitHub release. PyPI versions are immutable.

## Phase notes

- [`docs/phase2_summary.md`](docs/phase2_summary.md) — OAuth identity, tenants, and RBAC.
- [`docs/phase3_summary.md`](docs/phase3_summary.md) — PostgreSQL distributed durability.
- [`docs/phase4_summary.md`](docs/phase4_summary.md) — immutable enterprise action registry, schemas, semantics, and secret references.
- [`docs/phase5_summary.md`](docs/phase5_summary.md) — DAG orchestration, retries, approvals, checkpoints, parallel execution, and operator recovery.
- [`docs/phase6_summary.md`](docs/phase6_summary.md) — OpenTelemetry traces/metrics, durable hash-chained audit, and execution timelines.
