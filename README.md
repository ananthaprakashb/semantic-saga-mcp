# semantic-saga-mcp

A durable, governed Saga coordinator for agentic workflows, exposed through both [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) and A2A Protocol 1.0. Semantic Saga journals intent before side effects, executes only registered/versioned actions, and compensates completed or uncertain work when a workflow must unwind.

Version 0.9 adds **A2A agent interoperability** on top of MCP `2026-07-28`, enterprise identity, PostgreSQL distributed durability, the immutable Enterprise Action Registry, the Semantic Saga Engine, OpenTelemetry/audit, and tenant policy/governance. Peer agents can now discover Semantic Saga, delegate structured transactional work, persist A2A tasks, and continue an explicit saga across authorized agents in the same tenant without bypassing RBAC, policy, approvals, fencing, audit, or compensation semantics.

## Core guarantees

- **Write-ahead intent:** every concrete side effect is stored as `EXECUTING` before invocation.
- **Immutable action contracts:** each step persists the exact action version, SHA-256 definition hash, and non-secret contract snapshot.
- **Version-correct recovery:** historical compensation uses the historical snapshot, not the current action definition.
- **Stable idempotency:** forward retries reuse one persisted step id and therefore one idempotency identity.
- **Durable workflow DAGs:** nodes, dependencies, approvals, checkpoints, attempts, and recovery state survive restart.
- **Parallel ready nodes:** independent nodes execute concurrently under the current fenced saga lease.
- **Human approval + operator recovery:** policy/action gates are durable; uncertain work can enter `RECOVERY_REQUIRED`.
- **Tenant isolation + OAuth/RBAC:** remote workflows remain organization scoped and authorization controlled.
- **Tenant governance:** JSON or OPA policy can allow, deny, or require approval by action/resource/risk/principal/phase.
- **Hard budgets:** limits for steps, planned nodes, cumulative risk units, and requested parallelism are checked before ordinary rules.
- **Current-policy enforcement:** ready work is re-evaluated immediately before side effects.
- **Rollback safety:** business policy can stop new work but cannot make compensation unavailable after normal RBAC authorization.
- **Distributed safety:** renewable leases, fencing tokens, atomic sequencing, and PostgreSQL `SKIP LOCKED` recovery prevent stale ownership.
- **Distributed tracing:** W3C trace context flows through ingress, saga/workflow, policy, and downstream HTTP actions.
- **Durable audit evidence:** control-plane events and policy decisions are append-only through the application API and SHA-256 hash-chained per saga.
- **Payload-safe diagnostics:** policy/audit/timeline output excludes action input/result payloads, credential material, HTTP bodies, resolved secrets, and checkpoint payloads.
- **A2A task durability:** SQL-backed A2A tasks survive server restart and are tenant scoped separately from the transactional `saga_id`.
- **Protocol parity:** MCP and A2A enter the same `GovernedCoordinator`; neither protocol owns a second execution engine.

Semantic Saga coordinates systems that do not share an ACID transaction. A compensation can still fail; failures remain visible as `ROLLBACK_FAILED` for operator handling.

## Install

Python 3.10 or newer is required.

Base MCP server:

```bash
python -m pip install semantic-saga-mcp
semantic-saga-mcp
```

A2A interoperability is optional:

```bash
python -m pip install 'semantic-saga-mcp[a2a]'
semantic-saga-a2a
```

OpenTelemetry SDK/exporters are also optional:

```bash
python -m pip install 'semantic-saga-mcp[otel]'
```

For source development:

```bash
python -m pip install -e .
```

## MCP quick start

The default MCP transport is stdio, governance is disabled, and the default store is in-memory.

Durable local development:

```bash
semantic-saga-mcp \
  --database ./semantic-saga.db \
  --actions ./examples/action_registry.json \
  --policy-mode json \
  --policy-file ./examples/governance_policy.json
```

Horizontally scaled Streamable HTTP deployment:

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --allowed-host mcp.example.com \
  --postgres-dsn "$SAGA_POSTGRES_DSN" \
  --worker-id "$HOSTNAME" \
  --actions ./examples/action_registry.json \
  --policy-mode opa \
  --policy-opa-url http://opa.internal:8181
```

## A2A 1.0 interoperability

Phase 8 exposes Semantic Saga as an A2A Protocol 1.0 peer through the official Python SDK.

```bash
semantic-saga-a2a \
  --host 127.0.0.1 --port 8100 \
  --a2a-public-url http://127.0.0.1:8100 \
  --database ./semantic-saga.db \
  --actions ./examples/action_registry.json \
  --policy-mode json \
  --policy-file ./examples/governance_policy.json
```

Discovery:

```text
GET /.well-known/agent-card.json
```

Default A2A JSON-RPC endpoint:

```text
POST /a2a
```

The Agent Card advertises A2A `1.0`, JSON-RPC, `application/json` input/output, and skill groups for transactional orchestration, governed side effects, and transaction inspection.

### Structured commands only

Semantic Saga does not execute arbitrary natural-language instructions received from a peer. An A2A message must contain exactly one structured `Part.data` object.

```json
{
  "operation": "begin",
  "metadata": {"workflow": "employee-onboarding"}
}
```

```json
{
  "operation": "plan",
  "saga_id": "<saga-id>",
  "action": "create_repository",
  "input": {"name": "payments-service"}
}
```

```json
{
  "operation": "run",
  "saga_id": "<saga-id>",
  "max_parallel": 4
}
```

```json
{
  "operation": "rollback",
  "saga_id": "<saga-id>"
}
```

Supported operations are `begin`, `execute`, `plan`, `run`, `approve`, `retry`, `checkpoint`, `commit`, `rollback`, `get`, `timeline`, `audit`, `verify_audit`, `list_actions`, `get_action`, `policy_status`, and `policy_decisions`.

Each operation maps to its existing MCP authorization capability and then calls the same governed coordinator method.

### A2A identity and tasks

A2A bearer authentication reuses the Phase 2 JWT/static verifier and the same tenant/principal/role/scope claim conventions. The public Agent Card remains discoverable; task operations require authentication when auth is enabled.

With SQLite or PostgreSQL, the official A2A `DatabaseTaskStore` persists protocol tasks in an `a2a_tasks` table alongside Saga state. Task ownership is tenant scoped, so authorized peer agents in one organization can retrieve a task created by another peer, while another tenant cannot.

Keep the two durable handles distinct:

```text
A2A task id  = protocol-level handoff/status handle
saga_id      = transactional workflow/compensation handle
```

Phase 8 deliberately advertises `streaming: false` and `push_notifications: false`. SQL task persistence is durable, while live event queues are not yet a distributed recoverable delivery layer. Polling/task retrieval is the supported horizontal-scale behavior for this release.

A2A task cancellation is not used to blindly interrupt an uncertain real-world side effect. Use explicit saga rollback/compensation, or reconcile `RECOVERY_REQUIRED` work first.

See [`docs/a2a_interoperability.md`](docs/a2a_interoperability.md).

## Policy and governance

Authentication/RBAC and governance remain separate layers for both MCP and A2A:

```text
OAuth / authenticated peer identity
        │
        ▼
RBAC: may this principal use this capability?
        │
        ▼
Governance: may THIS action/resource/risk happen NOW?
        │
        ▼
Saga engine / side effect
```

### Built-in JSON policy

```bash
semantic-saga-mcp \
  --policy-mode json \
  --policy-file ./examples/governance_policy.json
```

Built-in evaluation order is deterministic:

1. hard budgets
2. tenant-specific rules in file order
3. wildcard rules in file order
4. approval threshold
5. effective default effect

Effects are `allow`, `deny`, and `require_approval`. Policy can match action ID, phase, semantic risk/domain/operation/resource, principal type, roles, and scopes.

Tenant budgets can bound `max_steps_per_saga`, `max_planned_nodes`, `max_risk_units`, and `max_parallel`.

See [`docs/governance_policy.schema.json`](docs/governance_policy.schema.json) and [`docs/policy_governance.md`](docs/policy_governance.md).

### OPA policy-as-code

```bash
export SAGA_OPA_TOKEN='optional-private-token'
semantic-saga-mcp \
  --policy-mode opa \
  --policy-opa-url http://opa.internal:8181 \
  --policy-opa-decision-path semantic_saga/decision \
  --policy-opa-token-env SAGA_OPA_TOKEN
```

Semantic Saga sends OPA only sanitized control-plane context. Action inputs/results and secrets are excluded. OPA errors, timeouts, invalid results, or undefined decisions fail closed for forward work.

Every evaluation creates a `POLICY_DECISION` event with decision ID, effect, reason, revision, backend, matched rules, phase/risk, and non-sensitive counters. Policy evidence shares the audit hash chain and trace correlation.

## Observability and audit

The base package includes the OpenTelemetry API. Export OTLP/HTTP traces and metrics with:

```bash
semantic-saga-mcp --otel-endpoint http://otel-collector:4318
```

MCP accepts W3C `traceparent`, `tracestate`, and `baggage` through request `_meta` with HTTP-header fallback. A2A attaches valid W3C HTTP trace headers before entering the same coordinator. Active context is injected into downstream HTTP actions.

Audit evidence includes control-plane identifiers/statuses, actor identity, action/version, policy/retry/approval metadata, timestamps, and trace/span correlation while intentionally excluding payloads/secrets.

Each saga has a SHA-256 event chain:

```text
previous_hash -> event_hash -> next previous_hash -> ...
```

`verify_audit_chain` recomputes the complete chain. This is tamper-evident, not a substitute for independently anchored/WORM evidence.

See [`docs/observability_audit.md`](docs/observability_audit.md).

## Semantic Saga Engine

The orchestration layer is opt-in. A typical governed workflow is:

1. begin a saga
2. plan version-pinned nodes
3. add dependencies
4. obtain required approvals
5. run ready nodes
6. add checkpoints when useful
7. commit after all planned nodes complete

Policy is evaluated at planning and again immediately before ready-node execution.

Durable node states include:

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

Action contracts can version retry/backoff, `failure_mode`, and `approval_required`. `failure_mode: "pause"` leaves a workflow in `RECOVERY_REQUIRED` for reconciliation rather than automatically guessing the external outcome.

See [`docs/workflow_engine.md`](docs/workflow_engine.md).

## Enterprise Action Registry

Actions are operator-owned immutable contracts with semantic metadata, JSON Schemas, execution policy, forward request, compensation request, and secret references.

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

Historical rollback uses the persisted contract/version/hash rather than a later active definition.

Phase 8 does **not** yet model an arbitrary remote A2A agent as `kind: a2a` in the action registry. Outbound A2A actions require persisted remote task receipts, remote-agent identity/version, idempotency linkage, terminal status, compensation artifacts, and a versioned compensation contract before they can meet the same safety standard as HTTP/runtime actions.

## Secret references

Do not store credential values in action manifests. Header values can use references such as:

```json
{
  "Authorization": {
    "secret_ref": "env://PAYMENTS_TOKEN",
    "prefix": "Bearer "
  }
}
```

The built-in provider supports `env://NAME`; the `SecretProvider` protocol can be implemented for enterprise secret managers.

## Storage

### In-memory

Use `SagaStore` for tests/ephemeral development. Saga state, audit evidence, governance decisions, and A2A protocol tasks disappear with the process.

### SQLite

```bash
semantic-saga-mcp --database ./semantic-saga.db
semantic-saga-a2a --database ./semantic-saga.db
```

SQLite uses WAL mode and is intended for a single-node deployment. Saga workflow state uses saga metadata/steps, audit/policy decisions use `audit_events`, and A2A protocol tasks use `a2a_tasks` when the A2A extra/server is enabled.

### PostgreSQL

```bash
semantic-saga-mcp --postgres-dsn "$SAGA_POSTGRES_DSN" --worker-id "$HOSTNAME"
semantic-saga-a2a --postgres-dsn "$SAGA_POSTGRES_DSN" --worker-id "$HOSTNAME-a2a"
```

PostgreSQL is recommended for multiple replicas. Saga leases/fencing protect workflow ownership; audit/policy events use advisory locking for per-saga hash-chain serialization; A2A tasks are persisted through SQLAlchemy/asyncpg in the same database using a separate table.

See [`docs/distributed_durability.md`](docs/distributed_durability.md).

## Enterprise identity

For remote deployment, Semantic Saga can validate OAuth/OIDC JWT access tokens using issuer, audience, JWKS, expiry, and allowed algorithms. Durable saga and A2A task ownership are tenant scoped.

| Identity | Read saga/registry/audit/policy | Orchestrate / execute | Commit / rollback |
| --- | --- | --- | --- |
| `viewer` | yes | no | no |
| `operator` | yes | yes | yes |
| `admin` | yes | yes | yes |

Equivalent scopes are `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`. Governance can further narrow the authorized operation for a specific tenant/resource/risk.

See [`docs/enterprise_identity.md`](docs/enterprise_identity.md).

## MCP tools

| Tool | Purpose |
| --- | --- |
| `list_actions` / `get_action` | Inspect immutable action contracts. |
| `get_policy_status` / `get_policy_decisions` | Inspect governance state/evidence. |
| `begin_saga` | Create an `ACTIVE` saga. |
| `execute_saga_step` | Immediate compatibility execution path. |
| `plan_saga_step` | Persist a version-pinned DAG node. |
| `run_ready_steps` | Re-evaluate governance and run ready dependency waves. |
| `approve_saga_step` | Approve/reject a gated node. |
| `retry_saga_step` | Reset failed/rejected/blocked work after current checks. |
| `checkpoint_saga` | Persist a named milestone. |
| `commit_saga` | Commit completed work. |
| `rollback_saga` / `trigger_rollback` | Execute compensation safety path. |
| `get_saga` | Inspect saga/workflow/recovery state. |
| `get_saga_timeline` | Inspect payload-safe timeline plus integrity. |
| `get_audit_events` / `verify_audit_chain` | Read/verify append-only audit evidence. |

## Built-in file transaction

`create_text_file@1.0.0` creates a relative `.txt` file below the configured file root and compensates only files created by that step. Existing files are never overwritten or deleted.

## Upgrade behavior

Steps created before 0.5 have no immutable action snapshot and are refused for compensation by default unless `--allow-legacy-action-recovery` is explicitly enabled.

Phase 6 introduced the audit table. Phase 7 governance decisions reuse it. Phase 8 adds no Saga schema migration; when SQL-backed A2A is enabled, the official A2A task store creates its separate `a2a_tasks` table.

Governance defaults to `--policy-mode none`, so upgrading does not silently enable business-policy restrictions. A2A dependencies are optional and do not affect an MCP-only installation unless the `[a2a]` extra is installed.

## Development and CI

```bash
python -m unittest discover -s tests -v
```

Pull requests validate Python 3.10–3.13, MCP 2026 Streamable HTTP, OAuth tenant/RBAC, governance approval/budgets, built-in JSON and OPA adapters, action-registry recovery, DAG/retry/approval/recovery behavior, audit integrity/payload safety, optional OpenTelemetry compatibility, A2A 1.0 discovery/client/task/tenant/restart behavior, package/Twine validation, and PostgreSQL 16 concurrency/fencing/recovery/audit/governance/A2A persistence and server startup.

## Publishing

The repository uses PyPI trusted publishing. Before release, update `__version__`, run CI/package validation, inspect distribution contents, and publish a GitHub release. PyPI versions are immutable.

## Phase notes

- [`docs/phase2_summary.md`](docs/phase2_summary.md) — OAuth identity, tenants, and RBAC.
- [`docs/phase3_summary.md`](docs/phase3_summary.md) — PostgreSQL distributed durability.
- [`docs/phase4_summary.md`](docs/phase4_summary.md) — immutable action registry, schemas, semantics, and secret references.
- [`docs/phase5_summary.md`](docs/phase5_summary.md) — DAG orchestration, retries, approvals, checkpoints, parallel execution, and operator recovery.
- [`docs/phase6_summary.md`](docs/phase6_summary.md) — OpenTelemetry traces/metrics, durable hash-chained audit, and execution timelines.
- [`docs/phase7_summary.md`](docs/phase7_summary.md) — tenant governance, budgets, risk-aware approvals, OPA integration, and policy evidence.
- [`docs/phase8_summary.md`](docs/phase8_summary.md) — A2A 1.0 discovery, governed peer delegation, and durable tenant-scoped protocol tasks.
