# A2A interoperability

Phase 8 exposes Semantic Saga as an A2A Protocol 1.0 peer while preserving the transaction, identity, policy, durability, and audit guarantees of the MCP ingress.

## Why A2A and MCP coexist

MCP remains the tool/data interface used by agent hosts to invoke Semantic Saga capabilities. A2A adds agent-to-agent discovery and durable task handoff. Both protocols enter the same `GovernedCoordinator` and therefore share:

- tenant ownership
- OAuth/RBAC authorization
- action registry versions and hashes
- JSON/OPA governance policy
- approval gates and budgets
- saga leases and fencing
- PostgreSQL/SQLite durability
- compensation and recovery
- OpenTelemetry correlation
- hash-chained audit evidence

The protocol is not the transaction boundary. The explicit `saga_id` is.

## Install and run

A2A dependencies are optional so existing MCP installations remain lightweight:

```bash
python -m pip install 'semantic-saga-mcp[a2a]'
```

Start the A2A server with SQLite:

```bash
semantic-saga-a2a \
  --host 127.0.0.1 --port 8100 \
  --a2a-public-url http://127.0.0.1:8100 \
  --database ./semantic-saga.db \
  --actions ./examples/action_registry.json
```

For horizontally scaled deployments use the same PostgreSQL database used by Semantic Saga workers:

```bash
semantic-saga-a2a \
  --host 0.0.0.0 --port 8100 \
  --a2a-public-url https://agents.example.com \
  --postgres-dsn "$SAGA_POSTGRES_DSN" \
  --auth-mode jwt \
  --auth-issuer https://id.example.com/ \
  --auth-audience semantic-saga \
  --auth-jwks-url https://id.example.com/.well-known/jwks.json \
  --policy-mode opa \
  --policy-opa-url http://opa.internal:8181
```

The public Agent Card is served from:

```text
/.well-known/agent-card.json
```

The default JSON-RPC endpoint is:

```text
/a2a
```

The RPC path can be changed with `--a2a-rpc-path`.

## Agent Card

Phase 8 advertises A2A protocol version `1.0` with JSON-RPC binding and these skill groups:

- `transactional-orchestration`
- `governed-side-effects`
- `transaction-inspection`

Input and output modes are `application/json`.

Phase 8 deliberately advertises:

```text
streaming: false
push_notifications: false
```

A2A tasks are durable in SQLite/PostgreSQL, but live event queues are not a cross-replica durability mechanism. Polling/retrieval of the durable task is the supported distributed guarantee in this phase. Streaming can be enabled later when the event delivery layer itself is distributed and recoverable.

## Structured command envelope

Semantic Saga does not execute arbitrary natural-language instructions received from an A2A peer. A request must contain exactly one structured `Part.data` object.

Examples:

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
  "input": {"name": "payments-service"},
  "depends_on": []
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

Supported operations map directly to the existing MCP authorization surface:

| A2A operation | Existing governed capability |
| --- | --- |
| `begin` | begin saga |
| `execute` | immediate action execution |
| `plan` | durable DAG node planning |
| `run` | ready-node execution |
| `approve` | approval decision |
| `retry` | operator retry |
| `checkpoint` | durable checkpoint |
| `commit` | commit saga |
| `rollback` | compensation safety path |
| `get` | inspect saga |
| `timeline` | payload-safe timeline |
| `audit` / `verify_audit` | audit evidence/integrity |
| `list_actions` / `get_action` | action registry inspection |
| `policy_status` / `policy_decisions` | governance inspection |

The A2A adapter calls the same coordinator methods rather than duplicating Saga logic.

## Identity and tenant ownership

Bearer authentication reuses the Phase 2 token verifiers and claim conventions:

- tenant claim: `tenant_id`, `tid`, or `org_id` by default
- principal: subject/client identity
- principal type
- roles
- OAuth scopes

The A2A task store uses tenant ownership. Authorized peer agents in one tenant can continue or inspect a durable A2A task created by another peer in that tenant. A different tenant cannot retrieve it.

Saga ownership uses the same tenant-derived ownership key as MCP. Therefore an explicit saga created through A2A can be continued by another authorized A2A peer in the same organization, and the same persistence model remains compatible with MCP-side ownership.

The Agent Card remains public for discovery. Actual task interactions require bearer authentication when authentication is configured. Non-local unauthenticated binding is refused unless the operator explicitly enables the private-network development override.

## RBAC and governance

A2A does not introduce a second permission system. Every A2A operation maps to its equivalent MCP tool name and passes through the existing `AuthorizationPolicy`.

After RBAC, the `GovernedCoordinator` evaluates the current tenant business policy exactly as it does for MCP. This includes:

- allow / deny / require approval
- semantic risk
- action/domain/operation/resource rules
- hard step/node/risk/parallelism budgets
- policy re-evaluation before ready work runs
- approval governance
- fail-closed OPA behavior

Rollback remains the safety path after normal RBAC authorization.

## Durable A2A tasks

With no database configured, A2A protocol tasks are in memory.

With SQLite, the official A2A `DatabaseTaskStore` stores protocol tasks in the same database file as Saga state, using its own `a2a_tasks` table.

With PostgreSQL, A2A protocol tasks use the same PostgreSQL database and a separate `a2a_tasks` table through SQLAlchemy/asyncpg.

This separates two concepts intentionally:

```text
A2A task id  = protocol-level handoff/status handle
saga_id      = durable transactional workflow handle
```

Restarting an A2A server does not lose a SQL-backed task or its associated Saga state.

## Observability and audit

Incoming W3C `traceparent`, `tracestate`, and `baggage` headers are attached before entering the governed coordinator. Coordinator/action spans therefore remain children of the peer-agent trace when a valid context is supplied.

A2A-created sagas add reserved `_interop` metadata identifying A2A 1.0 plus task/context IDs. The caller cannot use this field to alter authorization or policy decisions.

Business operations continue to write the same Phase 6/7 hash-chained audit events. A2A command payloads are not copied into policy/audit evidence beyond the existing sanitized control-plane fields.

## Cancellation and compensation

A2A task cancellation is not advertised as a way to interrupt a real-world side effect. An external API may already have committed before cancellation arrives, making blind interruption unsafe.

Callers should use the explicit `rollback` command when compensation is desired. If a side effect has an uncertain outcome, the Phase 5 `RECOVERY_REQUIRED` workflow remains the correct reconciliation path.

## Scope intentionally deferred

Phase 8 makes Semantic Saga an A2A server/peer. It does not yet add `kind: a2a` to the Enterprise Action Registry for calling arbitrary remote agents as forward/compensation actions.

Before adding outbound A2A actions, Semantic Saga should persist the remote task receipt, remote Agent Card identity/version, idempotency relationship, terminal status, artifacts required for compensation, and a versioned compensation contract. Treating an opaque agent task like a stateless HTTP request would weaken the guarantees established in Phases 3–7.
