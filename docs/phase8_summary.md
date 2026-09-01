# Phase 8 summary — A2A interoperability

Phase 8 adds A2A Protocol 1.0 as a second enterprise ingress into the same governed Semantic Saga engine.

## What changed

- Added optional `semantic-saga-mcp[a2a]` dependencies based on the official A2A Python SDK.
- Added `semantic-saga-a2a` CLI.
- Added public A2A Agent Card discovery at `/.well-known/agent-card.json`.
- Added A2A 1.0 JSON-RPC endpoint, default `/a2a`.
- Added strict structured A2A command envelope using exactly one `application/json` `Part.data` object.
- Reused Phase 2 bearer token verification, tenant claims, principal types, roles, and scopes.
- Reused the existing RBAC mapping rather than creating A2A-specific privileges.
- Reused Phase 7 `GovernedCoordinator`, JSON/OPA policies, risk rules, budgets, and approvals.
- Reused Phase 3 saga leases/fencing and Phase 4 immutable action contracts.
- Reused Phase 5 DAG/recovery/compensation behavior.
- Reused Phase 6 tracing and hash-chained audit evidence.
- Added tenant-scoped durable A2A task storage using the official SQL-backed task store.
- SQLite A2A tasks use `a2a_tasks` in the same database file as Saga state.
- PostgreSQL A2A tasks use `a2a_tasks` in the same database as Saga state.
- A2A-created sagas record reserved `_interop` metadata with A2A protocol/task/context correlation.
- Package version advanced to `0.9.0`.

## Supported command operations

`begin`, `execute`, `plan`, `run`, `approve`, `retry`, `checkpoint`, `commit`, `rollback`, `get`, `timeline`, `audit`, `verify_audit`, `list_actions`, `get_action`, `policy_status`, and `policy_decisions`.

Each operation maps to an existing MCP tool permission and then enters the same governed coordinator method.

## Distributed task guarantee

A2A protocol task state is durable in SQLite/PostgreSQL. The explicit `saga_id` remains the transactional handle. This supports:

1. peer agent A creates a task and saga;
2. task and saga state are persisted;
3. peer agent B in the same tenant can retrieve/continue them;
4. server restarts do not lose SQL-backed protocol tasks;
5. another tenant cannot retrieve either the task or saga.

## Streaming stance

The Agent Card intentionally advertises `streaming: false` and `push_notifications: false` in Phase 8. Durable SQL task state is not equivalent to a distributed recoverable event queue. Polling/task retrieval is the supported horizontal-scale behavior until the event delivery path has equivalent durability guarantees.

## Safety stance

- no free-form natural-language side-effect execution on the A2A ingress;
- no A2A-specific shortcut around RBAC or tenant ownership;
- current governance policy is still evaluated before side effects;
- cancellation does not blindly interrupt uncertain external effects;
- explicit rollback uses the existing compensation safety path;
- outbound remote A2A agents are not yet modeled as action-registry entries because their task receipts and compensation contracts must be persisted/versioned first.

## Validation targets

Phase 8 CI adds:

- base-install A2A command/RBAC/tenant tests without requiring optional A2A dependencies;
- Agent Card 1.0 validation;
- official A2A client integration against JSON-RPC;
- bearer gate and public discovery;
- same-tenant peer handoff;
- viewer read / mutation denial;
- cross-tenant isolation;
- policy-driven approval and parallelism-budget enforcement through A2A;
- SQLite A2A task persistence across store/server restart;
- PostgreSQL A2A task persistence and tenant isolation;
- PostgreSQL-backed A2A server startup.

See [`a2a_interoperability.md`](a2a_interoperability.md) for deployment and command details.
