# Phase 3 — Distributed durability and PostgreSQL

Phase 3 moves Semantic Saga from a single-node durable coordinator to a horizontally scalable execution kernel.

## Added

- PostgreSQL-backed `PostgresSagaStore` with connection pooling.
- Atomic per-saga sequence allocation for deterministic reverse compensation order.
- Renewable saga mutation/recovery leases.
- Monotonic fencing tokens that reject stale-worker journal writes.
- Lease heartbeat while forward actions and compensations are in flight.
- `FOR UPDATE SKIP LOCKED` recovery claiming across multiple instances.
- First-class `tenant_id` and `creator_principal_id` saga columns.
- SQLite lease/fencing support so local behavior follows the same coordinator contract.
- PostgreSQL CLI/environment configuration and pool controls.
- PostgreSQL 16 CI covering concurrency, fencing, recovery ownership, identity persistence, and real server startup.

## Failure invariant

If a worker loses its lease after a side effect may have occurred, it does **not** write a completion result with a stale token. The durable step remains `EXECUTING`, which marks the outcome as uncertain. A later recovery owner can safely compensate it using the stable compensation idempotency key.

## Storage choices

- In-memory: unit tests and ephemeral development.
- SQLite: durable single-node/local execution.
- PostgreSQL: organization-wide, multi-instance execution.

See `docs/distributed_durability.md` for the operational model.
