# Phase 5 summary — Semantic Saga Engine

Phase 5 turns the coordinator into an opt-in durable workflow engine while retaining the direct Saga API.

## Added

- durable DAG nodes with explicit dependencies
- dependency-wave scheduling
- concurrent execution of independent ready nodes under one renewable fenced saga lease
- version-pinned action snapshots at planning time
- deterministic exponential backoff and jitter
- forward retries that reuse one persisted step/idempotency identity
- versioned compensation retry policies
- human approval gates with principal/reason/timestamp audit data
- durable named checkpoints
- `rollback` and `pause` failure modes
- `RECOVERY_REQUIRED` for operator intervention
- explicit uncertain-outcome handling after worker restart
- operator retry with `force` required after uncertain external outcomes
- commit protection while planned workflow nodes are incomplete

## New MCP tools

- `plan_saga_step`
- `run_ready_steps`
- `approve_saga_step`
- `retry_saga_step`
- `checkpoint_saga`

Existing tools remain available and backward compatible.

## Persistence model

The workflow plan and checkpoint journal are stored in the reserved saga metadata key `_engine`. Concrete side effects continue to use the existing durable `steps` journal. Before each planned node invokes a side effect, a normal `EXECUTING` step is written with the immutable Phase 4 action snapshot, version, and hash. This avoids a database migration while preserving SQLite/PostgreSQL durability and recovery behavior.

## Safety model

The saga lease remains the mutation boundary. One replica owns a saga at a time; that owner may run several independent node actions concurrently. Fencing tokens prevent a stale owner from writing completion state after lease takeover.

A `pause` node found `EXECUTING` after restart is not automatically retried. It becomes an uncertain `FAILED` node and the saga enters `RECOVERY_REQUIRED`. The operator must reconcile external state before forcing the node back to `READY`, or choose rollback.

## Compatibility

Actions without an explicit `execution_policy` retain their Phase 4 snapshot/hash shape. Default retry/approval/failure behavior is computed at runtime so existing persisted action hashes remain recoverable.
