# Semantic Saga workflow engine

Version 0.6 adds an opt-in orchestration layer on top of the durable Saga coordinator. Existing clients may continue to call `execute_saga_step` directly. Organizations that need dependencies, approvals, parallel work, checkpoints, and operator recovery can plan a workflow DAG and then execute its ready nodes.

## Lifecycle

A planned node is persisted in the saga's reserved `_engine` metadata before any action is executed. Planning snapshots the same immutable action id, version, definition hash, and non-secret definition used by the Phase 4 action registry.

Typical flow:

1. `begin_saga`
2. call `plan_saga_step` for each node
3. reference earlier node ids in `depends_on`
4. call `approve_saga_step` for gated nodes
5. call `run_ready_steps`
6. optionally persist milestones with `checkpoint_saga`
7. `commit_saga` only after every planned node is `COMPLETED`

`run_ready_steps` repeatedly forms dependency waves. Independent `READY` nodes in one wave run concurrently under the same renewable saga lease and fencing token. A dependent node cannot become ready until every dependency is `COMPLETED`. A failed, rejected, or blocked dependency makes downstream work `BLOCKED`.

## Node states

- `WAITING_DEPENDENCY` — one or more dependencies have not completed.
- `WAITING_APPROVAL` — dependencies are ready but approval is still pending.
- `READY` — eligible for the next execution wave.
- `EXECUTING` — a durable action step exists and the side effect may be in flight.
- `COMPLETED` — forward action and output validation succeeded.
- `FAILED` — the configured forward attempts were exhausted.
- `REJECTED` — an approval decision rejected the node.
- `BLOCKED` — a dependency failed or was rejected.
- `COMPENSATED` / `COMPENSATION_FAILED` — mirrors rollback state after compensation.

## Execution policy

An action may include an immutable `execution_policy`:

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

The default policy remains backward compatible: one forward attempt, the coordinator's existing compensation retry count, automatic rollback, and no approval gate. Default policy is resolved at runtime and is not inserted into an action snapshot unless explicitly configured, so Phase 4 hashes remain valid.

Backoff jitter is deterministic and hash-derived. Forward retries reuse the same persisted step id, which also preserves the existing `Idempotency-Key`. Remote services must still honor idempotency for uncertain network outcomes.

## Failure modes

`failure_mode: "rollback"` keeps the original Saga behavior: an exhausted action failure marks the saga failed and triggers reverse compensation.

`failure_mode: "pause"` changes the operator contract. An exhausted planned node moves the saga to `RECOVERY_REQUIRED` without automatic compensation. The operator may reconcile the external system, then use `retry_saga_step` or explicitly roll the saga back.

If a worker restarts while a `pause` node is `EXECUTING`, the external outcome is treated as uncertain. Startup recovery marks the node failed with `uncertain_outcome: true` and the saga `RECOVERY_REQUIRED`. `retry_saga_step` refuses that node unless `force: true` is supplied after operator reconciliation. Using force does not prove safety; it is an explicit acknowledgment that the operator has reconciled the external state. The same persisted step id is reused so downstream idempotency remains stable.

## Approvals

Approval can be required by the action's execution policy or by the individual planned node. The modern MCP path records the authenticated principal id, decision time, and optional reason. Rejecting a node moves the saga to `RECOVERY_REQUIRED`; approving a rejected node again requires an explicit retry/reset.

## Checkpoints

`checkpoint_saga` appends a named durable checkpoint to the engine state. Checkpoints are audit/coordination markers; they do not themselves execute side effects or change dependency readiness.

## Parallelism and distributed safety

Parallelism occurs only among nodes owned by the same current saga lease. The process renews that lease while the wave is running. Every concrete action step is journaled as `EXECUTING` before its worker thread invokes the action. SQLite serializes journal writes locally; PostgreSQL uses the Phase 3 fencing contract and connection pool for concurrent writes.

This is deliberately not a distributed task queue. A different Semantic Saga replica cannot execute another node from the same saga concurrently because the saga lease is the safety boundary. Future phases can introduce finer-grained worker scheduling without weakening the current recovery invariant.
