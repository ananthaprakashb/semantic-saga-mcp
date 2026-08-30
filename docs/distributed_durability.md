# Distributed durability

Semantic Saga 0.4 adds a PostgreSQL execution store for horizontally scaled agentic workflows. SQLite remains the recommended local/single-node durable store; PostgreSQL is the production choice when several Semantic Saga instances share the same saga journal.

## Why leases and fencing are required

A durable journal alone does not make a saga coordinator safe to scale horizontally. Two workers can otherwise observe the same `ACTIVE` or recovery-eligible saga and both mutate it.

Semantic Saga therefore uses a per-saga renewable lease and a monotonically increasing fencing token:

1. a worker atomically acquires a saga lease;
2. acquisition increments the saga's fencing token;
3. step intent is written with the current token before a side effect starts;
4. a heartbeat renews the lease while the external action or compensation is in flight;
5. every journal mutation is checked against the current fencing token;
6. if a worker loses the lease, its stale token can no longer update saga or step state.

If a lease is lost after an external system may have applied a mutation, Semantic Saga deliberately leaves the step `EXECUTING`. The next recovery owner treats that step as uncertain and compensates it using the same stable idempotency key. This is safer than allowing a stale worker to mark an uncertain outcome as completed.

## Recovery claiming

PostgreSQL recovery uses `FOR UPDATE SKIP LOCKED`. Each startup worker can scan the same durable database, but a recovery-eligible saga is leased to only one worker at a time. Other workers skip rows already claimed by a concurrent recovery transaction.

The recoverable states remain:

- saga `FAILED`;
- saga `ROLLING_BACK`;
- saga `ROLLBACK_FAILED`; or
- saga `ACTIVE` with at least one `EXECUTING` step left by an interrupted/uncertain action.

## Atomic sequence allocation

Step order determines compensation order. PostgreSQL stores a `next_sequence` counter on each saga. Creating a step increments and returns that counter in the same database transaction as the step insert, preventing duplicate `(saga_id, sequence)` values under concurrency.

## First-class identity columns

Phase 2 stored creator identity in reserved audit metadata. Phase 3 additionally persists these fields as relational saga columns:

- `tenant_id`
- `creator_principal_id`

The existing hashed `session_id` remains the access/ownership key so cross-tenant lookups still behave as not found. First-class columns enable future tenant indexes, retention policy, audit export, and administrative reporting without parsing JSON metadata.

## Running with PostgreSQL

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --postgres-dsn 'postgresql://semantic_saga:secret@db.internal/semantic_saga' \
  --worker-id "$HOSTNAME" \
  --lease-seconds 30 \
  --postgres-pool-min 2 \
  --postgres-pool-max 20 \
  --auth-mode jwt \
  --auth-issuer https://idp.example.com/ \
  --auth-resource-url https://mcp.example.com/mcp \
  --auth-audience https://mcp.example.com \
  --auth-jwks-url https://idp.example.com/.well-known/jwks.json \
  --allowed-host mcp.example.com
```

Equivalent environment variables are available:

- `SAGA_POSTGRES_DSN`
- `SAGA_POSTGRES_POOL_MIN`
- `SAGA_POSTGRES_POOL_MAX`
- `SAGA_WORKER_ID`
- `SAGA_LEASE_SECONDS`
- `SAGA_RECOVERY_LIMIT`

`--database` and `--postgres-dsn` are mutually exclusive.

## Worker IDs

A worker ID is diagnostic identity for the current lease holder. If none is configured, Semantic Saga generates a unique process ID automatically. In Kubernetes or another orchestrator, a pod/instance identity such as `$HOSTNAME` is useful for operations and database inspection.

Correctness does not rely on worker IDs being globally unique: fencing tokens are the authoritative stale-writer protection, and active leases cannot be re-acquired until released or expired.

## Lease duration

The default lease is 30 seconds. The coordinator renews it while a forward action or compensation is running. A deployment should choose a duration long enough to tolerate normal scheduler/database jitter but short enough that a crashed worker can be recovered promptly.

The idempotency contract still applies: downstream forward and compensation endpoints must honor Semantic Saga's stable idempotency keys. Leases prevent coordinator split-brain; they cannot make independent external systems exactly-once.

## Database operations

The PostgreSQL adapter creates its required tables and indexes with idempotent DDL at startup. Production organizations should still manage database backups, encryption, credentials, network policy, connection limits, and schema-change rollout through their normal platform practices.

Phase 3 does not yet introduce an operator console or policy engine. Those layers can consume the durable tenant/principal fields and distributed execution primitives in later phases.
