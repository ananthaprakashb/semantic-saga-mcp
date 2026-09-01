# Observability and Audit

Semantic Saga 0.7 adds two complementary operational records:

1. **OpenTelemetry traces and metrics** for live distributed diagnostics; and
2. a **durable append-only audit journal** for reconstructing saga control-plane decisions.

They deliberately serve different purposes. Traces may be sampled or expire according to an organization's telemetry retention policy. The audit journal is stored next to saga state and is the durable record used by `get_audit_events`, `get_saga_timeline`, and `verify_audit_chain`.

## Trace propagation

Semantic Saga follows MCP SEP-414 and W3C Trace Context. For a modern MCP request it reads these keys from request `_meta`:

- `traceparent`
- `tracestate`
- `baggage`

For Streamable HTTP, W3C HTTP headers are accepted as a fallback when the corresponding `_meta` value is absent. Tracing data is never used to establish identity, tenant ownership, roles, or authorization.

The resulting trace path is:

```text
Agent / host
    │  traceparent / tracestate / baggage
    ▼
MCP tools/call span
    ▼
Semantic Saga lifecycle/workflow span
    ▼
Action forward span
    │  injected W3C Trace Context
    ▼
Downstream HTTP service
```

Parallel DAG nodes preserve the request parent context when work moves into executor threads, so independent action spans remain correlated with the same MCP request trace.

## OpenTelemetry signals

The base package depends only on `opentelemetry-api`. Without an SDK/exporter configuration, tracing and metrics are effectively no-op and require no collector.

For OTLP/HTTP export, install the optional extra:

```bash
python -m pip install 'semantic-saga-mcp[otel]'
```

Then configure a collector base endpoint:

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --otel-endpoint http://otel-collector:4318
```

Equivalent environment variables:

```text
SAGA_OTEL_ENDPOINT
SAGA_OTEL_HEADERS
SAGA_OTEL_SERVICE_NAME
```

`OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS` are also honored. `--otel-headers` / `SAGA_OTEL_HEADERS` use comma-separated `key=value` entries.

The service exports traces and metrics. OpenTelemetry logs are not used as the canonical audit record.

### Spans

Representative spans include:

```text
mcp.tools.call
semantic_saga.begin
semantic_saga.execute
semantic_saga.workflow.plan
semantic_saga.workflow.run
semantic_saga.action.forward
semantic_saga.approval
semantic_saga.checkpoint
semantic_saga.operator.retry
semantic_saga.commit
semantic_saga.rollback
semantic_saga.recovery
```

Attributes intentionally stay small and payload-free: tool/action name, action version, operation, outcome, approval decision, and similar control-plane dimensions. Saga IDs, arbitrary inputs/results, authentication tokens, action headers, and checkpoint payloads are not used as metric dimensions.

### Metrics

Semantic Saga records:

- `semantic_saga.saga.operations`
- `semantic_saga.action.attempts`
- `semantic_saga.compensation.attempts`
- `semantic_saga.approval.decisions`
- `semantic_saga.recovery.operations`
- `semantic_saga.action.duration`
- `semantic_saga.compensation.duration`

Metric labels are intentionally low-cardinality. Do not add saga IDs, step IDs, principal IDs, raw URLs, or user-provided values as metric labels.

## Durable audit journal

Every server process uses a `StoreAuditJournal` backed by the same persistence choice as saga state:

- `SagaStore` -> in-memory audit rows;
- `SQLiteSagaStore` -> `audit_events` in the same SQLite database; or
- `PostgresSagaStore` -> `audit_events` in the same PostgreSQL database/pool.

No second audit database is required.

Audit rows are append-only through the Semantic Saga application API. There is no update or delete journal operation.

A row contains only control-plane evidence:

```text
sequence
id
saga_id
event_type
actor_principal_id
actor_type
action
action_version
step_id
node_id
status
data                 # small event metadata only
trace_id
span_id
previous_hash
event_hash
created_at
```

Examples of event metadata are retry counts, approval decision, dependency count, checkpoint name, or whether an operator retry used `force`. The journal does **not** persist action input, action result, HTTP bodies, resolved secrets, credential headers, or checkpoint data.

## Hash-chain integrity

Events are chained per saga:

```text
Event 1
previous_hash = null
hash = SHA256(canonical(event 1))

Event 2
previous_hash = event1.hash
hash = SHA256(previous_hash + canonical(event 2))

Event 3
previous_hash = event2.hash
...
```

PostgreSQL uses a transaction-scoped advisory lock keyed by saga ID so concurrent appends cannot fork one saga's chain. SQLite uses an immediate write transaction. In-memory mode uses a process lock.

Call:

```text
verify_audit_chain
```

to recompute the complete chain and return whether it is valid, the number of events checked, and the current head hash.

This makes accidental or unauthorized row modification **tamper-evident**. It is not a cryptographic notarization service: an administrator with unrestricted database access could rewrite events and recompute the entire chain. Organizations that require stronger non-repudiation should periodically anchor the returned head hash in an independently controlled immutable/WORM system or signing service. That anchoring is intentionally outside Phase 6.

## Audit and timeline MCP tools

Authenticated read-capable principals can use:

### `get_audit_events`

Returns ordered audit events for a saga. Optional `event_types` filters the response and `limit` is capped at 5,000 rows.

### `verify_audit_chain`

Recomputes the complete chain independently of the normal query limit. A healthy response resembles:

```json
{
  "saga_id": "...",
  "valid": true,
  "events_checked": 12,
  "head_hash": "..."
}
```

### `get_saga_timeline`

Combines:

- saga status/timestamps;
- safe step summaries;
- safe DAG node summaries;
- audit events; and
- audit integrity status.

It intentionally omits step input/result/action-definition snapshots and checkpoint payloads. Use `get_saga` only when an authorized operator actually needs the full operational saga record.

All three tools call the normal saga ownership check first. A principal from another tenant receives `Saga not found`, even if it knows the saga ID.

## Actor identity

The authenticated request principal is attached to a request-local context and copied into relevant audit events. Actor identity does not become an OpenTelemetry metric label. This avoids high-cardinality metrics and unnecessary distribution of user identifiers into monitoring backends.

For approval/checkpoint decisions, the principal recorded by the existing workflow API remains authoritative and is also reflected in audit evidence.

## Secret and payload policy

The following must never be added to audit event data, span attributes, or metric labels:

- access tokens, API keys, cookies, or authorization headers;
- resolved `secret_ref` values;
- arbitrary action input/result objects;
- HTTP request/response bodies;
- checkpoint payload data;
- raw exception response bodies; or
- arbitrary MCP/user content.

If additional observability is needed, prefer identifiers and bounded classifications such as action ID/version, state, error type, risk level, retry count, or policy decision.

## Recommended operations

A production deployment normally sends OTLP data to an organization-controlled OpenTelemetry Collector and from there to its chosen tracing/metrics backend. Audit retention should be managed as part of the saga database lifecycle and backed up with the same rigor as saga state.

Alerting candidates include:

- sustained action or compensation failure rates;
- rising retry counts;
- `ROLLBACK_FAILED` or `RECOVERY_REQUIRED` events;
- repeated operator-forced retries; and
- audit-chain verification failures.

A future operator console can consume the timeline and audit APIs directly; Phase 6 deliberately provides the underlying evidence before adding that UI.
