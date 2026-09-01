# Phase 6 — Observability and Audit

Phase 6 turns Semantic Saga's durable execution state into an operationally observable and reviewable control plane.

## Delivered

- OpenTelemetry API instrumentation throughout MCP and saga execution.
- MCP SEP-414 / W3C `traceparent`, `tracestate`, and `baggage` extraction.
- W3C trace propagation into downstream HTTP actions.
- Trace-parent preservation across parallel DAG executor threads.
- Optional OTLP/HTTP trace and metric export through `semantic-saga-mcp[otel]`.
- Low-cardinality action, compensation, approval, recovery, and saga metrics.
- Durable append-only audit journal backed by memory, SQLite, or PostgreSQL.
- Per-saga SHA-256 audit hash chains and complete-chain integrity verification.
- PostgreSQL per-saga advisory locking for concurrent audit appends.
- Actor, action/version, step/node, status, retry, trace/span, and timestamp correlation.
- Payload-safe `get_saga_timeline`.
- Read-only `get_audit_events` and `verify_audit_chain` MCP tools.
- Existing tenant isolation and viewer/read-scope authorization applied to all observability tools.
- Explicit tests ensuring action inputs/results, resolved secret-like values, and checkpoint payloads are absent from audit/timeline output.

## Trace flow

```text
Agent / MCP host
       │
       │ W3C Trace Context via MCP _meta
       ▼
  tools/call span
       │
       ▼
 saga/workflow span
       │
       ├──────────────┐
       ▼              ▼
 action A span    action B span
       │              │
       ▼              ▼
 downstream       downstream
 HTTP service     HTTP service
```

Each durable audit event can carry the active `trace_id` and `span_id`, allowing an operator to correlate a business/control-plane transition with live trace data when that telemetry is retained.

## Audit integrity model

Audit events are chained independently for every saga:

```text
H1 = SHA256(null + event1)
H2 = SHA256(H1   + event2)
H3 = SHA256(H2   + event3)
```

The application exposes no audit update/delete operation. `verify_audit_chain` recomputes the complete chain and detects row mutation, deletion from the middle, reordering, or chain discontinuity.

The chain is intentionally described as **tamper-evident**, not tamper-proof. A database administrator capable of rewriting the complete audit table could also recompute hashes. Stronger external anchoring/WORM retention belongs to a future governance/operations phase.

## Data-minimization invariant

Audit and telemetry contain control-plane metadata, not business payloads. Phase 6 deliberately excludes:

```text
action input
action result
HTTP bodies
credential headers
resolved secret values
checkpoint payloads
arbitrary user/MCP content
```

This also avoids high-cardinality or sensitive metric dimensions.

## New MCP tools

```text
get_saga_timeline
get_audit_events
verify_audit_chain
```

All three first perform the same saga ownership lookup used by `get_saga`, so a caller outside the saga's tenant cannot use observability interfaces to infer another tenant's workflow.

## Packaging

Base installation:

```bash
pip install semantic-saga-mcp
```

includes the OpenTelemetry API and works with no collector.

Production OTLP exporters are optional:

```bash
pip install 'semantic-saga-mcp[otel]'
semantic-saga-mcp --otel-endpoint http://otel-collector:4318
```

Package version: **0.7.0**.

## Validation strategy

Phase 6 extends CI with:

- all existing Python 3.10–3.13 unit suites;
- payload-safety and audit-integrity tests;
- SQLite restart persistence;
- MCP audit/timeline tool round trips;
- a dedicated optional OpenTelemetry SDK/exporter job;
- W3C parent/child/downstream propagation tests;
- OAuth viewer access and cross-tenant denial for observability tools; and
- real PostgreSQL 16 audit persistence/integrity alongside the Phase 3 lease/fencing/recovery suite.
