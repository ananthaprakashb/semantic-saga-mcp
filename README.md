# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes governed side effects, journals intent before execution, and invokes compensating actions when work must unwind.

Version 0.8 adds **tenant policy and governance** on top of MCP `2026-07-28`, enterprise identity, PostgreSQL distributed durability, the immutable Enterprise Action Registry, the Semantic Saga Engine, and Phase 6 OpenTelemetry/audit. Organizations can now enforce action/resource/risk rules, budgets, approval thresholds, and current-policy re-evaluation before side effects, using either a deterministic built-in JSON policy or an external Open Policy Agent (OPA) decision point.

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
- **Tenant governance:** business policy can allow, deny, or require approval by action, semantic resource/risk, principal attributes, and execution phase.
- **Hard budgets:** per-tenant limits for steps, planned nodes, cumulative risk units, and requested parallelism are checked before rules can allow work.
- **Current-policy enforcement:** ready DAG nodes are re-evaluated immediately before side effects so stale plans cannot silently outrun a policy change.
- **Rollback safety:** business policy can stop new forward work but cannot make compensation unavailable after normal RBAC authorization.
- **Distributed safety:** renewable saga leases, fencing tokens, atomic step sequencing, and PostgreSQL `SKIP LOCKED` recovery prevent stale or duplicate workers from owning the same saga.
- **Distributed tracing:** MCP SEP-414/W3C trace context is correlated through MCP, saga/workflow, policy, and downstream HTTP action spans.
- **Low-cardinality metrics:** action/compensation attempts and durations, approval/recovery/saga operations, and policy decision count/latency can be exported with OpenTelemetry.
- **Durable audit evidence:** control-plane events and policy decisions are append-only through the application API and hash-chained per saga in memory, SQLite, or PostgreSQL.
- **Payload-safe diagnostics:** policy/audit/timeline output omits action input/result payloads, credential material, HTTP bodies, resolved secrets, and checkpoint payloads.

Semantic Saga coordinates systems that do not share an ACID transaction. A compensation can still fail; those failures are surfaced as `ROLLBACK_FAILED` for later operator handling.

## Quick start

Python 3.10 or newer is required.

```bash
python -m pip install -e .
semantic-saga-mcp
```

The default transport is stdio, governance is disabled, and the default store is in-memory. For durable local development with tenant policy:

```bash
semantic-saga-mcp \
  --database ./semantic-saga.db \
  --actions ./examples/action_registry.json \
  --policy-mode json \
  --policy-file ./examples/governance_policy.json
```

For horizontally scaled production use PostgreSQL and Streamable HTTP:

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

See [`docs/enterprise_identity.md`](docs/enterprise_identity.md), [`docs/distributed_durability.md`](docs/distributed_durability.md), [`docs/observability_audit.md`](docs/observability_audit.md), and [`docs/policy_governance.md`](docs/policy_governance.md).

## Policy and governance

Authentication/RBAC and governance are separate layers:

```text
OAuth / proxy identity
        │
        ▼
RBAC: may this principal call this MCP tool?
        │
        ▼
Governance: may this principal perform THIS action/resource/risk NOW?
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

The policy document supports wildcard and tenant-specific configuration, hard budgets, risk-based approval thresholds, and ordered rules. See [`docs/governance_policy.schema.json`](docs/governance_policy.schema.json).

Built-in evaluation order is deterministic:

1. hard budgets
2. tenant-specific rules in file order
3. wildcard rules in file order
4. approval threshold
5. effective default effect

Supported policy effects are `allow`, `deny`, and `require_approval`.

A governance policy can match action ID (including `*` patterns), execution phase, semantic risk/domain/operation/resource, principal type, roles, and scopes. Example:

```json
{
  "id": "critical-payment-approval-admin",
  "effect": "deny",
  "match": {
    "phases": ["approve"],
    "domains": ["payments"],
    "risks": ["critical"],
    "roles_none": ["admin"]
  },
  "reason": "Critical payment changes require an admin approver"
}
```

Tenant budgets can bound:

```json
{
  "max_steps_per_saga": 100,
  "max_planned_nodes": 100,
  "max_risk_units": 250,
  "max_parallel": 8
}
```

Default risk units are low=1, medium=2, high=5, critical=10, unknown=3 and can be overridden with `risk_weights`.

### OPA policy-as-code

```bash
export SAGA_OPA_TOKEN='optional-private-token'
semantic-saga-mcp \
  --policy-mode opa \
  --policy-opa-url http://opa.internal:8181 \
  --policy-opa-decision-path semantic_saga/decision \
  --policy-opa-token-env SAGA_OPA_TOKEN
```

Semantic Saga calls OPA's Data API with only sanitized control-plane context: tenant/principal/roles/scopes, action ID/version/kind + semantic metadata, saga counters, phase, requested parallelism, and approval state. Action inputs/results and secret material are never sent to the policy engine.

OPA errors, timeouts, invalid results, or undefined decisions fail closed for forward work.

### Durable policy evidence

Every evaluation produces a unique decision ID and appends a `POLICY_DECISION` event containing effect, reason, revision, backend, matched rules, phase, risk, and non-sensitive counters. Policy evidence shares the Phase 6 audit hash chain and trace/span correlation.

Use:

```text
get_policy_status
get_policy_decisions
get_audit_events
verify_audit_chain
```

Rollback and approval rejection remain available as narrow fail-safe operations and create `POLICY_SAFETY_OVERRIDE` evidence; they do not authorize new forward work.

See [`docs/policy_governance.md`](docs/policy_governance.md).

## Observability and audit

The base package includes the OpenTelemetry API. To export OTLP/HTTP traces and metrics:

```bash
python -m pip install 'semantic-saga-mcp[otel]'
semantic-saga-mcp --otel-endpoint http://otel-collector:4318
```

Environment equivalents include `SAGA_OTEL_ENDPOINT`, `SAGA_OTEL_HEADERS`, `SAGA_OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_EXPORTER_OTLP_HEADERS`.

Semantic Saga accepts W3C `traceparent`, `tracestate`, and `baggage` through MCP request `_meta`; Streamable HTTP headers are a fallback. Active context is injected into downstream HTTP actions.

The durable audit journal includes control-plane identifiers/statuses, actor identity, action/version, policy/retry/approval metadata, timestamps, and trace/span correlation while intentionally excluding payloads/secrets.

Each saga has a SHA-256 chain:

```text
previous_hash -> event_hash -> next previous_hash -> ...
```

`verify_audit_chain` recomputes the complete chain. It is tamper-evident, not a substitute for independently anchored/WORM evidence.

See [`docs/observability_audit.md`](docs/observability_audit.md).

## Semantic Saga Engine

The orchestration layer is opt-in. Existing integrations may continue using `execute_saga_step`, but governed high-risk work should generally use the planned workflow path:

1. `begin_saga`
2. `plan_saga_step` for each node
3. reference earlier node ids through `depends_on`
4. `approve_saga_step` where action/governance requires approval
5. `run_ready_steps`
6. optionally call `checkpoint_saga`
7. `commit_saga` after every planned node is `COMPLETED`

Policy is evaluated at planning and again immediately before ready-node execution.

Planned nodes use durable states:

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

Action contracts can version retry/backoff, `failure_mode`, and `approval_required`. Business governance is evaluated separately and can add an approval requirement but never remove the immutable action requirement.

`failure_mode: "rollback"` preserves classic Saga behavior. `failure_mode: "pause"` leaves the workflow in `RECOVERY_REQUIRED` for reconciliation/operator action.

## Enterprise Action Registry

Actions are operator-owned immutable contracts with semantic metadata, JSON Schemas, execution policy, forward request, compensation request, and secret references.

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

Historical rollback always uses the persisted contract/version/hash rather than a later active definition.

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

The built-in provider supports `env://NAME`; the `SecretProvider` protocol can be implemented for enterprise secret managers.

## Storage

### In-memory

Use `SagaStore` for tests/ephemeral development. Saga state, audit evidence, and policy decisions disappear with the process.

### SQLite

```bash
semantic-saga-mcp --database ./semantic-saga.db
```

SQLite uses WAL mode and is intended for one Semantic Saga deployment. Workflow DAG state lives in saga metadata, concrete executions use `steps`, and audit/policy decisions use the Phase 6 `audit_events` table.

### PostgreSQL

```bash
semantic-saga-mcp \
  --transport streamable-http \
  --postgres-dsn 'postgresql://semantic_saga:secret@db.internal/semantic_saga' \
  --worker-id "$HOSTNAME" \
  --lease-seconds 30
```

PostgreSQL is recommended for multiple replicas. Saga leases/fencing protect workflow ownership; audit/policy events use the same pool and PostgreSQL advisory locking serializes the hash chain per saga.

## Enterprise identity

For HTTP deployment, Semantic Saga can validate OAuth/OIDC JWT access tokens using issuer, audience, JWKS, expiry, and allowed algorithms. Durable saga ownership is tenant scoped.

| Identity | Read saga/registry/audit/policy | Orchestrate / execute | Commit / rollback |
| --- | --- | --- | --- |
| `viewer` | yes | no | no |
| `operator` | yes | yes | yes |
| `admin` | yes | yes | yes |

Equivalent scopes are `semantic-saga:read`, `semantic-saga:execute`, and `semantic-saga:admin`. Governance can further narrow what an authorized operator/admin may do for a specific resource/risk/tenant.

## MCP tools

| Tool | Purpose |
| --- | --- |
| `list_actions` | List active action contracts, schemas, semantic metadata, and execution policy. |
| `get_action` | Inspect one action id/version. |
| `get_policy_status` | Inspect effective backend/revision/budgets/approval threshold/rule IDs for the caller tenant. |
| `get_policy_decisions` | Read durable governance decisions for a tenant-owned saga. |
| `begin_saga` | Create an `ACTIVE` saga. |
| `execute_saga_step` | Immediate compatibility path; governance may deny or require planned approval. |
| `plan_saga_step` | Persist a version-pinned node; governance may add approval or reject. |
| `run_ready_steps` | Re-evaluate governance and execute ready dependency waves. |
| `approve_saga_step` | Approve/reject a gated node; approval itself may be governed. |
| `retry_saga_step` | Reset failed/rejected/blocked work after current governance checks. |
| `checkpoint_saga` | Persist a named workflow milestone. |
| `commit_saga` | Commit only after workflow + governance checks pass. |
| `rollback_saga` | Compensate eligible concrete steps in reverse order using the safety path. |
| `trigger_rollback` | Immediately start compensation after a client-detected failure. |
| `get_saga` | Inspect full saga/workflow/recovery state. |
| `get_saga_timeline` | Inspect payload-safe execution timeline plus audit integrity. |
| `get_audit_events` | Read ordered append-only audit evidence. |
| `verify_audit_chain` | Recompute the complete per-saga SHA-256 hash chain. |

## Built-in file transaction

`create_text_file@1.0.0` creates a relative `.txt` file below the configured file root and compensates only files created by that step. Existing files are never overwritten or deleted.

## Upgrade behavior

Steps created before 0.5 have no immutable action snapshot and are refused for compensation by default unless `--allow-legacy-action-recovery` is explicitly enabled.

Phase 6 creates the audit table automatically. **Phase 7 requires no database schema migration**: governance decisions use the same audit journal.

Governance defaults to `--policy-mode none`, so upgrading to 0.8 does not silently change existing authorization behavior. Enable JSON or OPA policy explicitly after validating your rules.

## Development and CI

```bash
python -m unittest discover -s tests -v
```

Pull requests validate Python 3.10–3.13, MCP 2026 Streamable HTTP, OAuth tenant/RBAC, governance approval/budget/tenant isolation, built-in JSON and OPA adapters, action-registry recovery, DAG/retry/approval/recovery behavior, audit integrity/payload safety, optional OpenTelemetry SDK/exporter compatibility, W3C trace propagation, package/Twine validation, and PostgreSQL 16 concurrency/fencing/recovery/audit/governance/server-startup guarantees.

## Publishing

The repository uses PyPI trusted publishing. Before release, update `__version__`, run CI/package validation, inspect distribution contents, and publish a GitHub release. PyPI versions are immutable.

## Phase notes

- [`docs/phase2_summary.md`](docs/phase2_summary.md) — OAuth identity, tenants, and RBAC.
- [`docs/phase3_summary.md`](docs/phase3_summary.md) — PostgreSQL distributed durability.
- [`docs/phase4_summary.md`](docs/phase4_summary.md) — immutable enterprise action registry, schemas, semantics, and secret references.
- [`docs/phase5_summary.md`](docs/phase5_summary.md) — DAG orchestration, retries, approvals, checkpoints, parallel execution, and operator recovery.
- [`docs/phase6_summary.md`](docs/phase6_summary.md) — OpenTelemetry traces/metrics, durable hash-chained audit, and execution timelines.
- [`docs/phase7_summary.md`](docs/phase7_summary.md) — tenant governance, budgets, risk-aware approvals, OPA integration, and policy decision evidence.
