# Phase 7 — Policy & Governance

Version: **0.8.0**

Phase 7 adds tenant-scoped business governance around the existing identity, durability, action-registry, workflow, and observability layers.

## Capabilities

- built-in deterministic JSON policy engine
- optional fail-closed Open Policy Agent REST adapter
- tenant-specific and wildcard policy scopes
- `allow`, `deny`, and `require_approval` effects
- risk-aware automatic approval thresholds
- action/domain/operation/resource/principal/role/scope matching
- hard saga budgets for steps, planned nodes, cumulative risk units, and requested parallelism
- current-policy re-evaluation immediately before ready DAG nodes execute
- policy-aware approvals and retries
- commit-time governance evaluation
- rollback/approval-rejection safety exceptions that cannot authorize new forward work
- durable hash-chained `POLICY_DECISION` evidence
- OpenTelemetry policy decision count and latency metrics
- MCP `get_policy_status` and `get_policy_decisions`
- package version 0.8.0

## Enforcement order

For the built-in engine:

1. hard budgets
2. tenant rules in file order
3. wildcard rules in file order
4. tenant/wildcard approval threshold
5. effective default effect

Budget limits cannot be relaxed by a later allow rule.

## Security model

The policy engine receives only control-plane metadata:

```text
principal identity/roles/scopes
phase
action id/version/kind
semantic domain/operation/resource/risk
saga state counters
prospective budget counters
approval state
```

It does **not** receive:

```text
action input
action result
HTTP body
resolved secret
credential header
checkpoint payload
```

The same sanitized document is used for the built-in engine and OPA.

## Policy decisions

Every evaluation writes a Phase 6 audit event containing a decision ID, effect, reason, revision, backend, matched rules, phase, risk, and non-sensitive budget counters.

Because policy decisions use the existing audit journal, no new database schema is required in Phase 7. PostgreSQL continues to serialize the audit chain with advisory transaction locking.

## Rollback safety

Business policy can reject new forward work but cannot make an already-started distributed transaction impossible to compensate. `rollback_saga` and `trigger_rollback` remain available after the existing RBAC authorization and produce a `POLICY_SAFETY_OVERRIDE` event.

Similarly, approval rejection remains a fail-safe operation. Governance may restrict who can approve, but it cannot require a principal to approve work.

## OPA integration

OPA mode uses the standard Data API shape:

```text
POST /v1/data/<decision_path>
```

OPA policy distribution/control remains external. This lets organizations continue using their existing Rego bundles, testing, distribution, and decision-log systems while Semantic Saga remains a policy enforcement point.

## Files

- `src/semantic_saga_mcp/policy.py`
- `src/semantic_saga_mcp/governance.py`
- `examples/governance_policy.json`
- `docs/governance_policy.schema.json`
- `docs/policy_governance.md`
- `tests/test_policy.py`
- `tests/test_governance.py`
- `tests/test_postgres_governance.py`

## Next direction

Phase 8 can focus on agent interoperability: an A2A adapter that exposes governed saga-backed operations/tasks to peer agents while preserving tenant identity, action policy, audit correlation, and recovery semantics.
