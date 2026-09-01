# Policy & Governance

Semantic Saga 0.8 adds a tenant-scoped policy enforcement layer around the existing durable saga engine. Authentication/RBAC and business governance remain separate:

1. OAuth/RBAC decides whether a principal may call an MCP orchestration/read tool at all.
2. Governance decides whether the requested action, resource, risk, approval, concurrency, and saga budget are permitted **right now** for that tenant.

Policy enforcement never receives action input/result payloads. The decision document contains only principal identity attributes, action semantic metadata, saga counters, and requested control-plane parameters.

## Backends

### Disabled

Default behavior remains backward compatible:

```bash
semantic-saga-mcp --policy-mode none
```

The engine returns an internal `allow` decision and existing Phase 1–6 behavior is unchanged.

### Built-in JSON policy

```bash
semantic-saga-mcp \
  --policy-mode json \
  --policy-file ./examples/governance_policy.json
```

The JSON document is validated at startup. See:

- `examples/governance_policy.json`
- `docs/governance_policy.schema.json`

A JSON policy is loaded once at process startup. Replace/redeploy the policy file to activate a new revision. Each decision records the revision used at evaluation time.

### Open Policy Agent (OPA)

```bash
export SAGA_OPA_TOKEN='optional-private-token'

semantic-saga-mcp \
  --policy-mode opa \
  --policy-opa-url http://opa.internal:8181 \
  --policy-opa-decision-path semantic_saga/decision \
  --policy-opa-timeout 2 \
  --policy-opa-token-env SAGA_OPA_TOKEN
```

Semantic Saga calls OPA's Data API at:

```text
POST /v1/data/semantic_saga/decision
```

Only the sanitized governance context is sent. The optional bearer token is resolved from the named environment variable and is never journaled.

OPA errors, timeouts, invalid results, and undefined decisions fail closed for forward work.

## Policy decision input

Conceptually, every decision receives:

```json
{
  "tenant_id": "finance",
  "principal": {
    "id": "alice",
    "type": "user",
    "roles": ["operator"],
    "scopes": ["semantic-saga:execute"],
    "authenticated": true
  },
  "phase": "plan",
  "action": {
    "id": "create_payment_account",
    "version": "2.1.0",
    "kind": "http",
    "semantic": {
      "domain": "payments",
      "operation": "create",
      "resource": "merchant_account",
      "risk": "high"
    }
  },
  "saga": {
    "id": "...",
    "status": "ACTIVE",
    "steps": 3,
    "planned_nodes": 5,
    "risk_units": 12
  },
  "request": {
    "prospective_steps": 3,
    "prospective_planned_nodes": 6,
    "prospective_risk_units": 17,
    "requested_max_parallel": 4,
    "approval_granted": false
  }
}
```

There is intentionally no action `input`, action `result`, HTTP request body, secret reference value, authorization header, or checkpoint payload.

## Effects

A decision has one of three effects:

- `allow` — continue.
- `deny` — reject before the side effect.
- `require_approval` — planning automatically adds an approval gate; immediate `execute_saga_step` is rejected and the caller is directed to the planned workflow path.

Every decision has a `decision_id`, revision, backend, reason, and matched rule IDs when available.

## Built-in JSON evaluation order

The built-in engine is intentionally deterministic.

1. **Hard budgets** are checked first. An allow rule cannot exceed a budget.
2. Tenant-specific rules are evaluated in file order.
3. Wildcard (`*`) rules are evaluated in file order.
4. `approval_at_or_above_risk` is applied.
5. The effective default effect is returned.

Tenant settings override wildcard settings for budgets/default effect/approval threshold. Tenant rules run before wildcard rules.

## Rule matching

A rule can match:

- action ID (`actions`, supports shell-style `*` patterns)
- phase (`plan`, `execute`, `run`, `approve`, `retry`, `commit`)
- semantic risk
- semantic domain
- semantic operation
- semantic resource
- principal type
- `roles_any`
- `roles_all`
- `roles_none`
- `scopes_any`

Example:

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

## Risk-aware approvals

Tenant policy may configure:

```json
{
  "approval_at_or_above_risk": "high"
}
```

A high/critical action planned without another approval requirement becomes `WAITING_APPROVAL` automatically. Existing action-level `execution_policy.approval_required` remains authoritative: governance can add an approval gate but never remove one.

Current policy is evaluated again before a `READY` node executes. If policy changed after planning:

- a new deny stops the wave before that node's side effect;
- a newly required approval stops execution unless the planned node already has a valid approval gate/decision.

This prevents stale plans from silently outrunning a policy change.

## Budgets

The JSON backend supports these hard tenant limits:

```json
{
  "budgets": {
    "max_steps_per_saga": 100,
    "max_planned_nodes": 100,
    "max_risk_units": 250,
    "max_parallel": 8
  }
}
```

Risk units are derived from the action registry's semantic `risk` field. Defaults:

| Risk | Units |
| --- | ---: |
| low | 1 |
| medium | 2 |
| high | 5 |
| critical | 10 |
| unknown | 3 |

`risk_weights` can override these values in the policy document.

Planned workflow nodes count once toward risk units. Their concrete step rows are not double-counted after execution. Immediate compatibility-path steps are counted directly.

## Rollback safety

Forward business policy does not disable compensation. `rollback_saga` / `trigger_rollback` remain available after normal RBAC authorization even if the current business policy is deny-all or the OPA service is unavailable.

Semantic Saga records a `POLICY_SAFETY_OVERRIDE` audit event for this safety path. Approval rejection is handled similarly: a principal who is already allowed to call the approval tool may always reject pending work; governance cannot force risky work to be approved.

This exception is deliberately narrow. It does **not** allow new forward side effects.

## Durable policy evidence

Every evaluated policy decision is appended to the Phase 6 hash-chained audit journal as `POLICY_DECISION` with:

- decision ID
- effect
- reason
- policy revision
- backend (`none`, `json`, `opa`, or `error`)
- matched rule IDs
- phase
- semantic risk
- non-sensitive budget/approval counters
- current trace/span correlation when tracing is active

The action payload is never copied into policy evidence.

Use MCP:

```text
get_policy_status
get_policy_decisions
get_audit_events
verify_audit_chain
```

`get_policy_decisions` first performs the normal saga ownership lookup, so a caller from another tenant cannot use governance evidence as a cross-tenant side channel.

## OpenTelemetry

Phase 7 adds:

```text
semantic_saga.policy.decisions
semantic_saga.policy.duration
```

Metric dimensions are limited to decision effect/backend. Tenant IDs, principal IDs, saga IDs, action payload values, and reasons are deliberately excluded from metric attributes.

## OPA result contract

OPA may return a boolean:

```json
{"result": true}
```

or an object:

```json
{
  "result": {
    "effect": "require_approval",
    "reason": "Production database changes require review",
    "revision": "bundle-2026-08-31",
    "matched_rules": ["prod.db.approval"]
  }
}
```

An optional `decision_id` may also be returned. If absent, Semantic Saga generates one.

## Governance boundaries

Phase 7 is an enforcement layer, not a centralized policy-management product. For the built-in backend, the policy file is operator-managed configuration. For OPA, bundle distribution, policy source control, testing, status, and decision-log collection remain OPA responsibilities.

The `PolicyEngine` interface is intentionally small so additional enterprise engines can be added later without coupling the saga engine to one policy language.
