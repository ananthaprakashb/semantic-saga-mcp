# Enterprise Action Registry

Semantic Saga 0.6 treats every side-effecting action as a versioned contract. The contract is selected by the server operator, validated before execution, and snapshotted into the saga journal before the external side effect begins. Phase 5 adds optional versioned execution policy for retry, approval, and failure handling.

## Why versioned actions matter

A saga may be recovered hours or days after the original process disappears. If an operator changes an action's rollback URL, retry contract, approval requirement, or semantics in the meantime, using the new definition to recover an old step can violate the original workflow contract.

For every new concrete step Semantic Saga therefore persists `action`, `action_version`, `action_definition_hash`, and the exact non-secret `action_definition`. The snapshot is written while the step is still `EXECUTING`, before the forward request is sent.

## Registry format

Use `schema_version: 1` and an `actions` array. Each HTTP action declares one active immutable version, semantic metadata, input/output JSON Schemas, optional execution policy, and forward/compensation requests.

See `examples/action_registry.json` and `docs/action_registry.schema.json` for a complete example.

## Execution policy

An action may include:

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

`failure_mode` is either `rollback` or `pause`. `rollback` preserves classic Saga behavior. `pause` moves an exhausted planned workflow into `RECOVERY_REQUIRED` so an operator can reconcile, retry, or roll back.

Forward retries reuse the same persisted step id and therefore the same `Idempotency-Key`. Jitter is deterministic and hash-derived. Remote endpoints still need to honor idempotency because a timeout can leave the caller uncertain about whether the remote side effect happened.

Default behavior remains compatible with 0.5: one forward attempt, the coordinator's existing compensation retry count, automatic rollback, and no approval gate. If `execution_policy` is omitted, those defaults are resolved at runtime and are not inserted into the immutable action snapshot. This preserves Phase 4 hashes for existing definitions.

## Immutable versions

Within one registry process, the same `id` + `version` cannot be registered with different content. Change the version whenever behavior relevant to execution or compensation changes, including URL, method, body template, schema, semantic metadata, execution policy, or built-in implementation.

Only one version of an action can be `active: true`. Older versions can remain in the manifest with `active: false` for inspection. Historical HTTP recovery does not require an old version to remain active—or even present in the current manifest—because the exact HTTP contract is reconstructed from the persisted step snapshot.

## Runtime and built-in actions

Python/runtime actions cannot be safely reconstructed from JSON alone. Semantic Saga stores the implementation identity and a source hash. Recovery requires the currently registered runtime action to have the same id, version, and definition hash. If code changed without a version bump, compensation fails closed with `ROLLBACK_FAILED` instead of invoking changed logic.

When changing `create_text_file` or any future built-in action, bump its registered action version.

## JSON Schema enforcement

Input schemas are checked before a step is created or any external side effect occurs. Invalid input leaves the saga `ACTIVE` and creates no step.

Output schemas are checked after the action returns. Because the remote side effect may already have happened, an invalid output is treated as a workflow failure: the returned receipt is journaled on the failed step and rollback or pause policy is applied using that receipt.

## Secret references

Do not put credential values into action files. Header values may contain a structured secret reference:

```json
{
  "Authorization": {
    "secret_ref": "env://PAYMENTS_TOKEN",
    "prefix": "Bearer "
  }
}
```

The built-in provider supports `env://NAME`. The reference is persisted in the immutable action snapshot; the resolved secret value is not. Dry-run previews redact secret-referenced headers without resolving or printing their values.

The Python `SecretProvider` protocol is intentionally small so deployments can add adapters for Vault, AWS Secrets Manager, Azure Key Vault, GCP Secret Manager, or another organization-owned system without changing the saga coordinator.

## Historical recovery behavior

For HTTP steps Semantic Saga verifies the stored hash and identity, reconstructs the exact historical HTTP contract, resolves current secret material, and invokes compensation using the original step's stable rollback idempotency key.

For runtime steps it verifies the persisted hash, locates the exact registered runtime version, and requires the implementation definition hash to match before compensation. Any mismatch is surfaced as `COMPENSATION_FAILED` / `ROLLBACK_FAILED`.

## Upgrading journals from before 0.5

Steps created by releases before 0.5 do not contain immutable action snapshots. Semantic Saga refuses to compensate these rows by default because it cannot prove that the currently configured action has the same semantics as the historical action.

Preferred migration path: recover or complete pending pre-0.5 sagas with the pre-upgrade release, then upgrade. If an operator has independently verified that the currently configured actions are identical to the historical definitions, `--allow-legacy-action-recovery` (or `SAGA_ALLOW_LEGACY_ACTION_RECOVERY=true`) explicitly opts into the old behavior.

## MCP discovery

Authenticated read-capable principals can call `list_actions` and `get_action`. The discovery result includes the resolved execution policy but never resolves or exposes secret values. `viewer`, `operator`, `admin`, `semantic-saga:read`, and broader execute/admin scopes can inspect the registry according to the existing authorization model.
