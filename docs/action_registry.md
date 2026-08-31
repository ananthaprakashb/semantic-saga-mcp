# Enterprise Action Registry

Semantic Saga 0.5 treats every side-effecting action as a versioned contract. The contract is selected by the server operator, validated before execution, and snapshotted into the saga journal before the external side effect begins.

## Why versioned actions matter

A saga may be recovered hours or days after the original process disappears. If an operator changes an action's rollback URL or semantics in the meantime, using the new definition to compensate an old step can damage a different resource or violate the original workflow contract.

For every new step Semantic Saga therefore persists:

- `action` — stable action id;
- `action_version` — operator-assigned immutable version;
- `action_definition_hash` — SHA-256 of the canonical definition snapshot; and
- `action_definition` — the exact non-secret contract used by the step.

The snapshot is written while the step is still `EXECUTING`, before the forward request is sent.

## Registry format

Use `schema_version: 1` and an `actions` array. Each HTTP action declares one active immutable version, semantic metadata, input/output JSON Schemas, and forward/compensation requests.

```json
{
  "schema_version": 1,
  "actions": [
    {
      "id": "create_repository",
      "version": "1.0.0",
      "kind": "http",
      "active": true,
      "semantic": {
        "domain": "source_control",
        "operation": "create",
        "resource": "repository",
        "reversibility": "full",
        "risk": "medium",
        "effects": {"creates": ["source_control.repository"]}
      },
      "input_schema": {
        "type": "object",
        "required": ["name"],
        "properties": {"name": {"type": "string", "minLength": 1}},
        "additionalProperties": false
      },
      "output_schema": {
        "type": "object",
        "required": ["id"],
        "properties": {"id": {"type": "string"}}
      },
      "forward": {
        "url": "https://scm.internal/repos",
        "method": "POST",
        "headers": {
          "Authorization": {
            "secret_ref": "env://SCM_SERVICE_TOKEN",
            "prefix": "Bearer "
          }
        },
        "body": {"name": "${input.name}"}
      },
      "compensation": {
        "url": "https://scm.internal/repos/delete",
        "method": "POST",
        "headers": {
          "Authorization": {
            "secret_ref": "env://SCM_SERVICE_TOKEN",
            "prefix": "Bearer "
          }
        },
        "body": {"id": "${result.id}"}
      }
    }
  ]
}
```

See `examples/action_registry.json` and `docs/action_registry.schema.json`.

## Immutable versions

Within one registry process, the same `id` + `version` cannot be registered with different content. Change the version whenever any behavior relevant to execution or compensation changes, including URL, method, body template, schema, semantic metadata, or built-in implementation.

Only one version of an action can be `active: true`. Older versions can remain in the manifest with `active: false` for inspection. Historical HTTP recovery does not require an old version to remain active—or even present in the current manifest—because the exact HTTP contract is reconstructed from the persisted step snapshot.

## Runtime and built-in actions

Python/runtime actions cannot be safely reconstructed from a JSON snapshot alone. Semantic Saga stores the implementation identity and a source hash. Recovery requires the currently registered runtime action to have the same id, version, and definition hash. If code changed without a version bump, compensation fails closed with `ROLLBACK_FAILED` instead of invoking changed logic.

When changing `create_text_file` or any future built-in action, bump its registered action version.

## JSON Schema enforcement

Input schemas are checked before a step is created or any external side effect occurs. Invalid input leaves the saga `ACTIVE` and creates no step.

Output schemas are checked after the action returns. Because the remote side effect may already have happened, an invalid output is treated as a workflow failure: the returned receipt is journaled on the failed step and rollback is attempted using that receipt.

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

HTTP step:

1. verify the persisted definition hash;
2. verify action id/version match the snapshot;
3. reconstruct the HTTP compensation from that exact snapshot;
4. resolve current secret material through the referenced secret provider; and
5. invoke compensation using the original step's stable rollback idempotency key.

Runtime step:

1. verify the persisted definition hash;
2. locate the exact registered runtime version; and
3. require its implementation definition hash to match before compensation.

Any mismatch is surfaced as `COMPENSATION_FAILED` / `ROLLBACK_FAILED`.

## Upgrading journals from before 0.5

Steps created by releases before 0.5 do not contain immutable action snapshots. Semantic Saga refuses to compensate these rows by default because it cannot prove that the currently configured action has the same semantics as the historical action.

Preferred migration path: recover or complete pending pre-0.5 sagas with the pre-upgrade release, then deploy 0.5.

If an operator has independently verified that the currently configured actions are identical to the historical definitions, `--allow-legacy-action-recovery` (or `SAGA_ALLOW_LEGACY_ACTION_RECOVERY=true`) explicitly opts into the old behavior. This is a migration escape hatch, not the recommended steady state.

## MCP discovery

Authenticated read-capable principals can call:

- `list_actions` — active ids, versions, hashes, schemas, and semantic metadata;
- `get_action` — one action id and optional version.

These tools do not resolve or expose secret values. `viewer`, `operator`, `admin`, `semantic-saga:read`, and broader execute/admin scopes can inspect the registry according to the existing authorization model.
