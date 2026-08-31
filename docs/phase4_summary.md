# Phase 4 — Enterprise Action Registry

Phase 4 turns action configuration into a durable, inspectable contract rather than mutable process configuration.

## Delivered

- immutable action ids + versions;
- canonical SHA-256 definition hashes;
- persisted action version/hash/snapshot on every new saga step before the side effect;
- historical HTTP compensation reconstructed from the persisted snapshot;
- fail-closed runtime/built-in implementation hash validation;
- Draft 2020-12 input and output JSON Schema validation;
- semantic action metadata for domain, operation, resource, reversibility, risk, and effects;
- secret references with a pluggable `SecretProvider` protocol and built-in `env://` provider;
- secret redaction in dry-run output and no resolved secret material in saga snapshots;
- `list_actions` and `get_action` MCP discovery tools;
- SQLite and PostgreSQL migrations for registry fields;
- explicit pre-0.5 legacy-recovery escape hatch;
- manifest schema and example registry.

## Recovery invariant

A step must be compensated under the same contract that was journaled before its forward side effect. HTTP actions can be rebuilt from the immutable persisted snapshot. Runtime actions must still have the exact registered implementation version/hash. A mismatch never falls back silently to the current action definition.

## Compatibility

Legacy `actions.json` maps are still accepted and are represented as `legacy-v1` HTTP action contracts for newly created steps. Existing pre-0.5 journal rows have no immutable snapshot and therefore fail closed during recovery unless the operator explicitly enables legacy action recovery after verifying compatibility.

## Version

This phase advances the package to `0.5.0`.
