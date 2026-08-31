from __future__ import annotations

import hashlib
from typing import Any, Mapping

DEFAULT_EXECUTION_POLICY: dict[str, Any] = {
    "forward": {"max_attempts": 1, "initial_backoff_seconds": 0.0, "backoff_multiplier": 2.0, "max_backoff_seconds": 30.0, "jitter": 0.0},
    "compensation": {"max_attempts": 3, "initial_backoff_seconds": 0.0, "backoff_multiplier": 2.0, "max_backoff_seconds": 30.0, "jitter": 0.0},
    "failure_mode": "rollback",
    "approval_required": False,
}

class EnginePolicyError(ValueError):
    pass

def _retry_policy(raw: Mapping[str, Any] | None, default: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(default)
    if raw:
        value.update(raw)
    try:
        attempts = int(value["max_attempts"])
        initial = float(value["initial_backoff_seconds"])
        multiplier = float(value["backoff_multiplier"])
        maximum = float(value["max_backoff_seconds"])
        jitter = float(value["jitter"])
    except (KeyError, TypeError, ValueError) as exc:
        raise EnginePolicyError(f"Invalid retry policy: {exc}") from exc
    if attempts < 1:
        raise EnginePolicyError("max_attempts must be >= 1")
    if initial < 0 or maximum < 0:
        raise EnginePolicyError("backoff values must be >= 0")
    if multiplier < 1:
        raise EnginePolicyError("backoff_multiplier must be >= 1")
    if not 0 <= jitter <= 1:
        raise EnginePolicyError("jitter must be between 0 and 1")
    return {"max_attempts": attempts, "initial_backoff_seconds": initial, "backoff_multiplier": multiplier, "max_backoff_seconds": maximum, "jitter": jitter}

def normalize_execution_policy(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = raw or {}
    failure_mode = str(raw.get("failure_mode", DEFAULT_EXECUTION_POLICY["failure_mode"]))
    if failure_mode not in {"rollback", "pause"}:
        raise EnginePolicyError("failure_mode must be 'rollback' or 'pause'")
    approval_required = raw.get("approval_required", DEFAULT_EXECUTION_POLICY["approval_required"])
    if not isinstance(approval_required, bool):
        raise EnginePolicyError("approval_required must be a boolean")
    return {
        "forward": _retry_policy(raw.get("forward"), DEFAULT_EXECUTION_POLICY["forward"]),
        "compensation": _retry_policy(raw.get("compensation"), DEFAULT_EXECUTION_POLICY["compensation"]),
        "failure_mode": failure_mode,
        "approval_required": approval_required,
    }

def retry_delay_seconds(policy: Mapping[str, Any], attempt_number: int, seed: str) -> float:
    if attempt_number < 1:
        raise ValueError("attempt_number must be >= 1")
    initial = float(policy.get("initial_backoff_seconds", 0.0))
    multiplier = float(policy.get("backoff_multiplier", 2.0))
    maximum = float(policy.get("max_backoff_seconds", 30.0))
    jitter = float(policy.get("jitter", 0.0))
    base = min(maximum, initial * (multiplier ** (attempt_number - 1)))
    if base <= 0 or jitter <= 0:
        return base
    digest = hashlib.sha256(f"{seed}:{attempt_number}".encode("utf-8")).digest()
    unit = int.from_bytes(digest[:8], "big") / float(2**64 - 1)
    return max(0.0, base * (1.0 - jitter + (2.0 * jitter * unit)))

def engine_state(metadata: Mapping[str, Any]) -> dict[str, Any]:
    raw = metadata.get("_engine")
    if not isinstance(raw, dict):
        return {"version": 1, "nodes": {}, "checkpoints": []}
    state = dict(raw)
    state.setdefault("version", 1)
    state.setdefault("nodes", {})
    state.setdefault("checkpoints", [])
    if state["version"] != 1 or not isinstance(state["nodes"], dict) or not isinstance(state["checkpoints"], list):
        raise EnginePolicyError("Invalid persisted saga engine state")
    return state

def dependency_status(node: Mapping[str, Any], nodes: Mapping[str, Mapping[str, Any]]) -> str:
    dependencies = node.get("depends_on", [])
    if not isinstance(dependencies, list):
        return "blocked"
    if not dependencies:
        return "ready"
    statuses: list[str] = []
    for dependency in dependencies:
        target = nodes.get(str(dependency))
        if target is None:
            return "blocked"
        statuses.append(str(target.get("status", "")))
    if all(status == "COMPLETED" for status in statuses):
        return "ready"
    if any(status in {"FAILED", "REJECTED", "BLOCKED"} for status in statuses):
        return "blocked"
    return "waiting"
