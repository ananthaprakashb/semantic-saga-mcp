from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from .actions import Action
from .store import SagaStore


def now() -> str:
    return datetime.now(UTC).isoformat()


class SagaError(RuntimeError):
    pass


class Coordinator:
    def __init__(self, store: SagaStore, actions: dict[str, Action], compensation_retries: int = 3) -> None:
        self.store, self.actions, self.compensation_retries = store, actions, compensation_retries

    def begin(self, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        saga_id, timestamp = str(uuid.uuid4()), now()
        self.store.execute("INSERT INTO sagas VALUES (?, 'ACTIVE', ?, ?, ?, NULL)",
                           (saga_id, json.dumps(metadata or {}), timestamp, timestamp))
        return self.get(saga_id)

    def execute(self, saga_id: str, action_name: str, values: dict[str, Any]) -> dict[str, Any]:
        saga = self._require(saga_id)
        if saga["status"] != "ACTIVE":
            raise SagaError(f"Saga {saga_id} is {saga['status']}; steps require ACTIVE")
        if action_name not in self.actions:
            raise SagaError(f"Unknown action: {action_name}")
        sequence = self.store.one("SELECT COALESCE(MAX(sequence), 0) + 1 AS n FROM steps WHERE saga_id=?", (saga_id,))["n"]
        step_id, timestamp = str(uuid.uuid4()), now()
        # Persist intent before the side effect. A crash therefore leaves an UNCERTAIN step
        # that rollback will compensate using the same idempotency key.
        self.store.execute("INSERT INTO steps VALUES (?, ?, ?, ?, ?, 'EXECUTING', NULL, NULL, 0, ?, ?)",
                           (step_id, saga_id, sequence, action_name, json.dumps(values), timestamp, timestamp))
        try:
            result = self.actions[action_name].execute(values, saga_id, step_id)
            self.store.execute("UPDATE steps SET status='COMPLETED', result=?, updated_at=? WHERE id=?",
                               (json.dumps(result), now(), step_id))
            return SagaStore.decode(self.store.one("SELECT * FROM steps WHERE id=?", (step_id,)))
        except Exception as exc:
            self.store.execute("UPDATE steps SET status='FAILED', error=?, updated_at=? WHERE id=?", (str(exc), now(), step_id))
            self.store.execute("UPDATE sagas SET status='FAILED', error=?, updated_at=? WHERE id=?", (str(exc), now(), saga_id))
            self.rollback(saga_id)
            raise SagaError(f"Action {action_name} failed; rollback attempted: {exc}") from exc

    def commit(self, saga_id: str) -> dict[str, Any]:
        saga = self._require(saga_id)
        if saga["status"] != "ACTIVE":
            raise SagaError(f"Only ACTIVE sagas can commit (was {saga['status']})")
        self.store.execute("UPDATE sagas SET status='COMMITTED', updated_at=? WHERE id=?", (now(), saga_id))
        return self.get(saga_id)

    def rollback(self, saga_id: str) -> dict[str, Any]:
        saga = self._require(saga_id)
        if saga["status"] in ("COMMITTED", "ROLLED_BACK"):
            raise SagaError(f"Saga {saga_id} is already {saga['status']}")
        self.store.execute("UPDATE sagas SET status='ROLLING_BACK', updated_at=? WHERE id=?", (now(), saga_id))
        steps = self.store.all("SELECT * FROM steps WHERE saga_id=? AND status IN ('COMPLETED','EXECUTING','FAILED','COMPENSATION_FAILED') ORDER BY sequence DESC", (saga_id,))
        failures = []
        for raw in steps:
            step = SagaStore.decode(raw)
            action = self.actions.get(step["action"])
            if not action:
                failures.append(f"{step['id']}: action definition unavailable")
                continue
            last_error = None
            for attempt in range(self.compensation_retries):
                try:
                    action.compensate(step["input"], step["result"], saga_id, step["id"])
                    self.store.execute("UPDATE steps SET status='COMPENSATED', compensation_attempts=?, error=NULL, updated_at=? WHERE id=?", (attempt + 1, now(), step["id"]))
                    last_error = None
                    break
                except Exception as exc:
                    last_error = str(exc)
            if last_error:
                failures.append(f"{step['id']}: {last_error}")
                self.store.execute("UPDATE steps SET status='COMPENSATION_FAILED', compensation_attempts=?, error=?, updated_at=? WHERE id=?", (self.compensation_retries, last_error, now(), step["id"]))
        status = "ROLLBACK_FAILED" if failures else "ROLLED_BACK"
        self.store.execute("UPDATE sagas SET status=?, error=?, updated_at=? WHERE id=?", (status, "; ".join(failures) or saga.get("error"), now(), saga_id))
        return self.get(saga_id)

    def get(self, saga_id: str) -> dict[str, Any]:
        saga = SagaStore.decode(self._require(saga_id))
        saga["steps"] = [SagaStore.decode(row) for row in self.store.all("SELECT * FROM steps WHERE saga_id=? ORDER BY sequence", (saga_id,))]
        return saga

    def _require(self, saga_id: str) -> dict[str, Any]:
        row = self.store.one("SELECT * FROM sagas WHERE id=?", (saga_id,))
        if not row:
            raise SagaError(f"Saga not found: {saga_id}")
        return row
