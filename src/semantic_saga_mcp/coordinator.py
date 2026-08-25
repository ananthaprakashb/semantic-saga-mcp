from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from .actions import Action
from .store import SagaStoreProtocol


def now() -> str:
    return datetime.now(UTC).isoformat()


class SagaError(RuntimeError):
    pass


class Coordinator:
    def __init__(self, store: SagaStoreProtocol, actions: dict[str, Action], compensation_retries: int = 3) -> None:
        self.store, self.actions, self.compensation_retries = store, actions, compensation_retries

    def begin(self, metadata: dict[str, Any] | None = None, *, session_id: str = "default") -> dict[str, Any]:
        saga_id, timestamp = str(uuid.uuid4()), now()
        self.store.create_saga({"id": saga_id, "session_id": session_id, "status": "ACTIVE", "metadata": metadata or {}, "created_at": timestamp, "updated_at": timestamp, "error": None})
        return self.get(saga_id, session_id=session_id)

    def execute(self, saga_id: str, action_name: str, values: dict[str, Any], *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        if saga["status"] != "ACTIVE":
            raise SagaError(f"Saga {saga_id} is {saga['status']}; steps require ACTIVE")
        if action_name not in self.actions:
            raise SagaError(f"Unknown action: {action_name}")
        step_id, timestamp = str(uuid.uuid4()), now()
        # Persist intent before the side effect. A crash therefore leaves an UNCERTAIN step
        # that rollback will compensate using the same idempotency key.
        self.store.create_step({"id": step_id, "saga_id": saga_id, "action": action_name, "input": values, "status": "EXECUTING", "result": None, "error": None, "compensation_attempts": 0, "created_at": timestamp, "updated_at": timestamp})
        try:
            result = self.actions[action_name].execute(values, saga_id, step_id)
            self.store.update_step(step_id, status="COMPLETED", result=result, updated_at=now())
            return self.store.get_step(step_id)  # type: ignore[return-value]
        except Exception as exc:
            self.store.update_step(step_id, status="FAILED", error=str(exc), updated_at=now())
            self.store.update_saga(saga_id, status="FAILED", error=str(exc), updated_at=now())
            self.rollback(saga_id, session_id=session_id)
            raise SagaError(f"Action {action_name} failed; rollback attempted: {exc}") from exc

    def commit(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        if saga["status"] != "ACTIVE":
            raise SagaError(f"Only ACTIVE sagas can commit (was {saga['status']})")
        self.store.update_saga(saga_id, status="COMMITTED", updated_at=now())
        return self.get(saga_id, session_id=session_id)

    def rollback(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        if saga["status"] in ("COMMITTED", "ROLLED_BACK"):
            raise SagaError(f"Saga {saga_id} is already {saga['status']}")
        self.store.update_saga(saga_id, status="ROLLING_BACK", updated_at=now())
        steps = self.store.list_steps(saga_id, {"COMPLETED", "EXECUTING", "FAILED", "COMPENSATION_FAILED"}, reverse=True)
        failures = []
        for raw in steps:
            step = raw
            action = self.actions.get(step["action"])
            if not action:
                failures.append(f"{step['id']}: action definition unavailable")
                continue
            last_error = None
            for attempt in range(self.compensation_retries):
                try:
                    action.compensate(step["input"], step["result"], saga_id, step["id"])
                    self.store.update_step(step["id"], status="COMPENSATED", compensation_attempts=attempt + 1, error=None, updated_at=now())
                    last_error = None
                    break
                except Exception as exc:
                    last_error = str(exc)
            if last_error:
                failures.append(f"{step['id']}: {last_error}")
                self.store.update_step(step["id"], status="COMPENSATION_FAILED", compensation_attempts=self.compensation_retries, error=last_error, updated_at=now())
        status = "ROLLBACK_FAILED" if failures else "ROLLED_BACK"
        self.store.update_saga(saga_id, status=status, error="; ".join(failures) or saga.get("error"), updated_at=now())
        return self.get(saga_id, session_id=session_id)

    def get(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        saga["steps"] = self.store.list_steps(saga_id)
        return saga

    def resume_pending_rollbacks(self) -> list[dict[str, Any]]:
        """Resume every rollback left incomplete by a previous process."""
        return [self.rollback(saga_id, session_id=session_id) for saga_id, session_id in self.store.pending_rollbacks()]

    def _require(self, saga_id: str, session_id: str) -> dict[str, Any]:
        row = self.store.get_saga(saga_id, session_id)
        if not row:
            raise SagaError(f"Saga not found: {saga_id}")
        return row
