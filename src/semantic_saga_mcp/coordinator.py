from __future__ import annotations

import threading
import uuid
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any

from .actions import Action
from .store import LeaseLostError, SagaStoreProtocol


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SagaError(RuntimeError):
    pass


class _LeaseGuard(AbstractContextManager["_LeaseGuard"]):
    def __init__(
        self,
        store: SagaStoreProtocol,
        saga_id: str,
        session_id: str,
        worker_id: str,
        lease_seconds: float,
        token: int | None = None,
    ) -> None:
        self.store = store
        self.saga_id = saga_id
        self.session_id = session_id
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.token = token
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_LeaseGuard":
        if self.token is None:
            self.token = self.store.acquire_saga_lease(
                self.saga_id,
                self.session_id,
                self.worker_id,
                self.lease_seconds,
            )
            if self.token is None:
                raise SagaError(f"Saga {self.saga_id} is busy on another worker; retry")
        interval = max(0.05, self.lease_seconds / 3)
        self._thread = threading.Thread(target=self._heartbeat, args=(interval,), daemon=True)
        self._thread.start()
        return self

    def _heartbeat(self, interval: float) -> None:
        while not self._stop.wait(interval):
            try:
                assert self.token is not None
                renewed = self.store.renew_saga_lease(
                    self.saga_id,
                    self.worker_id,
                    self.token,
                    self.lease_seconds,
                )
            except Exception:
                renewed = False
            if not renewed:
                self._lost.set()
                return

    def check(self) -> None:
        if self._lost.is_set():
            raise LeaseLostError(
                f"Worker {self.worker_id} lost lease for saga {self.saga_id}; outcome may be uncertain"
            )

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(0.1, self.lease_seconds))
        if self.token is not None and not self._lost.is_set():
            self.store.release_saga_lease(self.saga_id, self.worker_id, self.token)
        return None


class Coordinator:
    def __init__(
        self,
        store: SagaStoreProtocol,
        actions: dict[str, Action],
        compensation_retries: int = 3,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.store = store
        self.actions = actions
        self.compensation_retries = compensation_retries
        self.worker_id = worker_id or f"worker:{uuid.uuid4()}"
        self.lease_seconds = lease_seconds

    def begin(
        self,
        metadata: dict[str, Any] | None = None,
        *,
        session_id: str = "default",
        tenant_id: str | None = None,
        principal_id: str | None = None,
    ) -> dict[str, Any]:
        saga_id, timestamp = str(uuid.uuid4()), now()
        self.store.create_saga(
            {
                "id": saga_id,
                "session_id": session_id,
                "tenant_id": tenant_id,
                "creator_principal_id": principal_id,
                "status": "ACTIVE",
                "metadata": metadata or {},
                "created_at": timestamp,
                "updated_at": timestamp,
                "error": None,
            }
        )
        return self.get(saga_id, session_id=session_id)

    def _lease(self, saga_id: str, session_id: str, token: int | None = None) -> _LeaseGuard:
        return _LeaseGuard(
            self.store,
            saga_id,
            session_id,
            self.worker_id,
            self.lease_seconds,
            token=token,
        )

    def execute(self, saga_id: str, action_name: str, values: dict[str, Any], *, session_id: str = "default") -> dict[str, Any]:
        failure: Exception | None = None
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] != "ACTIVE":
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; steps require ACTIVE")
                if action_name not in self.actions:
                    raise SagaError(f"Unknown action: {action_name}")
                assert lease.token is not None
                step_id, timestamp = str(uuid.uuid4()), now()
                self.store.create_step(
                    {
                        "id": step_id,
                        "saga_id": saga_id,
                        "action": action_name,
                        "input": values,
                        "status": "EXECUTING",
                        "result": None,
                        "error": None,
                        "compensation_attempts": 0,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    },
                    fence_token=lease.token,
                )
                try:
                    lease.check()
                    result = self.actions[action_name].execute(values, saga_id, step_id)
                    lease.check()
                except LeaseLostError:
                    raise
                except Exception as exc:
                    lease.check()
                    self.store.update_step(
                        step_id,
                        fence_token=lease.token,
                        status="FAILED",
                        error=str(exc),
                        updated_at=now(),
                    )
                    self.store.update_saga(
                        saga_id,
                        fence_token=lease.token,
                        status="FAILED",
                        error=str(exc),
                        updated_at=now(),
                    )
                    failure = exc
                else:
                    self.store.update_step(
                        step_id,
                        fence_token=lease.token,
                        status="COMPLETED",
                        result=result,
                        updated_at=now(),
                    )
                    return self.store.get_step(step_id)  # type: ignore[return-value]
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

        assert failure is not None
        try:
            self.rollback(saga_id, session_id=session_id)
        except SagaError as rollback_error:
            raise SagaError(
                f"Action {action_name} failed; rollback could not complete: {failure}; {rollback_error}"
            ) from failure
        raise SagaError(f"Action {action_name} failed; rollback attempted: {failure}") from failure

    def commit(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] != "ACTIVE":
                    raise SagaError(f"Only ACTIVE sagas can commit (was {saga['status']})")
                lease.check()
                assert lease.token is not None
                self.store.update_saga(
                    saga_id,
                    fence_token=lease.token,
                    status="COMMITTED",
                    updated_at=now(),
                )
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc
        return self.get(saga_id, session_id=session_id)

    def rollback(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        return self._rollback(saga_id, session_id=session_id, claimed_token=None)

    def _rollback(self, saga_id: str, *, session_id: str, claimed_token: int | None) -> dict[str, Any]:
        try:
            with self._lease(saga_id, session_id, token=claimed_token) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] in ("COMMITTED", "ROLLED_BACK"):
                    raise SagaError(f"Saga {saga_id} is already {saga['status']}")
                assert lease.token is not None
                self.store.update_saga(
                    saga_id,
                    fence_token=lease.token,
                    status="ROLLING_BACK",
                    updated_at=now(),
                )
                steps = self.store.list_steps(
                    saga_id,
                    {"COMPLETED", "EXECUTING", "FAILED", "COMPENSATION_FAILED"},
                    reverse=True,
                )
                failures = []
                for step in steps:
                    lease.check()
                    action = self.actions.get(step["action"])
                    if not action:
                        failures.append(f"{step['id']}: action definition unavailable")
                        continue
                    last_error = None
                    for attempt in range(self.compensation_retries):
                        try:
                            lease.check()
                            action.compensate(step["input"], step["result"], saga_id, step["id"])
                            lease.check()
                            self.store.update_step(
                                step["id"],
                                fence_token=lease.token,
                                status="COMPENSATED",
                                compensation_attempts=attempt + 1,
                                error=None,
                                updated_at=now(),
                            )
                            last_error = None
                            break
                        except LeaseLostError:
                            raise
                        except Exception as exc:
                            last_error = str(exc)
                    if last_error:
                        failures.append(f"{step['id']}: {last_error}")
                        self.store.update_step(
                            step["id"],
                            fence_token=lease.token,
                            status="COMPENSATION_FAILED",
                            compensation_attempts=self.compensation_retries,
                            error=last_error,
                            updated_at=now(),
                        )
                status = "ROLLBACK_FAILED" if failures else "ROLLED_BACK"
                self.store.update_saga(
                    saga_id,
                    fence_token=lease.token,
                    status=status,
                    error="; ".join(failures) or saga.get("error"),
                    updated_at=now(),
                )
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc
        return self.get(saga_id, session_id=session_id)

    def get(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        saga["steps"] = self.store.list_steps(saga_id)
        return saga

    def resume_pending_rollbacks(self, limit: int = 100) -> list[dict[str, Any]]:
        """Claim and resume recovery work without duplicate processing across workers."""
        results: list[dict[str, Any]] = []
        claims = self.store.claim_pending_rollbacks(self.worker_id, self.lease_seconds, limit)
        for saga_id, session_id, token in claims:
            try:
                results.append(self._rollback(saga_id, session_id=session_id, claimed_token=token))
            except SagaError:
                # Lease loss or an operator-fixable rollback error remains durable
                # and can be claimed by a later recovery pass.
                continue
        return results

    def _require(self, saga_id: str, session_id: str) -> dict[str, Any]:
        row = self.store.get_saga(saga_id, session_id)
        if not row:
            raise SagaError(f"Saga not found: {saga_id}")
        return row
