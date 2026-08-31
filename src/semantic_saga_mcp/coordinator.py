from __future__ import annotations

import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any

from .actions import Action
from .engine import dependency_status, engine_state, retry_delay_seconds
from .registry import ActionDefinition, ActionRegistry, ActionRegistryError, RegisteredAction
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
        actions: ActionRegistry | dict[str, Action],
        compensation_retries: int = 3,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.store = store
        self._legacy_actions: dict[str, Action] | None = None
        if isinstance(actions, ActionRegistry):
            self.registry = actions
            self.actions = {
                item["id"]: self.registry.resolve(item["id"]).action
                for item in self.registry.list_definitions()
            }
        else:
            self._legacy_actions = actions
            self.registry = ActionRegistry(allow_legacy_recovery=True)
            for name, action in actions.items():
                self.registry.register_runtime(name, action)
            self.actions = actions
        self.compensation_retries = compensation_retries
        self.worker_id = worker_id or f"worker:{uuid.uuid4()}"
        self.lease_seconds = lease_seconds

    def _sync_legacy_action(self, action_name: str) -> None:
        if self._legacy_actions is None:
            return
        action = self._legacy_actions.get(action_name)
        if action is None:
            return
        try:
            current = self.registry.resolve(action_name)
        except ActionRegistryError:
            current = None
        if current is None or current.action is not action:
            self.registry.register_runtime(action_name, action)

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

    def _invoke_forward(
        self,
        registered: RegisteredAction,
        values: dict[str, Any],
        saga_id: str,
        step_id: str,
        lease: _LeaseGuard,
    ) -> tuple[bool, Any, str | None, int]:
        policy = registered.definition.policy["forward"]
        last_error: str | None = None
        result: Any = None
        result_available = False
        attempts = int(policy["max_attempts"])
        for attempt in range(1, attempts + 1):
            try:
                lease.check()
                result = registered.action.execute(values, saga_id, step_id)
                result_available = True
                lease.check()
                self.registry.validate_output(registered.definition, result)
                return True, result, None, attempt
            except LeaseLostError:
                raise
            except Exception as exc:
                last_error = str(exc)
                if attempt < attempts:
                    delay = retry_delay_seconds(policy, attempt, f"{saga_id}:{step_id}:forward")
                    if delay:
                        time.sleep(delay)
        return False, result if result_available else None, last_error, attempts

    def execute(self, saga_id: str, action_name: str, values: dict[str, Any], *, session_id: str = "default") -> dict[str, Any]:
        failure: str | None = None
        failure_mode = "rollback"
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] != "ACTIVE":
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; steps require ACTIVE")
                self._sync_legacy_action(action_name)
                try:
                    registered = self.registry.prepare(action_name, values)
                except ActionRegistryError as exc:
                    raise SagaError(str(exc)) from exc
                definition = registered.definition
                failure_mode = definition.policy["failure_mode"]
                assert lease.token is not None
                step_id, timestamp = str(uuid.uuid4()), now()
                self.store.create_step(
                    {
                        "id": step_id,
                        "saga_id": saga_id,
                        "action": action_name,
                        "action_version": definition.version,
                        "action_definition_hash": definition.hash,
                        "action_definition": definition.snapshot(),
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
                success, result, error, _attempts = self._invoke_forward(
                    registered, values, saga_id, step_id, lease
                )
                if success:
                    self.store.update_step(
                        step_id,
                        fence_token=lease.token,
                        status="COMPLETED",
                        result=result,
                        error=None,
                        updated_at=now(),
                    )
                    return self.store.get_step(step_id)  # type: ignore[return-value]
                failure = error or "action failed"
                changes: dict[str, Any] = {"status": "FAILED", "error": failure, "updated_at": now()}
                if result is not None:
                    changes["result"] = result
                self.store.update_step(step_id, fence_token=lease.token, **changes)
                self.store.update_saga(
                    saga_id,
                    fence_token=lease.token,
                    status="RECOVERY_REQUIRED" if failure_mode == "pause" else "FAILED",
                    error=failure,
                    updated_at=now(),
                )
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

        assert failure is not None
        if failure_mode == "pause":
            raise SagaError(f"Action {action_name} failed; saga paused for operator recovery: {failure}")
        try:
            self.rollback(saga_id, session_id=session_id)
        except SagaError as rollback_error:
            raise SagaError(
                f"Action {action_name} failed; rollback could not complete: {failure}; {rollback_error}"
            ) from rollback_error
        raise SagaError(f"Action {action_name} failed; rollback attempted: {failure}")

    def _save_engine(self, saga: dict[str, Any], state: dict[str, Any], token: int) -> None:
        metadata = dict(saga.get("metadata") or {})
        metadata["_engine"] = state
        self.store.update_saga(saga["id"], fence_token=token, metadata=metadata, updated_at=now())
        saga["metadata"] = metadata

    @staticmethod
    def _node_public(node: dict[str, Any]) -> dict[str, Any]:
        value = dict(node)
        value.pop("action_definition", None)
        return value

    def plan_step(
        self,
        saga_id: str,
        action_name: str,
        values: dict[str, Any],
        *,
        session_id: str = "default",
        key: str | None = None,
        depends_on: list[str] | None = None,
        approval_required: bool | None = None,
    ) -> dict[str, Any]:
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] != "ACTIVE":
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; planning requires ACTIVE")
                self._sync_legacy_action(action_name)
                try:
                    registered = self.registry.prepare(action_name, values)
                except ActionRegistryError as exc:
                    raise SagaError(str(exc)) from exc
                state = engine_state(saga.get("metadata") or {})
                nodes: dict[str, dict[str, Any]] = state["nodes"]
                dependencies = list(dict.fromkeys(depends_on or []))
                missing = [item for item in dependencies if item not in nodes]
                if missing:
                    raise SagaError(f"Unknown dependency node(s): {', '.join(missing)}")
                if key is not None:
                    if not key.strip():
                        raise SagaError("Step key must be non-empty")
                    if any(node.get("key") == key for node in nodes.values()):
                        raise SagaError(f"Duplicate step key: {key}")
                definition = registered.definition
                required = definition.policy["approval_required"] if approval_required is None else approval_required
                if not isinstance(required, bool):
                    raise SagaError("approval_required must be a boolean")
                node_id = str(uuid.uuid4())
                node: dict[str, Any] = {
                    "id": node_id,
                    "key": key,
                    "action": action_name,
                    "action_version": definition.version,
                    "action_definition_hash": definition.hash,
                    "action_definition": definition.snapshot(),
                    "input": values,
                    "depends_on": dependencies,
                    "status": "WAITING_DEPENDENCY" if dependencies else ("WAITING_APPROVAL" if required else "READY"),
                    "approval_required": required,
                    "approval": {"status": "PENDING"} if required else None,
                    "step_id": None,
                    "attempts": 0,
                    "result": None,
                    "error": None,
                    "uncertain_outcome": False,
                    "planned_at": now(),
                    "updated_at": now(),
                }
                nodes[node_id] = node
                assert lease.token is not None
                self._refresh_nodes(state)
                self._save_engine(saga, state, lease.token)
                return self._node_public(nodes[node_id])
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

    def _refresh_nodes(self, state: dict[str, Any]) -> None:
        nodes: dict[str, dict[str, Any]] = state["nodes"]
        for node in nodes.values():
            if node.get("status") not in {"WAITING_DEPENDENCY", "BLOCKED"}:
                continue
            dep = dependency_status(node, nodes)
            if dep == "blocked":
                node["status"] = "BLOCKED"
                node["error"] = "A dependency did not complete successfully"
            elif dep == "waiting":
                node["status"] = "WAITING_DEPENDENCY"
                node["error"] = None
            else:
                approved = not node.get("approval_required") or (node.get("approval") or {}).get("status") == "APPROVED"
                node["status"] = "READY" if approved else "WAITING_APPROVAL"
                node["error"] = None
            node["updated_at"] = now()

    def approve_step(
        self,
        saga_id: str,
        node_id: str,
        *,
        session_id: str = "default",
        approved: bool = True,
        reason: str | None = None,
        principal_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] not in {"ACTIVE", "RECOVERY_REQUIRED"}:
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; approval is unavailable")
                state = engine_state(saga.get("metadata") or {})
                node = state["nodes"].get(node_id)
                if node is None:
                    raise SagaError(f"Workflow node not found: {node_id}")
                if not node.get("approval_required"):
                    raise SagaError(f"Workflow node {node_id} does not require approval")
                if node.get("status") not in {"WAITING_APPROVAL", "REJECTED"}:
                    raise SagaError(f"Workflow node {node_id} is {node.get('status')}; approval is not pending")
                node["approval"] = {
                    "status": "APPROVED" if approved else "REJECTED",
                    "principal_id": principal_id,
                    "reason": reason,
                    "decided_at": now(),
                }
                if approved:
                    node["status"] = "READY" if dependency_status(node, state["nodes"]) == "ready" else "WAITING_DEPENDENCY"
                    node["error"] = None
                    if saga["status"] == "RECOVERY_REQUIRED":
                        self.store.update_saga(saga_id, fence_token=lease.token, status="ACTIVE", error=None, updated_at=now())
                        saga["status"] = "ACTIVE"
                else:
                    node["status"] = "REJECTED"
                    node["error"] = reason or "Approval rejected"
                    self.store.update_saga(
                        saga_id,
                        fence_token=lease.token,
                        status="RECOVERY_REQUIRED",
                        error=f"Approval rejected for workflow node {node_id}",
                        updated_at=now(),
                    )
                    saga["status"] = "RECOVERY_REQUIRED"
                node["updated_at"] = now()
                assert lease.token is not None
                self._save_engine(saga, state, lease.token)
                return self._node_public(node)
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

    def retry_step(
        self,
        saga_id: str,
        node_id: str,
        *,
        session_id: str = "default",
        force: bool = False,
    ) -> dict[str, Any]:
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] not in {"ACTIVE", "RECOVERY_REQUIRED"}:
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; retry is unavailable")
                state = engine_state(saga.get("metadata") or {})
                node = state["nodes"].get(node_id)
                if node is None:
                    raise SagaError(f"Workflow node not found: {node_id}")
                if node.get("status") not in {"FAILED", "BLOCKED", "REJECTED"}:
                    raise SagaError(f"Workflow node {node_id} is {node.get('status')}; only failed/rejected/blocked nodes can retry")
                if node.get("uncertain_outcome") and not force:
                    raise SagaError("Node has an uncertain external outcome; use force only after operator reconciliation")
                if node.get("approval_required") and (node.get("approval") or {}).get("status") != "APPROVED":
                    node["approval"] = {"status": "PENDING"}
                    node["status"] = "WAITING_APPROVAL"
                else:
                    dep = dependency_status(node, state["nodes"])
                    node["status"] = "READY" if dep == "ready" else "WAITING_DEPENDENCY"
                node["error"] = None
                node["uncertain_outcome"] = False
                node["updated_at"] = now()
                assert lease.token is not None
                self.store.update_saga(saga_id, fence_token=lease.token, status="ACTIVE", error=None, updated_at=now())
                saga["status"] = "ACTIVE"
                self._refresh_nodes(state)
                self._save_engine(saga, state, lease.token)
                return self._node_public(node)
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

    def checkpoint(
        self,
        saga_id: str,
        name: str,
        data: dict[str, Any] | None = None,
        *,
        session_id: str = "default",
        principal_id: str | None = None,
    ) -> dict[str, Any]:
        if not name.strip():
            raise SagaError("Checkpoint name must be non-empty")
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] not in {"ACTIVE", "RECOVERY_REQUIRED"}:
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; checkpoint is unavailable")
                state = engine_state(saga.get("metadata") or {})
                checkpoint = {
                    "id": str(uuid.uuid4()),
                    "name": name,
                    "data": data or {},
                    "principal_id": principal_id,
                    "created_at": now(),
                }
                state["checkpoints"].append(checkpoint)
                assert lease.token is not None
                self._save_engine(saga, state, lease.token)
                return checkpoint
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

    def _registered_for_node(self, node: dict[str, Any]) -> RegisteredAction:
        self._sync_legacy_action(str(node["action"]))
        try:
            return self.registry.resolve_for_step(node)
        except ActionRegistryError as exc:
            raise SagaError(str(exc)) from exc

    @staticmethod
    def _persisted_policy_for_node(node: dict[str, Any]) -> dict[str, Any] | None:
        snapshot = node.get("action_definition")
        if not isinstance(snapshot, dict):
            return None
        try:
            return ActionDefinition.from_snapshot(snapshot).policy
        except ActionRegistryError:
            return None

    def run_ready_steps(
        self,
        saga_id: str,
        *,
        session_id: str = "default",
        max_parallel: int = 4,
        max_steps: int = 100,
    ) -> dict[str, Any]:
        if not 1 <= max_parallel <= 32:
            raise SagaError("max_parallel must be between 1 and 32")
        if not 1 <= max_steps <= 1000:
            raise SagaError("max_steps must be between 1 and 1000")
        rollback_after = False
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] != "ACTIVE":
                    raise SagaError(f"Saga {saga_id} is {saga['status']}; ready-step execution requires ACTIVE")
                assert lease.token is not None
                state = engine_state(saga.get("metadata") or {})
                budget = max_steps
                while budget > 0:
                    self._refresh_nodes(state)
                    ready = [node for node in state["nodes"].values() if node.get("status") == "READY"]
                    if not ready:
                        self._save_engine(saga, state, lease.token)
                        break
                    wave = ready[: min(max_parallel, budget)]
                    prepared: list[tuple[dict[str, Any], RegisteredAction, str]] = []
                    for node in wave:
                        lease.check()
                        registered = self._registered_for_node(node)
                        try:
                            self.registry._validate(registered.definition.input_schema, node["input"], label=f"{node['action']} input")
                        except ActionRegistryError as exc:
                            node["status"] = "FAILED"
                            node["error"] = str(exc)
                            node["updated_at"] = now()
                            continue
                        step_id = node.get("step_id")
                        if step_id:
                            existing = self.store.get_step(step_id)
                            if existing is None:
                                node["status"] = "FAILED"
                                node["error"] = "Persisted workflow step is missing"
                                node["updated_at"] = now()
                                continue
                            self.store.update_step(step_id, fence_token=lease.token, status="EXECUTING", error=None, updated_at=now())
                        else:
                            step_id = str(uuid.uuid4())
                            timestamp = now()
                            self.store.create_step(
                                {
                                    "id": step_id,
                                    "saga_id": saga_id,
                                    "action": node["action"],
                                    "action_version": node["action_version"],
                                    "action_definition_hash": node["action_definition_hash"],
                                    "action_definition": node["action_definition"],
                                    "input": node["input"],
                                    "status": "EXECUTING",
                                    "result": None,
                                    "error": None,
                                    "compensation_attempts": 0,
                                    "created_at": timestamp,
                                    "updated_at": timestamp,
                                },
                                fence_token=lease.token,
                            )
                            node["step_id"] = step_id
                        node["status"] = "EXECUTING"
                        node["updated_at"] = now()
                        prepared.append((node, registered, step_id))
                    self._save_engine(saga, state, lease.token)
                    if not prepared:
                        break

                    results: dict[str, tuple[bool, Any, str | None, int]] = {}
                    with ThreadPoolExecutor(max_workers=min(max_parallel, len(prepared))) as pool:
                        futures = {
                            pool.submit(self._invoke_forward, registered, node["input"], saga_id, step_id, lease): (node, step_id)
                            for node, registered, step_id in prepared
                        }
                        for future in as_completed(futures):
                            node, _step_id = futures[future]
                            try:
                                results[node["id"]] = future.result()
                            except LeaseLostError:
                                raise
                            except Exception as exc:
                                results[node["id"]] = (False, None, str(exc), 1)

                    failures: list[dict[str, Any]] = []
                    for node, _registered, step_id in prepared:
                        success, result, error, attempts = results[node["id"]]
                        node["attempts"] = int(node.get("attempts", 0)) + attempts
                        node["updated_at"] = now()
                        if success:
                            node["status"] = "COMPLETED"
                            node["result"] = result
                            node["error"] = None
                            node["uncertain_outcome"] = False
                            self.store.update_step(
                                step_id,
                                fence_token=lease.token,
                                status="COMPLETED",
                                result=result,
                                error=None,
                                updated_at=now(),
                            )
                        else:
                            node["status"] = "FAILED"
                            node["result"] = result
                            node["error"] = error or "action failed"
                            node["uncertain_outcome"] = False
                            changes: dict[str, Any] = {"status": "FAILED", "error": node["error"], "updated_at": now()}
                            if result is not None:
                                changes["result"] = result
                            self.store.update_step(step_id, fence_token=lease.token, **changes)
                            failures.append(node)
                    budget -= len(prepared)
                    self._refresh_nodes(state)
                    self._save_engine(saga, state, lease.token)
                    if failures:
                        rollback_after = any(
                            (self._persisted_policy_for_node(node) or {"failure_mode": "pause"})["failure_mode"] == "rollback"
                            for node in failures
                        )
                        status = "FAILED" if rollback_after else "RECOVERY_REQUIRED"
                        message = "; ".join(f"{node['id']}: {node['error']}" for node in failures)
                        self.store.update_saga(saga_id, fence_token=lease.token, status=status, error=message, updated_at=now())
                        saga["status"] = status
                        saga["error"] = message
                        break
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc

        if rollback_after:
            return self.rollback(saga_id, session_id=session_id)
        return self.get(saga_id, session_id=session_id)

    def commit(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        try:
            with self._lease(saga_id, session_id) as lease:
                saga = self._require(saga_id, session_id)
                if saga["status"] != "ACTIVE":
                    raise SagaError(f"Only ACTIVE sagas can commit (was {saga['status']})")
                state = engine_state(saga.get("metadata") or {})
                incomplete = [node["id"] for node in state["nodes"].values() if node.get("status") != "COMPLETED"]
                if incomplete:
                    raise SagaError(f"Saga has incomplete workflow nodes: {', '.join(incomplete)}")
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
                    self._sync_legacy_action(step["action"])
                    try:
                        registered = self.registry.resolve_for_step(step)
                    except ActionRegistryError as exc:
                        message = str(exc)
                        failures.append(f"{step['id']}: {message}")
                        self.store.update_step(
                            step["id"],
                            fence_token=lease.token,
                            status="COMPENSATION_FAILED",
                            error=message,
                            updated_at=now(),
                        )
                        continue
                    policy = registered.definition.policy["compensation"]
                    attempts = int(policy["max_attempts"])
                    if registered.definition.execution_policy is None:
                        attempts = self.compensation_retries
                    last_error = None
                    completed_attempts = 0
                    for attempt in range(1, attempts + 1):
                        completed_attempts = attempt
                        try:
                            lease.check()
                            registered.action.compensate(step["input"], step["result"], saga_id, step["id"])
                            lease.check()
                            self.store.update_step(
                                step["id"],
                                fence_token=lease.token,
                                status="COMPENSATED",
                                compensation_attempts=attempt,
                                error=None,
                                updated_at=now(),
                            )
                            last_error = None
                            break
                        except LeaseLostError:
                            raise
                        except Exception as exc:
                            last_error = str(exc)
                            if attempt < attempts:
                                delay = retry_delay_seconds(policy, attempt, f"{saga_id}:{step['id']}:compensation")
                                if delay:
                                    time.sleep(delay)
                    if last_error:
                        failures.append(f"{step['id']}: {last_error}")
                        self.store.update_step(
                            step["id"],
                            fence_token=lease.token,
                            status="COMPENSATION_FAILED",
                            compensation_attempts=completed_attempts,
                            error=last_error,
                            updated_at=now(),
                        )
                status = "ROLLBACK_FAILED" if failures else "ROLLED_BACK"
                metadata = dict(saga.get("metadata") or {})
                try:
                    state = engine_state(metadata)
                except Exception:
                    state = None
                if state is not None and "_engine" in metadata:
                    step_rows = {row["id"]: row for row in self.store.list_steps(saga_id)}
                    for node in state["nodes"].values():
                        step_id = node.get("step_id")
                        if step_id in step_rows:
                            step_status = step_rows[step_id]["status"]
                            if step_status == "COMPENSATED":
                                node["status"] = "COMPENSATED"
                            elif step_status == "COMPENSATION_FAILED":
                                node["status"] = "COMPENSATION_FAILED"
                            node["updated_at"] = now()
                    metadata["_engine"] = state
                self.store.update_saga(
                    saga_id,
                    fence_token=lease.token,
                    status=status,
                    metadata=metadata,
                    error="; ".join(failures) or saga.get("error"),
                    updated_at=now(),
                )
        except LeaseLostError as exc:
            raise SagaError(str(exc)) from exc
        return self.get(saga_id, session_id=session_id)

    def get(self, saga_id: str, *, session_id: str = "default") -> dict[str, Any]:
        saga = self._require(saga_id, session_id)
        saga["steps"] = self.store.list_steps(saga_id)
        metadata = saga.get("metadata") or {}
        if isinstance(metadata, dict) and "_engine" in metadata:
            state = engine_state(metadata)
            saga["workflow"] = {
                "version": state["version"],
                "nodes": [self._node_public(node) for node in state["nodes"].values()],
                "checkpoints": state["checkpoints"],
            }
        return saga

    def list_actions(self) -> dict[str, Any]:
        return {"actions": self.registry.list_definitions()}

    def get_action(self, action_id: str, version: str | None = None) -> dict[str, Any]:
        try:
            return self.registry.get_definition(action_id, version)
        except ActionRegistryError as exc:
            raise SagaError(str(exc)) from exc

    def resume_pending_rollbacks(self, limit: int = 100) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        claims = self.store.claim_pending_rollbacks(self.worker_id, self.lease_seconds, limit)
        for saga_id, session_id, token in claims:
            try:
                saga = self._require(saga_id, session_id)
                metadata = saga.get("metadata") or {}
                state = engine_state(metadata) if isinstance(metadata, dict) and "_engine" in metadata else None
                executing_nodes: list[dict[str, Any]] = []
                if state is not None:
                    executing_nodes = [node for node in state["nodes"].values() if node.get("status") == "EXECUTING"]

                # A crash makes every in-flight node's remote outcome uncertain.
                # If any member of the wave requires pause semantics—or if its
                # persisted policy cannot be proven—do not auto-compensate only
                # part of that wave. Escalate every in-flight node for operator
                # reconciliation so no sibling is stranded as EXECUTING.
                requires_operator = bool(executing_nodes) and any(
                    (self._persisted_policy_for_node(node) or {"failure_mode": "pause"})["failure_mode"] == "pause"
                    for node in executing_nodes
                )
                if requires_operator:
                    for node in executing_nodes:
                        node["status"] = "FAILED"
                        node["error"] = "Worker restarted while external outcome was uncertain"
                        node["uncertain_outcome"] = True
                        node["updated_at"] = now()
                    metadata = dict(metadata)
                    metadata["_engine"] = state
                    self.store.update_saga(
                        saga_id,
                        fence_token=token,
                        status="RECOVERY_REQUIRED",
                        metadata=metadata,
                        error="Operator reconciliation required for uncertain workflow outcome",
                        updated_at=now(),
                    )
                    self.store.release_saga_lease(saga_id, self.worker_id, token)
                    results.append(self.get(saga_id, session_id=session_id))
                else:
                    results.append(self._rollback(saga_id, session_id=session_id, claimed_token=token))
            except SagaError:
                continue
        return results

    def _require(self, saga_id: str, session_id: str) -> dict[str, Any]:
        row = self.store.get_saga(saga_id, session_id)
        if not row:
            raise SagaError(f"Saga not found: {saga_id}")
        return row
