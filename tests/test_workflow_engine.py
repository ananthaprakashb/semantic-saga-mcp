import tempfile
import threading
import time
import unittest
from pathlib import Path

from semantic_saga_mcp.coordinator import Coordinator, SagaError
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore, SQLiteSagaStore


class FlakyAction:
    def __init__(self, fail_count=0):
        self.fail_count = fail_count
        self.calls = 0
        self.step_ids = []
        self.compensations = 0

    def execute(self, values, saga_id, step_id):
        self.calls += 1
        self.step_ids.append(step_id)
        if self.calls <= self.fail_count:
            raise RuntimeError(f"forward failure {self.calls}")
        return {"value": values.get("value", self.calls)}

    def compensate(self, values, result, saga_id, step_id):
        self.compensations += 1


class CompensationFlakyAction(FlakyAction):
    def compensate(self, values, result, saga_id, step_id):
        self.compensations += 1
        if self.compensations < 3:
            raise RuntimeError("compensation retry")


class ParallelAction:
    def __init__(self):
        self.barrier = threading.Barrier(2)
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.completed = []

    def execute(self, values, saga_id, step_id):
        phase = values["phase"]
        if phase in {"a", "b"}:
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            self.barrier.wait(timeout=2)
            time.sleep(0.02)
            with self.lock:
                self.active -= 1
        with self.lock:
            self.completed.append(phase)
        return {"phase": phase}

    def compensate(self, values, result, saga_id, step_id):
        return None


def runtime_registry(action, *, policy=None, approval=False):
    registry = ActionRegistry()
    effective = dict(policy or {})
    if approval:
        effective["approval_required"] = True
    registry.register_runtime(
        "work",
        action,
        version="1.0.0",
        input_schema={
            "type": "object",
            "properties": {
                "value": {},
                "phase": {"type": "string"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        execution_policy=effective or None,
    )
    return registry


class WorkflowEngineTests(unittest.TestCase):
    def test_forward_retry_reuses_one_step_and_idempotency_identity(self):
        action = FlakyAction(fail_count=2)
        registry = runtime_registry(
            action,
            policy={
                "forward": {
                    "max_attempts": 3,
                    "initial_backoff_seconds": 0,
                    "jitter": 0,
                }
            },
        )
        coordinator = Coordinator(SagaStore(), registry)
        saga_id = coordinator.begin()["id"]
        node = coordinator.plan_step(saga_id, "work", {"value": 7})
        result = coordinator.run_ready_steps(saga_id)

        self.assertEqual(result["status"], "ACTIVE")
        self.assertEqual(result["workflow"]["nodes"][0]["status"], "COMPLETED")
        self.assertEqual(result["workflow"]["nodes"][0]["attempts"], 3)
        self.assertEqual(action.calls, 3)
        self.assertEqual(len(set(action.step_ids)), 1)
        self.assertEqual(result["workflow"]["nodes"][0]["id"], node["id"])
        self.assertEqual(len(result["steps"]), 1)

    def test_dependency_waves_run_independent_nodes_in_parallel(self):
        action = ParallelAction()
        coordinator = Coordinator(SagaStore(), runtime_registry(action))
        saga_id = coordinator.begin()["id"]
        first = coordinator.plan_step(saga_id, "work", {"phase": "a"}, key="a")
        second = coordinator.plan_step(saga_id, "work", {"phase": "b"}, key="b")
        third = coordinator.plan_step(
            saga_id,
            "work",
            {"phase": "c"},
            key="c",
            depends_on=[first["id"], second["id"]],
        )

        result = coordinator.run_ready_steps(saga_id, max_parallel=2)
        nodes = {node["id"]: node for node in result["workflow"]["nodes"]}
        self.assertEqual(action.max_active, 2)
        self.assertEqual(nodes[first["id"]]["status"], "COMPLETED")
        self.assertEqual(nodes[second["id"]]["status"], "COMPLETED")
        self.assertEqual(nodes[third["id"]]["status"], "COMPLETED")
        self.assertEqual(action.completed[-1], "c")

    def test_approval_gate_and_checkpoint_are_durable(self):
        action = FlakyAction()
        coordinator = Coordinator(SagaStore(), runtime_registry(action, approval=True))
        saga_id = coordinator.begin()["id"]
        node = coordinator.plan_step(saga_id, "work", {"value": 1})
        self.assertEqual(node["status"], "WAITING_APPROVAL")
        coordinator.run_ready_steps(saga_id)
        self.assertEqual(action.calls, 0)

        approved = coordinator.approve_step(
            saga_id,
            node["id"],
            approved=True,
            reason="change ticket approved",
            principal_id="alice",
        )
        self.assertEqual(approved["status"], "READY")
        self.assertEqual(approved["approval"]["principal_id"], "alice")
        checkpoint = coordinator.checkpoint(saga_id, "pre-deploy", {"ticket": "CHG-42"}, principal_id="alice")
        result = coordinator.run_ready_steps(saga_id)
        self.assertEqual(result["workflow"]["nodes"][0]["status"], "COMPLETED")
        self.assertEqual(result["workflow"]["checkpoints"][0]["id"], checkpoint["id"])

    def test_pause_failure_requires_operator_retry_then_can_commit(self):
        action = FlakyAction(fail_count=2)
        registry = runtime_registry(
            action,
            policy={
                "failure_mode": "pause",
                "forward": {"max_attempts": 2, "initial_backoff_seconds": 0},
            },
        )
        coordinator = Coordinator(SagaStore(), registry)
        saga_id = coordinator.begin()["id"]
        node = coordinator.plan_step(saga_id, "work", {"value": 9})
        paused = coordinator.run_ready_steps(saga_id)
        self.assertEqual(paused["status"], "RECOVERY_REQUIRED")
        self.assertEqual(paused["workflow"]["nodes"][0]["status"], "FAILED")
        self.assertEqual(paused["workflow"]["nodes"][0]["attempts"], 2)
        self.assertEqual(action.compensations, 0)

        retried = coordinator.retry_step(saga_id, node["id"])
        self.assertEqual(retried["status"], "READY")
        finished = coordinator.run_ready_steps(saga_id)
        self.assertEqual(finished["workflow"]["nodes"][0]["status"], "COMPLETED")
        committed = coordinator.commit(saga_id)
        self.assertEqual(committed["status"], "COMMITTED")
        self.assertEqual(len(committed["steps"]), 1)
        self.assertEqual(action.step_ids[0], action.step_ids[-1])

    def test_explicit_compensation_policy_overrides_coordinator_default(self):
        action = CompensationFlakyAction()
        registry = runtime_registry(
            action,
            policy={
                "compensation": {"max_attempts": 3, "initial_backoff_seconds": 0}
            },
        )
        coordinator = Coordinator(SagaStore(), registry, compensation_retries=1)
        saga_id = coordinator.begin()["id"]
        coordinator.execute(saga_id, "work", {"value": 1})
        result = coordinator.rollback(saga_id)
        self.assertEqual(result["status"], "ROLLED_BACK")
        self.assertEqual(action.compensations, 3)
        self.assertEqual(result["steps"][0]["compensation_attempts"], 3)

    def test_commit_rejects_incomplete_planned_nodes(self):
        coordinator = Coordinator(SagaStore(), runtime_registry(FlakyAction(), approval=True))
        saga_id = coordinator.begin()["id"]
        coordinator.plan_step(saga_id, "work", {"value": 1})
        with self.assertRaisesRegex(SagaError, "incomplete workflow nodes"):
            coordinator.commit(saga_id)

    def test_sqlite_restart_preserves_plan_approval_and_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "engine.db")
            action = FlakyAction()
            registry = runtime_registry(action, approval=True)
            first = Coordinator(SQLiteSagaStore(path), registry)
            saga_id = first.begin()["id"]
            node = first.plan_step(saga_id, "work", {"value": 5}, key="durable")
            first.checkpoint(saga_id, "planned", {"node": node["id"]})

            restarted = Coordinator(SQLiteSagaStore(path), registry)
            loaded = restarted.get(saga_id)
            self.assertEqual(loaded["workflow"]["nodes"][0]["status"], "WAITING_APPROVAL")
            self.assertEqual(loaded["workflow"]["nodes"][0]["key"], "durable")
            self.assertEqual(loaded["workflow"]["checkpoints"][0]["name"], "planned")
            restarted.approve_step(saga_id, node["id"], principal_id="operator")
            completed = restarted.run_ready_steps(saga_id)
            self.assertEqual(completed["workflow"]["nodes"][0]["status"], "COMPLETED")


if __name__ == "__main__":
    unittest.main()
