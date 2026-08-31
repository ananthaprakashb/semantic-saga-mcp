import unittest

from semantic_saga_mcp.coordinator import Coordinator, SagaError, now
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore


class RecoverableAction:
    def __init__(self):
        self.compensations = 0

    def execute(self, values, saga_id, step_id):
        return {"ok": True}

    def compensate(self, values, result, saga_id, step_id):
        self.compensations += 1


class WorkflowRecoveryTests(unittest.TestCase):
    def test_pause_node_with_uncertain_restart_requires_operator_force(self):
        action = RecoverableAction()
        registry = ActionRegistry()
        registry.register_runtime(
            "work",
            action,
            version="1.0.0",
            execution_policy={"failure_mode": "pause"},
        )
        store = SagaStore()
        first = Coordinator(store, registry, worker_id="worker-before")
        saga_id = first.begin()["id"]
        planned = first.plan_step(saga_id, "work", {"value": 1})

        saga = first.get(saga_id)
        node = saga["metadata"]["_engine"]["nodes"][planned["id"]]
        step_id = "uncertain-step"
        timestamp = now()
        store.create_step(
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
            }
        )
        node["step_id"] = step_id
        node["status"] = "EXECUTING"
        store.update_saga(saga_id, metadata=saga["metadata"], updated_at=now())

        restarted = Coordinator(store, registry, worker_id="worker-after")
        recovered = restarted.resume_pending_rollbacks()
        self.assertEqual(len(recovered), 1)
        current = restarted.get(saga_id)
        workflow_node = current["workflow"]["nodes"][0]
        self.assertEqual(current["status"], "RECOVERY_REQUIRED")
        self.assertEqual(workflow_node["status"], "FAILED")
        self.assertTrue(workflow_node["uncertain_outcome"])
        self.assertEqual(action.compensations, 0)

        with self.assertRaisesRegex(SagaError, "uncertain external outcome"):
            restarted.retry_step(saga_id, planned["id"])
        reset = restarted.retry_step(saga_id, planned["id"], force=True)
        self.assertEqual(reset["status"], "READY")


if __name__ == "__main__":
    unittest.main()
