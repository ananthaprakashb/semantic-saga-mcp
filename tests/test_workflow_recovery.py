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


def make_executing(store, saga_id, node, step_id):
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
        make_executing(store, saga_id, node, "uncertain-step")
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

    def test_mixed_inflight_wave_escalates_all_nodes_when_any_policy_pauses(self):
        pause_action = RecoverableAction()
        rollback_action = RecoverableAction()
        registry = ActionRegistry()
        registry.register_runtime(
            "pause_work",
            pause_action,
            version="1.0.0",
            execution_policy={"failure_mode": "pause"},
        )
        registry.register_runtime(
            "rollback_work",
            rollback_action,
            version="1.0.0",
            execution_policy={"failure_mode": "rollback"},
        )
        store = SagaStore()
        first = Coordinator(store, registry, worker_id="worker-before")
        saga_id = first.begin()["id"]
        pause_node = first.plan_step(saga_id, "pause_work", {"value": 1})
        rollback_node = first.plan_step(saga_id, "rollback_work", {"value": 2})

        saga = first.get(saga_id)
        nodes = saga["metadata"]["_engine"]["nodes"]
        make_executing(store, saga_id, nodes[pause_node["id"]], "pause-step")
        make_executing(store, saga_id, nodes[rollback_node["id"]], "rollback-step")
        store.update_saga(saga_id, metadata=saga["metadata"], updated_at=now())

        restarted = Coordinator(store, registry, worker_id="worker-after")
        recovered = restarted.resume_pending_rollbacks()
        self.assertEqual(len(recovered), 1)
        current = restarted.get(saga_id)
        self.assertEqual(current["status"], "RECOVERY_REQUIRED")
        by_id = {node["id"]: node for node in current["workflow"]["nodes"]}
        for node_id in (pause_node["id"], rollback_node["id"]):
            self.assertEqual(by_id[node_id]["status"], "FAILED")
            self.assertTrue(by_id[node_id]["uncertain_outcome"])
        self.assertEqual(pause_action.compensations, 0)
        self.assertEqual(rollback_action.compensations, 0)
        self.assertEqual({step["status"] for step in current["steps"]}, {"EXECUTING"})


if __name__ == "__main__":
    unittest.main()
