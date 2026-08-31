import unittest

from mcp import Client

from semantic_saga_mcp.coordinator import Coordinator
from semantic_saga_mcp.execution import ExecutionContextResolver
from semantic_saga_mcp.mcp_server import build_mcp_server
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore


class McpAction:
    def __init__(self):
        self.calls = 0

    def execute(self, values, saga_id, step_id):
        self.calls += 1
        return {"ok": True, "name": values["name"]}

    def compensate(self, values, result, saga_id, step_id):
        return None


class WorkflowMcpTests(unittest.IsolatedAsyncioTestCase):
    async def test_plan_approve_checkpoint_and_run_through_mcp(self):
        action = McpAction()
        registry = ActionRegistry()
        registry.register_runtime(
            "deploy",
            action,
            version="1.0.0",
            input_schema={
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
                "additionalProperties": False,
            },
            output_schema={"type": "object"},
            execution_policy={"approval_required": True},
        )
        coordinator = Coordinator(SagaStore(), registry)
        server = build_mcp_server(
            coordinator,
            ExecutionContextResolver(local_owner_id="stdio:phase5-test"),
        )

        async with Client(server) as client:
            created = await client.call_tool("begin_saga", {"metadata": {"workflow": "deploy"}})
            saga_id = created.structured_content["id"]

            planned = await client.call_tool(
                "plan_saga_step",
                {"saga_id": saga_id, "action": "deploy", "input": {"name": "api"}, "key": "deploy-api"},
            )
            self.assertFalse(planned.is_error)
            node_id = planned.structured_content["id"]
            self.assertEqual(planned.structured_content["status"], "WAITING_APPROVAL")

            checkpoint = await client.call_tool(
                "checkpoint_saga",
                {"saga_id": saga_id, "name": "reviewed", "data": {"ticket": "CHG-7"}},
            )
            self.assertFalse(checkpoint.is_error)

            approved = await client.call_tool(
                "approve_saga_step",
                {"saga_id": saga_id, "node_id": node_id, "approved": True, "reason": "approved"},
            )
            self.assertFalse(approved.is_error)
            self.assertEqual(approved.structured_content["status"], "READY")
            self.assertEqual(approved.structured_content["approval"]["principal_id"], "local-process")

            executed = await client.call_tool(
                "run_ready_steps",
                {"saga_id": saga_id, "max_parallel": 2, "max_steps": 10},
            )
            self.assertFalse(executed.is_error)
            self.assertEqual(executed.structured_content["workflow"]["nodes"][0]["status"], "COMPLETED")
            self.assertEqual(executed.structured_content["workflow"]["checkpoints"][0]["name"], "reviewed")
            self.assertEqual(action.calls, 1)

            committed = await client.call_tool("commit_saga", {"saga_id": saga_id})
            self.assertFalse(committed.is_error)
            self.assertEqual(committed.structured_content["status"], "COMMITTED")


if __name__ == "__main__":
    unittest.main()
