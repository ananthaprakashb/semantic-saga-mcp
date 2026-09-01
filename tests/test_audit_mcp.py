import json
import unittest

from mcp import Client

from semantic_saga_mcp.execution import ExecutionContextResolver
from semantic_saga_mcp.instrumented_coordinator import InstrumentedCoordinator
from semantic_saga_mcp.mcp_server import build_mcp_server
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore


class McpAuditAction:
    def execute(self, values, saga_id, step_id):
        return {"receipt": "mcp-result-private", "ok": True}

    def compensate(self, values, result, saga_id, step_id):
        return None


class AuditMcpTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeline_audit_and_integrity_tools_round_trip(self):
        registry = ActionRegistry()
        registry.register_runtime(
            "audit-action",
            McpAuditAction(),
            version="1.0.0",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
        )
        coordinator = InstrumentedCoordinator(SagaStore(), registry)
        server = build_mcp_server(
            coordinator,
            ExecutionContextResolver(local_owner_id="stdio:phase6-test"),
        )

        async with Client(server) as client:
            created = await client.call_tool("begin_saga", {"metadata": {"case": "phase6"}})
            self.assertFalse(created.is_error)
            saga_id = created.structured_content["id"]

            executed = await client.call_tool(
                "execute_saga_step",
                {"saga_id": saga_id, "action": "audit-action", "input": {"secret": "mcp-input-private"}},
            )
            self.assertFalse(executed.is_error)

            timeline = await client.call_tool("get_saga_timeline", {"saga_id": saga_id})
            self.assertFalse(timeline.is_error)
            encoded = json.dumps(timeline.structured_content, sort_keys=True)
            self.assertNotIn("mcp-input-private", encoded)
            self.assertNotIn("mcp-result-private", encoded)
            self.assertGreaterEqual(len(timeline.structured_content["audit"]), 3)

            audit = await client.call_tool(
                "get_audit_events",
                {"saga_id": saga_id, "event_types": ["ACTION_COMPLETED"]},
            )
            self.assertFalse(audit.is_error)
            self.assertEqual(
                [event["event_type"] for event in audit.structured_content["events"]],
                ["ACTION_COMPLETED"],
            )

            integrity = await client.call_tool("verify_audit_chain", {"saga_id": saga_id})
            self.assertFalse(integrity.is_error)
            self.assertTrue(integrity.structured_content["valid"])
            self.assertIsNotNone(integrity.structured_content["head_hash"])


if __name__ == "__main__":
    unittest.main()
