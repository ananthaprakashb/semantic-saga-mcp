import unittest

from semantic_saga_mcp.coordinator import Coordinator, SagaError
from semantic_saga_mcp.server import McpServer
from semantic_saga_mcp.store import SagaStore


class FakeAction:
    def __init__(self, events, fail=False, rollback_fail=False):
        self.events, self.fail, self.rollback_fail = events, fail, rollback_fail

    def execute(self, values, saga_id, step_id):
        self.events.append(("do", values["value"]))
        if self.fail:
            raise RuntimeError("forward failed")
        return {"receipt": values["value"]}

    def compensate(self, values, result, saga_id, step_id):
        self.events.append(("undo", values["value"]))
        if self.rollback_fail:
            raise RuntimeError("undo failed")


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.actions = {"ok": FakeAction(self.events), "fail": FakeAction(self.events, fail=True)}
        self.coordinator = Coordinator(SagaStore(), self.actions, compensation_retries=2)

    def test_explicit_rollback_is_reverse_order(self):
        saga = self.coordinator.begin({"order": 7})
        self.coordinator.execute(saga["id"], "ok", {"value": 1})
        self.coordinator.execute(saga["id"], "ok", {"value": 2})
        result = self.coordinator.rollback(saga["id"])
        self.assertEqual(result["status"], "ROLLED_BACK")
        self.assertEqual(self.events, [("do", 1), ("do", 2), ("undo", 2), ("undo", 1)])

    def test_failed_step_automatically_rolls_back_prior_steps(self):
        saga_id = self.coordinator.begin()["id"]
        self.coordinator.execute(saga_id, "ok", {"value": 1})
        with self.assertRaisesRegex(SagaError, "rollback attempted"):
            self.coordinator.execute(saga_id, "fail", {"value": 2})
        result = self.coordinator.get(saga_id)
        self.assertEqual(result["status"], "ROLLED_BACK")
        # The failing request is compensated too: a timeout/error may happen after
        # the remote service applied its mutation.
        self.assertEqual(self.events, [("do", 1), ("do", 2), ("undo", 2), ("undo", 1)])

    def test_compensation_failure_is_visible_and_retried(self):
        self.actions["bad_undo"] = FakeAction(self.events, rollback_fail=True)
        saga_id = self.coordinator.begin()["id"]
        self.coordinator.execute(saga_id, "bad_undo", {"value": 1})
        result = self.coordinator.rollback(saga_id)
        self.assertEqual(result["status"], "ROLLBACK_FAILED")
        self.assertEqual(result["steps"][0]["compensation_attempts"], 2)

    def test_commit_prevents_rollback(self):
        saga_id = self.coordinator.begin()["id"]
        self.assertEqual(self.coordinator.commit(saga_id)["status"], "COMMITTED")
        with self.assertRaises(SagaError):
            self.coordinator.rollback(saga_id)

    def test_mcp_tools_round_trip(self):
        server = McpServer(self.coordinator)
        response = server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "begin_saga", "arguments": {}}})
        self.assertEqual(response["id"], 1)
        self.assertEqual(response["result"]["structuredContent"]["status"], "ACTIVE")

    def test_sessions_cannot_read_or_rollback_each_others_sagas(self):
        server = McpServer(self.coordinator)
        begin = server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "begin_saga", "arguments": {}}}, "agent-a")
        saga_id = begin["result"]["structuredContent"]["id"]
        response = server.dispatch({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "rollback_saga", "arguments": {"saga_id": saga_id}}}, "agent-b")
        self.assertEqual(response["error"]["code"], -32602)
        self.assertEqual(self.coordinator.get(saga_id, session_id="agent-a")["status"], "ACTIVE")

    def test_tool_arguments_are_strictly_validated_before_execution(self):
        server = McpServer(self.coordinator)
        response = server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "begin_saga", "arguments": {"metadata": "not-an-object"}}})
        self.assertEqual(response["error"]["code"], -32602)
        self.assertEqual(self.coordinator.store.one("SELECT COUNT(*) AS n FROM sagas")["n"], 0)

        response = server.dispatch({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "get_saga", "arguments": {"saga_id": "x", "unexpected": True}}})
        self.assertEqual(response["error"]["code"], -32602)


if __name__ == "__main__":
    unittest.main()
