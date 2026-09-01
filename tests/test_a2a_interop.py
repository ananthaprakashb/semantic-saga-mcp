import importlib.util
import tempfile
import unittest
from pathlib import Path


A2A_AVAILABLE = importlib.util.find_spec("a2a") is not None


@unittest.skipUnless(A2A_AVAILABLE, "install semantic-saga-mcp[a2a]")
class A2AInteropTests(unittest.IsolatedAsyncioTestCase):
    def test_agent_card_advertises_a2a_1_0_without_streaming(self):
        from semantic_saga_mcp.a2a_server import build_agent_card

        card = build_agent_card("https://saga.example.com", "/a2a", auth_required=True)
        self.assertEqual(card.supported_interfaces[0].protocol_binding, "JSONRPC")
        self.assertEqual(card.supported_interfaces[0].protocol_version, "1.0")
        self.assertEqual(card.supported_interfaces[0].url, "https://saga.example.com/a2a")
        self.assertFalse(card.capabilities.streaming)
        self.assertFalse(card.capabilities.push_notifications)
        self.assertEqual(list(card.default_input_modes), ["application/json"])
        self.assertEqual(list(card.default_output_modes), ["application/json"])
        self.assertIn("bearer", card.security_schemes)
        self.assertEqual(card.security_schemes["bearer"].http_auth_security_scheme.scheme, "Bearer")
        self.assertGreaterEqual(len(card.skills), 3)

    async def test_sqlite_task_store_persists_and_is_tenant_scoped(self):
        from a2a.server.context import ServerCallContext
        from a2a.types import Task, TaskState, TaskStatus
        from semantic_saga_mcp.a2a_server import build_task_store

        with tempfile.TemporaryDirectory() as temp:
            db = str(Path(temp) / "semantic-saga.db")
            store, engine = build_task_store(sqlite_path=db, postgres_dsn=None)
            await store.initialize()
            try:
                task = Task(
                    id="task-a2a-1",
                    context_id="context-a2a-1",
                    status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
                )
                acme = ServerCallContext(tenant="acme")
                other = ServerCallContext(tenant="other")
                await store.save(task, acme)
                restored = await store.get(task.id, acme)
                self.assertIsNotNone(restored)
                self.assertEqual(restored.id, task.id)
                self.assertEqual(restored.status.state, TaskState.TASK_STATE_COMPLETED)
                self.assertIsNone(await store.get(task.id, other))
                self.assertTrue(Path(db).exists())
            finally:
                await engine.dispose()


if __name__ == "__main__":
    unittest.main()
