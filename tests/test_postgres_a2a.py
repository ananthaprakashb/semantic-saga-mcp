import importlib.util
import os
import unittest
import uuid


A2A_AVAILABLE = importlib.util.find_spec("a2a") is not None
DSN = os.getenv("SAGA_TEST_POSTGRES_DSN")


@unittest.skipUnless(A2A_AVAILABLE and DSN, "A2A extra and SAGA_TEST_POSTGRES_DSN are required")
class PostgresA2ATests(unittest.IsolatedAsyncioTestCase):
    async def test_a2a_task_persists_across_store_instances_and_tenants_are_isolated(self):
        from a2a.server.context import ServerCallContext
        from a2a.types import Task, TaskState, TaskStatus
        from semantic_saga_mcp.a2a_server import build_task_store

        task_id = "pg-a2a-" + uuid.uuid4().hex
        task = Task(
            id=task_id,
            context_id="ctx-" + uuid.uuid4().hex,
            status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        )
        acme = ServerCallContext(tenant="acme")
        other = ServerCallContext(tenant="other")

        first, first_engine = build_task_store(sqlite_path=None, postgres_dsn=DSN)
        await first.initialize()
        try:
            await first.save(task, acme)
            self.assertEqual((await first.get(task_id, acme)).id, task_id)
            self.assertIsNone(await first.get(task_id, other))
        finally:
            await first_engine.dispose()

        second, second_engine = build_task_store(sqlite_path=None, postgres_dsn=DSN)
        await second.initialize()
        try:
            restored = await second.get(task_id, acme)
            self.assertIsNotNone(restored)
            self.assertEqual(restored.status.state, TaskState.TASK_STATE_COMPLETED)
            self.assertIsNone(await second.get(task_id, other))
            await second.delete(task_id, acme)
        finally:
            await second_engine.dispose()


if __name__ == "__main__":
    unittest.main()
