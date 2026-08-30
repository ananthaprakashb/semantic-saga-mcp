import tempfile
import time
import unittest
import uuid
from pathlib import Path

from semantic_saga_mcp.store import LeaseLostError, SagaStore, SQLiteSagaStore


def saga_record(saga_id: str = "saga-1", session_id: str = "tenant-owner"):
    timestamp = "2026-08-30T00:00:00+00:00"
    return {
        "id": saga_id,
        "session_id": session_id,
        "tenant_id": "acme",
        "creator_principal_id": "alice",
        "status": "ACTIVE",
        "metadata": {},
        "created_at": timestamp,
        "updated_at": timestamp,
        "error": None,
    }


class LeaseContractMixin:
    def make_store(self):
        raise NotImplementedError

    def test_active_lease_serializes_workers_and_fences_stale_writes(self):
        store = self.make_store()
        record = saga_record(str(uuid.uuid4()))
        store.create_saga(record)

        first = store.acquire_saga_lease(record["id"], record["session_id"], "worker-a", 0.15)
        self.assertIsNotNone(first)
        self.assertIsNone(
            store.acquire_saga_lease(record["id"], record["session_id"], "worker-b", 1.0)
        )

        store.update_saga(record["id"], fence_token=first, status="FAILED")
        time.sleep(0.2)
        second = store.acquire_saga_lease(record["id"], record["session_id"], "worker-b", 1.0)
        self.assertIsNotNone(second)
        self.assertGreater(second, first)

        with self.assertRaises(LeaseLostError):
            store.update_saga(record["id"], fence_token=first, status="COMMITTED")
        store.update_saga(record["id"], fence_token=second, status="ROLLING_BACK")
        self.assertEqual(store.get_saga(record["id"], record["session_id"])["status"], "ROLLING_BACK")

    def test_pending_recovery_is_claimed_once(self):
        store = self.make_store()
        record = saga_record(str(uuid.uuid4()))
        record["status"] = "FAILED"
        store.create_saga(record)

        first = store.claim_pending_rollbacks("worker-a", 2.0)
        second = store.claim_pending_rollbacks("worker-b", 2.0)
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])


class InMemoryLeaseTests(LeaseContractMixin, unittest.TestCase):
    def make_store(self):
        return SagaStore()


class SQLiteLeaseTests(LeaseContractMixin, unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "sagas.db")

    def tearDown(self):
        self.directory.cleanup()

    def make_store(self):
        return SQLiteSagaStore(self.path)

    def test_explicit_tenant_and_creator_fields_survive_restart(self):
        store = self.make_store()
        record = saga_record(str(uuid.uuid4()))
        store.create_saga(record)

        restarted = SQLiteSagaStore(self.path)
        loaded = restarted.get_saga(record["id"], record["session_id"])
        self.assertEqual(loaded["tenant_id"], "acme")
        self.assertEqual(loaded["creator_principal_id"], "alice")


if __name__ == "__main__":
    unittest.main()
