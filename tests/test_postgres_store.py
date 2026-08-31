import os
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from semantic_saga_mcp.store import LeaseLostError, PostgresSagaStore


DSN = os.getenv("SAGA_TEST_POSTGRES_DSN")


def saga_record(status: str = "ACTIVE"):
    timestamp = "2026-08-30T00:00:00+00:00"
    return {
        "id": str(uuid.uuid4()),
        "session_id": "tenant:acme",
        "tenant_id": "acme",
        "creator_principal_id": "alice",
        "status": status,
        "metadata": {"test": True},
        "created_at": timestamp,
        "updated_at": timestamp,
        "error": None,
    }


def action_snapshot():
    return {
        "id": "test",
        "version": "1.2.3",
        "kind": "http",
        "semantic": {"domain": "test", "risk": "low"},
        "input_schema": {"type": "object"},
        "forward": {"url": "https://example.test/do"},
        "compensation": {"url": "https://example.test/undo"},
    }


@unittest.skipUnless(DSN, "SAGA_TEST_POSTGRES_DSN is not configured")
class PostgresSagaStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=12)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        with self.store._pool.connection() as conn:
            conn.execute("DELETE FROM steps")
            conn.execute("DELETE FROM sagas")
            conn.commit()

    def test_explicit_tenant_and_creator_are_first_class_columns(self):
        record = saga_record()
        self.store.create_saga(record)
        loaded = self.store.get_saga(record["id"], record["session_id"])
        self.assertEqual(loaded["tenant_id"], "acme")
        self.assertEqual(loaded["creator_principal_id"], "alice")

    def test_action_contract_fields_round_trip_as_first_class_step_data(self):
        record = saga_record()
        self.store.create_saga(record)
        token = self.store.acquire_saga_lease(record["id"], record["session_id"], "worker-a", 10.0)
        snapshot = action_snapshot()
        stored = self.store.create_step(
            {
                "id": str(uuid.uuid4()),
                "saga_id": record["id"],
                "action": "test",
                "action_version": "1.2.3",
                "action_definition_hash": "a" * 64,
                "action_definition": snapshot,
                "input": {"number": 1},
                "status": "EXECUTING",
                "result": None,
                "error": None,
                "compensation_attempts": 0,
                "created_at": record["created_at"],
                "updated_at": record["updated_at"],
            },
            fence_token=token,
        )
        self.assertEqual(stored["action_version"], "1.2.3")
        self.assertEqual(stored["action_definition_hash"], "a" * 64)
        self.assertEqual(stored["action_definition"], snapshot)

        restarted = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=2)
        try:
            loaded = restarted.get_step(stored["id"])
            self.assertEqual(loaded["action_definition"], snapshot)
            self.assertEqual(loaded["action_version"], "1.2.3")
        finally:
            restarted.close()

    def test_step_sequence_allocation_is_atomic_under_concurrency(self):
        record = saga_record()
        self.store.create_saga(record)
        token = self.store.acquire_saga_lease(record["id"], record["session_id"], "worker-a", 10.0)
        self.assertIsNotNone(token)

        timestamp = record["created_at"]

        def create(number: int):
            return self.store.create_step(
                {
                    "id": str(uuid.uuid4()),
                    "saga_id": record["id"],
                    "action": "test",
                    "action_version": "1.2.3",
                    "action_definition_hash": "a" * 64,
                    "action_definition": action_snapshot(),
                    "input": {"number": number},
                    "status": "EXECUTING",
                    "result": None,
                    "error": None,
                    "compensation_attempts": 0,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                fence_token=token,
            )

        with ThreadPoolExecutor(max_workers=12) as pool:
            rows = list(pool.map(create, range(24)))

        self.assertEqual(sorted(row["sequence"] for row in rows), list(range(1, 25)))
        self.assertEqual(
            [row["sequence"] for row in self.store.list_steps(record["id"])],
            list(range(1, 25)),
        )

    def test_new_lease_fences_a_stale_worker(self):
        record = saga_record()
        self.store.create_saga(record)
        first = self.store.acquire_saga_lease(record["id"], record["session_id"], "worker-a", 0.15)
        self.assertIsNotNone(first)
        time.sleep(0.2)
        second = self.store.acquire_saga_lease(record["id"], record["session_id"], "worker-b", 2.0)
        self.assertIsNotNone(second)
        self.assertGreater(second, first)

        with self.assertRaises(LeaseLostError):
            self.store.update_saga(record["id"], fence_token=first, status="COMMITTED")
        self.store.update_saga(record["id"], fence_token=second, status="FAILED")
        self.assertEqual(self.store.get_saga(record["id"], record["session_id"])["status"], "FAILED")

    def test_skip_locked_recovery_claim_prevents_duplicate_workers(self):
        record = saga_record(status="FAILED")
        self.store.create_saga(record)
        second_store = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=2)
        try:
            first_claims = self.store.claim_pending_rollbacks("worker-a", 5.0, 10)
            second_claims = second_store.claim_pending_rollbacks("worker-b", 5.0, 10)
            self.assertEqual(len(first_claims), 1)
            self.assertEqual(second_claims, [])

            saga_id, _, token = first_claims[0]
            self.store.release_saga_lease(saga_id, "worker-a", token)
            reclaimed = second_store.claim_pending_rollbacks("worker-b", 5.0, 10)
            self.assertEqual(len(reclaimed), 1)
            self.assertGreater(reclaimed[0][2], token)
        finally:
            second_store.close()


if __name__ == "__main__":
    unittest.main()
