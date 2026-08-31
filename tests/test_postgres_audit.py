import os
import unittest

from semantic_saga_mcp.instrumented_coordinator import InstrumentedCoordinator
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import PostgresSagaStore


DSN = os.getenv("SAGA_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "SAGA_TEST_POSTGRES_DSN is required")
class PostgresAuditTests(unittest.TestCase):
    def test_audit_chain_persists_across_postgres_store_instances(self):
        first_store = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=2)
        try:
            first = InstrumentedCoordinator(first_store, ActionRegistry(), worker_id="audit-first")
            saga_id = first.begin(tenant_id="audit-tenant", principal_id="alice")["id"]
            first.checkpoint(saga_id, "postgres-audit", {"private": "must-not-be-audited"}, principal_id="alice")
            first_events = first.get_audit_events(saga_id)["events"]
            self.assertGreaterEqual(len(first_events), 2)
            self.assertTrue(first.verify_audit_chain(saga_id)["valid"])
        finally:
            first_store.close()

        second_store = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=2)
        try:
            second = InstrumentedCoordinator(second_store, ActionRegistry(), worker_id="audit-second")
            events = second.get_audit_events(saga_id)["events"]
            self.assertEqual(
                [event["event_hash"] for event in first_events],
                [event["event_hash"] for event in events],
            )
            self.assertTrue(second.verify_audit_chain(saga_id)["valid"])
            self.assertNotIn("must-not-be-audited", str(events))
        finally:
            second_store.close()


if __name__ == "__main__":
    unittest.main()
