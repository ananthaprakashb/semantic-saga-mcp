import os
import unittest

from semantic_saga_mcp.governance import GovernedCoordinator
from semantic_saga_mcp.policy import JsonPolicyEngine, PolicySubject, policy_subject_scope
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import PostgresSagaStore


DSN = os.getenv("SAGA_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "SAGA_TEST_POSTGRES_DSN is required")
class PostgresGovernanceTests(unittest.TestCase):
    def test_policy_decisions_persist_and_remain_hash_chained(self):
        policy = JsonPolicyEngine(
            {
                "schema_version": 1,
                "revision": "postgres-policy-1",
                "default_effect": "allow",
                "tenants": {"*": {"default_effect": "allow", "budgets": {"max_planned_nodes": 1}}},
            }
        )
        subject = PolicySubject(
            tenant_id="pg-policy-tenant",
            principal_id="alice",
            principal_type="user",
            roles=("operator",),
            scopes=(),
        )
        first_store = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=2)
        try:
            first = GovernedCoordinator(first_store, ActionRegistry(), worker_id="policy-first", policy_engine=policy)
            with policy_subject_scope(subject):
                saga_id = first.begin(
                    tenant_id="pg-policy-tenant", principal_id="alice", session_id="pg-policy-owner"
                )["id"]
                # Commit produces a governance decision even when the saga has no planned nodes.
                committed = first.commit(saga_id, session_id="pg-policy-owner")
                self.assertEqual(committed["status"], "COMMITTED")
                decisions = first.get_policy_decisions(saga_id, session_id="pg-policy-owner")["events"]
                self.assertEqual(len(decisions), 1)
                self.assertEqual(decisions[0]["data"]["revision"], "postgres-policy-1")
                self.assertTrue(first.verify_audit_chain(saga_id, session_id="pg-policy-owner")["valid"])
                hashes = [event["event_hash"] for event in first.get_audit_events(saga_id, session_id="pg-policy-owner")["events"]]
        finally:
            first_store.close()

        second_store = PostgresSagaStore(DSN, min_pool_size=1, max_pool_size=2)
        try:
            second = GovernedCoordinator(second_store, ActionRegistry(), worker_id="policy-second", policy_engine=policy)
            with policy_subject_scope(subject):
                decisions = second.get_policy_decisions(saga_id, session_id="pg-policy-owner")["events"]
                self.assertEqual(decisions[0]["data"]["decision_id"], decisions[0]["data"]["decision_id"])
                self.assertTrue(second.verify_audit_chain(saga_id, session_id="pg-policy-owner")["valid"])
                self.assertEqual(
                    hashes,
                    [event["event_hash"] for event in second.get_audit_events(saga_id, session_id="pg-policy-owner")["events"]],
                )
        finally:
            second_store.close()


if __name__ == "__main__":
    unittest.main()
