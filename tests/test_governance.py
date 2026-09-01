from __future__ import annotations

import json
import unittest

from semantic_saga_mcp.coordinator import SagaError
from semantic_saga_mcp.governance import GovernedCoordinator
from semantic_saga_mcp.policy import JsonPolicyEngine, NoopPolicyEngine, PolicySubject, policy_subject_scope
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore


class CountingAction:
    def __init__(self) -> None:
        self.executions = 0
        self.compensations = 0

    def execute(self, values: dict, saga_id: str, step_id: str) -> dict:
        self.executions += 1
        return {"ok": True, "receipt": f"r-{self.executions}"}

    def compensate(self, values: dict, result: dict, saga_id: str, step_id: str) -> None:
        self.compensations += 1


def subject(*roles: str) -> PolicySubject:
    return PolicySubject(
        tenant_id="acme",
        principal_id="alice" if "admin" not in roles else "root",
        principal_type="user",
        roles=tuple(roles),
        scopes=(),
        authenticated=True,
    )


def engine(*, max_nodes: int = 10, threshold: str | None = "high", rules: list[dict] | None = None) -> JsonPolicyEngine:
    tenant: dict = {
        "default_effect": "allow",
        "budgets": {"max_steps_per_saga": 10, "max_planned_nodes": max_nodes, "max_risk_units": 50, "max_parallel": 4},
        "rules": rules or [],
    }
    if threshold is not None:
        tenant["approval_at_or_above_risk"] = threshold
    return JsonPolicyEngine(
        {
            "schema_version": 1,
            "revision": "g7",
            "default_effect": "deny",
            "tenants": {"*": tenant},
        }
    )


class GovernanceCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = SagaStore()
        self.registry = ActionRegistry()
        self.high = CountingAction()
        self.low = CountingAction()
        self.critical = CountingAction()
        for name, action, risk in (
            ("high_change", self.high, "high"),
            ("low_change", self.low, "low"),
            ("critical_change", self.critical, "critical"),
        ):
            self.registry.register_runtime(
                name,
                action,
                version="1.0.0",
                semantic={"domain": "test", "operation": "change", "resource": name, "reversibility": "full", "risk": risk},
                input_schema={"type": "object", "additionalProperties": True},
                output_schema={"type": "object", "required": ["ok", "receipt"]},
            )
        self.coordinator = GovernedCoordinator(self.store, self.registry, policy_engine=engine())
        with policy_subject_scope(subject("operator")):
            self.saga = self.coordinator.begin({}, session_id="tenant-acme", tenant_id="acme", principal_id="alice")

    def test_high_risk_plan_adds_approval_gate(self) -> None:
        with policy_subject_scope(subject("operator")):
            node = self.coordinator.plan_step(
                self.saga["id"], "high_change", {"sentinel": "SECRET-PAYLOAD"}, session_id="tenant-acme"
            )
            self.assertTrue(node["approval_required"])
            self.assertEqual(node["status"], "WAITING_APPROVAL")
            approved = self.coordinator.approve_step(
                self.saga["id"], node["id"], session_id="tenant-acme", principal_id="alice", approved=True
            )
            self.assertEqual(approved["status"], "READY")
            result = self.coordinator.run_ready_steps(self.saga["id"], session_id="tenant-acme")
            self.assertEqual(self.high.executions, 1)
            self.assertEqual(result["workflow"]["nodes"][0]["status"], "COMPLETED")

    def test_immediate_high_risk_execution_requires_planned_approval(self) -> None:
        with policy_subject_scope(subject("operator")):
            with self.assertRaisesRegex(SagaError, "requires approval"):
                self.coordinator.execute(self.saga["id"], "high_change", {}, session_id="tenant-acme")
        self.assertEqual(self.high.executions, 0)
        self.assertEqual(self.store.list_steps(self.saga["id"]), [])

    def test_budget_denies_second_planned_node(self) -> None:
        coordinator = GovernedCoordinator(self.store, self.registry, policy_engine=engine(max_nodes=1, threshold=None))
        with policy_subject_scope(subject("operator")):
            saga = coordinator.begin({}, session_id="tenant-budget", tenant_id="acme", principal_id="alice")
            coordinator.plan_step(saga["id"], "low_change", {}, session_id="tenant-budget")
            with self.assertRaisesRegex(SagaError, "planned-node budget"):
                coordinator.plan_step(saga["id"], "low_change", {}, session_id="tenant-budget")

    def test_policy_is_re_evaluated_before_ready_node_runs(self) -> None:
        coordinator = GovernedCoordinator(self.store, self.registry, policy_engine=engine(threshold=None))
        with policy_subject_scope(subject("operator")):
            saga = coordinator.begin({}, session_id="tenant-refresh", tenant_id="acme", principal_id="alice")
            node = coordinator.plan_step(saga["id"], "low_change", {}, session_id="tenant-refresh")
            self.assertEqual(node["status"], "READY")
            coordinator.policy_engine = engine(
                threshold=None,
                rules=[
                    {
                        "id": "stop-low-at-run",
                        "effect": "deny",
                        "match": {"actions": ["low_change"], "phases": ["run"]},
                        "reason": "Policy changed before execution",
                    }
                ],
            )
            with self.assertRaisesRegex(SagaError, "Policy denied"):
                coordinator.run_ready_steps(saga["id"], session_id="tenant-refresh")
        self.assertEqual(self.low.executions, 0)

    def test_critical_approval_can_require_admin(self) -> None:
        coordinator = GovernedCoordinator(
            self.store,
            self.registry,
            policy_engine=engine(
                threshold="critical",
                rules=[
                    {
                        "id": "critical-approval-admin",
                        "effect": "deny",
                        "match": {"risks": ["critical"], "phases": ["approve"], "roles_none": ["admin"]},
                        "reason": "Critical approval requires admin",
                    }
                ],
            ),
        )
        with policy_subject_scope(subject("operator")):
            saga = coordinator.begin({}, session_id="tenant-admin", tenant_id="acme", principal_id="alice")
            node = coordinator.plan_step(saga["id"], "critical_change", {}, session_id="tenant-admin")
            with self.assertRaisesRegex(SagaError, "Critical approval requires admin"):
                coordinator.approve_step(saga["id"], node["id"], session_id="tenant-admin", principal_id="alice")
        with policy_subject_scope(subject("admin")):
            approved = coordinator.approve_step(saga["id"], node["id"], session_id="tenant-admin", principal_id="root")
            self.assertEqual(approved["status"], "READY")

    def test_policy_audit_does_not_store_action_payload(self) -> None:
        with policy_subject_scope(subject("operator")):
            self.coordinator.plan_step(
                self.saga["id"], "high_change", {"token": "NEVER-IN-POLICY-AUDIT"}, session_id="tenant-acme"
            )
            decisions = self.coordinator.get_policy_decisions(self.saga["id"], session_id="tenant-acme")
        rendered = json.dumps(decisions, sort_keys=True)
        self.assertNotIn("NEVER-IN-POLICY-AUDIT", rendered)
        self.assertIn("POLICY_DECISION", rendered)
        self.assertTrue(self.coordinator.verify_audit_chain(self.saga["id"], session_id="tenant-acme")["valid"])

    def test_rollback_remains_available_after_policy_turns_deny(self) -> None:
        coordinator = GovernedCoordinator(self.store, self.registry, policy_engine=NoopPolicyEngine())
        with policy_subject_scope(subject("operator")):
            saga = coordinator.begin({}, session_id="tenant-rollback", tenant_id="acme", principal_id="alice")
            coordinator.execute(saga["id"], "low_change", {}, session_id="tenant-rollback")
            coordinator.policy_engine = JsonPolicyEngine(
                {"schema_version": 1, "revision": "deny-all", "default_effect": "deny", "tenants": {"*": {"default_effect": "deny"}}}
            )
            rolled = coordinator.rollback(saga["id"], session_id="tenant-rollback")
            self.assertEqual(rolled["status"], "ROLLED_BACK")
            self.assertEqual(self.low.compensations, 1)
            evidence = coordinator.get_policy_decisions(saga["id"], session_id="tenant-rollback")
            self.assertTrue(any(event["event_type"] == "POLICY_SAFETY_OVERRIDE" for event in evidence["events"]))

    def test_policy_status_is_tenant_scoped(self) -> None:
        with policy_subject_scope(subject("viewer")):
            status = self.coordinator.get_policy_status()
            self.assertEqual(status["tenant_id"], "acme")
            with self.assertRaisesRegex(SagaError, "caller's tenant"):
                self.coordinator.get_policy_status("other")
        with policy_subject_scope(subject("admin")):
            other = self.coordinator.get_policy_status("other")
            self.assertEqual(other["tenant_id"], "other")


if __name__ == "__main__":
    unittest.main()
