from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from semantic_saga_mcp.policy import JsonPolicyEngine, OpaPolicyEngine, PolicyError


class JsonPolicyEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = JsonPolicyEngine(
            {
                "schema_version": 1,
                "revision": "policy-7",
                "default_effect": "deny",
                "risk_weights": {"low": 1, "medium": 2, "high": 5, "critical": 10, "unknown": 3},
                "tenants": {
                    "*": {
                        "default_effect": "allow",
                        "budgets": {
                            "max_steps_per_saga": 3,
                            "max_planned_nodes": 3,
                            "max_risk_units": 8,
                            "max_parallel": 2
                        },
                        "approval_at_or_above_risk": "high",
                        "rules": [
                            {
                                "id": "deny-irreversible",
                                "effect": "deny",
                                "match": {"risks": ["critical"], "resources": ["irreversible_*"]},
                                "reason": "Irreversible critical resources are blocked"
                            }
                        ]
                    },
                    "acme": {
                        "rules": [
                            {
                                "id": "admin-only-prod-delete",
                                "effect": "deny",
                                "match": {
                                    "actions": ["delete_prod_*"],
                                    "phases": ["execute", "plan", "run"],
                                    "roles_none": ["admin"]
                                },
                                "reason": "Production deletion requires admin"
                            }
                        ]
                    }
                }
            }
        )

    @staticmethod
    def context(*, action: str = "deploy_app", risk: str = "low", resource: str = "service", roles: list[str] | None = None, phase: str = "plan", request: dict | None = None) -> dict:
        return {
            "tenant_id": "acme",
            "principal": {"id": "alice", "type": "user", "roles": roles or ["operator"], "scopes": []},
            "phase": phase,
            "action": {
                "id": action,
                "version": "1.0.0",
                "kind": "runtime",
                "semantic": {"risk": risk, "domain": "cloud", "operation": "change", "resource": resource},
            },
            "saga": {"id": "s1", "status": "ACTIVE", "steps": 0, "planned_nodes": 0, "risk_units": 0},
            "request": request or {},
        }

    def test_high_risk_requires_approval(self) -> None:
        decision = self.engine.decide(self.context(risk="high"))
        self.assertEqual(decision.effect, "require_approval")
        approved = self.engine.decide(self.context(risk="high", request={"approval_granted": True}))
        self.assertEqual(approved.effect, "allow")

    def test_budget_is_hard_limit(self) -> None:
        decision = self.engine.decide(self.context(request={"prospective_risk_units": 9}))
        self.assertEqual(decision.effect, "deny")
        self.assertIn("risk-unit budget", decision.reason)

    def test_tenant_rule_precedes_wildcard_default(self) -> None:
        denied = self.engine.decide(self.context(action="delete_prod_database", roles=["operator"]))
        self.assertEqual(denied.effect, "deny")
        self.assertEqual(denied.matched_rules, ("admin-only-prod-delete",))
        allowed = self.engine.decide(self.context(action="delete_prod_database", roles=["admin"]))
        self.assertEqual(allowed.effect, "allow")

    def test_wildcard_deny(self) -> None:
        decision = self.engine.decide(self.context(risk="critical", resource="irreversible_account"))
        self.assertEqual(decision.effect, "deny")
        self.assertEqual(decision.matched_rules, ("deny-irreversible",))

    def test_status_is_effective_tenant_view(self) -> None:
        status = self.engine.status("acme")
        self.assertEqual(status["revision"], "policy-7")
        self.assertEqual(status["budgets"]["max_parallel"], 2)
        self.assertIn("admin-only-prod-delete", status["rule_ids"])
        self.assertIn("deny-irreversible", status["rule_ids"])

    def test_invalid_policy_rejected(self) -> None:
        with self.assertRaises(PolicyError):
            JsonPolicyEngine({"schema_version": 1, "default_effect": "maybe", "tenants": {}})


class _OpaHandler(BaseHTTPRequestHandler):
    response_document: dict = {"result": {"effect": "allow", "reason": "ok", "revision": "bundle-42", "matched_rules": ["rego.allow"]}}
    seen_input: dict | None = None

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        document = json.loads(self.rfile.read(length))
        type(self).seen_input = document.get("input")
        payload = json.dumps(type(self).response_document).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args: object) -> None:
        return


class OpaPolicyEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _OpaHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.engine = OpaPolicyEngine(f"http://{host}:{port}", decision_path="semantic_saga/decision")

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_opa_object_decision_and_sanitized_context(self) -> None:
        context = JsonPolicyEngineTests.context()
        decision = self.engine.decide(context)
        self.assertEqual(decision.effect, "allow")
        self.assertEqual(decision.revision, "bundle-42")
        self.assertEqual(_OpaHandler.seen_input, context)
        self.assertNotIn("input", context["action"])

    def test_opa_undefined_fails_closed(self) -> None:
        previous = _OpaHandler.response_document
        try:
            _OpaHandler.response_document = {}
            decision = self.engine.decide(JsonPolicyEngineTests.context())
            self.assertEqual(decision.effect, "deny")
        finally:
            _OpaHandler.response_document = previous


if __name__ == "__main__":
    unittest.main()
