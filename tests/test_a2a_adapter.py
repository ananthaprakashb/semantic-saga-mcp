import unittest
from types import SimpleNamespace

from pydantic import ValidationError

from semantic_saga_mcp.a2a_adapter import A2ACommand, SemanticSagaA2AExecutor, tenant_owner_id
from semantic_saga_mcp.auth import AuthorizationError
from semantic_saga_mcp.coordinator import SagaError
from semantic_saga_mcp.governance import GovernedCoordinator
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore


class A2AAdapterTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = GovernedCoordinator(SagaStore(), ActionRegistry(), worker_id="a2a-unit")
        self.executor = SemanticSagaA2AExecutor(self.coordinator)
        self.context = SimpleNamespace(context_id="ctx-1", task_id="task-1")
        self.operator = {
            "tenant_id": "acme",
            "principal_id": "agent-a",
            "principal_type": "service",
            "roles": ("operator",),
            "scopes": (),
            "authenticated": True,
        }

    def test_command_requires_operation_specific_fields(self):
        with self.assertRaises(ValidationError):
            A2ACommand.model_validate({"operation": "get"})
        with self.assertRaises(ValidationError):
            A2ACommand.model_validate({"operation": "execute", "saga_id": "saga-1"})
        with self.assertRaises(ValidationError):
            A2ACommand.model_validate(
                {"operation": "plan", "saga_id": "saga-1", "action": "x", "unknown": True}
            )

    def test_tenant_owner_is_stable_and_tenant_scoped(self):
        self.assertEqual(tenant_owner_id("acme"), tenant_owner_id("acme"))
        self.assertNotEqual(tenant_owner_id("acme"), tenant_owner_id("other"))
        self.assertTrue(tenant_owner_id("acme").startswith("tenant:"))

    def test_same_tenant_peer_can_continue_explicit_saga(self):
        created = self.executor._dispatch(
            A2ACommand(operation="begin", metadata={"case": "a2a"}), self.operator, self.context
        )
        saga_id = created["id"]
        self.assertEqual(created["tenant_id"], "acme")
        self.assertEqual(created["metadata"]["_identity"]["principal_id"], "agent-a")
        self.assertEqual(created["metadata"]["_interop"]["protocol"], "a2a")

        second_agent = dict(self.operator, principal_id="agent-b")
        fetched = self.executor._dispatch(A2ACommand(operation="get", saga_id=saga_id), second_agent, self.context)
        self.assertEqual(fetched["id"], saga_id)

    def test_cross_tenant_peer_cannot_read_saga(self):
        saga_id = self.executor._dispatch(A2ACommand(operation="begin"), self.operator, self.context)["id"]
        outsider = dict(self.operator, tenant_id="other", principal_id="outsider")
        with self.assertRaisesRegex(SagaError, "Saga not found"):
            self.executor._dispatch(A2ACommand(operation="get", saga_id=saga_id), outsider, self.context)

    def test_viewer_can_read_but_cannot_mutate(self):
        saga_id = self.executor._dispatch(A2ACommand(operation="begin"), self.operator, self.context)["id"]
        viewer = dict(self.operator, principal_id="viewer", roles=("viewer",))
        fetched = self.executor._dispatch(A2ACommand(operation="get", saga_id=saga_id), viewer, self.context)
        self.assertEqual(fetched["id"], saga_id)
        with self.assertRaises(AuthorizationError):
            self.executor._dispatch(A2ACommand(operation="commit", saga_id=saga_id), viewer, self.context)


if __name__ == "__main__":
    unittest.main()
