import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# Keep direct unittest discovery working from a source checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mcp.server.auth.provider import AccessToken

from semantic_saga_mcp.auth import (
    AuthorizationError,
    AuthorizationPolicy,
    IdentityError,
    StaticTokenVerifier,
)
from semantic_saga_mcp.coordinator import Coordinator, SagaError
from semantic_saga_mcp.execution import ExecutionContextResolver
from semantic_saga_mcp.mcp_server import build_mcp_server
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore


class RegistryRuntimeAction:
    def execute(self, values, saga_id, step_id):
        return {"ok": True}

    def compensate(self, values, result, saga_id, step_id):
        return None


class ExecutionContextTests(unittest.TestCase):
    def test_stdio_owner_is_stable_and_local_process_is_admin(self):
        resolver = ExecutionContextResolver(local_owner_id="stdio:test")
        first = resolver.resolve(SimpleNamespace(request=None))
        second = resolver.resolve(SimpleNamespace(request=None))
        self.assertEqual(first.owner_id, "stdio:test")
        self.assertEqual(first.owner_id, second.owner_id)
        self.assertEqual(first.transport, "stdio")
        self.assertEqual(first.roles, ("admin",))

    def test_untrusted_proxy_headers_are_ignored(self):
        request = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "mallory",
                "x-semantic-saga-roles": "admin",
            }
        )
        resolver = ExecutionContextResolver(allow_anonymous_http=True)
        context = resolver.resolve(SimpleNamespace(request=request))
        self.assertEqual(context.tenant_id, "local-http")
        self.assertEqual(context.identity_source, "anonymous-development")

    def test_trusted_proxy_ownership_is_tenant_not_principal_scoped(self):
        resolver = ExecutionContextResolver(trust_proxy_headers=True)
        alice = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "alice",
                "x-semantic-saga-roles": "operator",
            }
        )
        robot = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "deploy-bot",
                "x-semantic-saga-principal-type": "service",
                "x-semantic-saga-roles": "operator",
            }
        )
        other = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "other",
                "x-semantic-saga-principal": "alice",
                "x-semantic-saga-roles": "operator",
            }
        )
        alice_ctx = resolver.resolve(SimpleNamespace(request=alice))
        robot_ctx = resolver.resolve(SimpleNamespace(request=robot))
        other_ctx = resolver.resolve(SimpleNamespace(request=other))
        self.assertEqual(alice_ctx.owner_id, robot_ctx.owner_id)
        self.assertNotEqual(alice_ctx.owner_id, other_ctx.owner_id)
        self.assertEqual(robot_ctx.principal_type, "service")

    def test_verified_oauth_token_supplies_tenant_roles_and_scopes(self):
        token = AccessToken(
            token="secret",
            client_id="agent-client",
            subject="alice",
            scopes=["semantic-saga", "semantic-saga:execute"],
            claims={"tenant_id": "acme", "roles": ["operator"]},
        )
        request = SimpleNamespace(headers={"authorization": "Bearer secret"})
        with patch("semantic_saga_mcp.execution.get_access_token", return_value=token):
            context = ExecutionContextResolver().resolve(SimpleNamespace(request=request))
        self.assertEqual(context.tenant_id, "acme")
        self.assertEqual(context.principal_id, "alice")
        self.assertEqual(context.roles, ("operator",))
        self.assertIn("semantic-saga:execute", context.scopes)
        self.assertEqual(context.identity_source, "oauth-access-token")
        self.assertNotIn("secret", context.owner_id)

    def test_authenticated_token_requires_tenant_by_default(self):
        token = AccessToken(
            token="secret",
            client_id="agent-client",
            subject="alice",
            scopes=["semantic-saga"],
            claims={"roles": ["viewer"]},
        )
        request = SimpleNamespace(headers={"authorization": "Bearer secret"})
        with patch("semantic_saga_mcp.execution.get_access_token", return_value=token):
            with self.assertRaisesRegex(IdentityError, "missing a tenant claim"):
                ExecutionContextResolver().resolve(SimpleNamespace(request=request))

    def test_protected_remote_mode_requires_proxy_principal(self):
        resolver = ExecutionContextResolver(
            trust_proxy_headers=True,
            require_proxy_identity=True,
        )
        request = SimpleNamespace(headers={})
        with self.assertRaisesRegex(IdentityError, "proxy identity is required"):
            resolver.resolve(SimpleNamespace(request=request))

    def test_same_tenant_agents_can_continue_but_other_tenant_cannot(self):
        resolver = ExecutionContextResolver(trust_proxy_headers=True)
        alice = resolver.resolve(
            SimpleNamespace(request=SimpleNamespace(headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "alice",
                "x-semantic-saga-roles": "operator",
            }))
        )
        bot = resolver.resolve(
            SimpleNamespace(request=SimpleNamespace(headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "bot",
                "x-semantic-saga-roles": "operator",
            }))
        )
        outsider = resolver.resolve(
            SimpleNamespace(request=SimpleNamespace(headers={
                "x-semantic-saga-tenant": "other",
                "x-semantic-saga-principal": "alice",
                "x-semantic-saga-roles": "operator",
            }))
        )
        coordinator = Coordinator(SagaStore(), {})
        saga = coordinator.begin({"_identity": alice.audit_metadata()}, session_id=alice.owner_id)
        self.assertEqual(coordinator.get(saga["id"], session_id=bot.owner_id)["id"], saga["id"])
        with self.assertRaises(SagaError):
            coordinator.get(saga["id"], session_id=outsider.owner_id)


class AuthorizationPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = AuthorizationPolicy()

    def test_viewer_is_read_only_and_can_inspect_action_registry(self):
        for tool in ("get_saga", "list_actions", "get_action"):
            self.policy.authorize(tool, roles=["viewer"], scopes=[])
        with self.assertRaises(AuthorizationError):
            self.policy.authorize("begin_saga", roles=["viewer"], scopes=[])

    def test_operator_can_mutate(self):
        for tool in ("begin_saga", "execute_saga_step", "commit_saga", "rollback_saga"):
            self.policy.authorize(tool, roles=["operator"], scopes=[])

    def test_scopes_can_grant_service_principal_without_roles(self):
        self.policy.authorize("get_saga", roles=[], scopes=["semantic-saga:read"])
        self.policy.authorize("list_actions", roles=[], scopes=["semantic-saga:read"])
        self.policy.authorize("begin_saga", roles=[], scopes=["semantic-saga:execute"])


class StaticTokenVerifierTests(unittest.IsolatedAsyncioTestCase):
    async def test_static_token_file_is_explicit_dev_test_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tokens.json"
            path.write_text(
                '{"dev-token":{"client_id":"client","subject":"alice","scopes":["semantic-saga"],"claims":{"tenant_id":"acme","roles":["operator"]}}}',
                encoding="utf-8",
            )
            verifier = StaticTokenVerifier.from_file(str(path))
            accepted = await verifier.verify_token("dev-token")
            rejected = await verifier.verify_token("wrong")
            self.assertEqual(accepted.subject, "alice")
            self.assertEqual(accepted.claims["tenant_id"], "acme")
            self.assertIsNone(rejected)


class OfficialMcpV2Tests(unittest.IsolatedAsyncioTestCase):
    async def test_modern_discovery_and_tools_use_same_explicit_saga_handle(self):
        from mcp import Client

        coordinator = Coordinator(SagaStore(), {})
        server = build_mcp_server(
            coordinator,
            ExecutionContextResolver(local_owner_id="stdio:test-client"),
        )

        async with Client(server) as client:
            self.assertEqual(client.protocol_version, "2026-07-28")

            created = await client.call_tool("begin_saga", {"metadata": {"team": "platform"}})
            self.assertFalse(created.is_error)
            saga_id = created.structured_content["id"]
            self.assertEqual(created.structured_content["metadata"]["_identity"]["tenant_id"], "local")

            fetched = await client.call_tool("get_saga", {"saga_id": saga_id})
            self.assertFalse(fetched.is_error)
            self.assertEqual(fetched.structured_content["id"], saga_id)
            self.assertEqual(fetched.structured_content["status"], "ACTIVE")

    async def test_action_registry_is_discoverable_through_mcp(self):
        from mcp import Client

        registry = ActionRegistry()
        registry.register_runtime(
            "demo",
            RegistryRuntimeAction(),
            version="2.1.0",
            semantic={"domain": "demo", "operation": "write", "risk": "low"},
            input_schema={"type": "object"},
        )
        coordinator = Coordinator(SagaStore(), registry)
        server = build_mcp_server(
            coordinator,
            ExecutionContextResolver(local_owner_id="stdio:registry-client"),
        )

        async with Client(server) as client:
            listed = await client.call_tool("list_actions", {})
            self.assertFalse(listed.is_error)
            self.assertEqual(listed.structured_content["actions"][0]["id"], "demo")
            self.assertEqual(listed.structured_content["actions"][0]["version"], "2.1.0")
            fetched = await client.call_tool("get_action", {"action": "demo", "version": "2.1.0"})
            self.assertFalse(fetched.is_error)
            self.assertEqual(fetched.structured_content["semantic"]["risk"], "low")
            self.assertIn("definition_hash", fetched.structured_content)

    async def test_modern_tool_arguments_remain_strict(self):
        from mcp import Client

        coordinator = Coordinator(SagaStore(), {})
        server = build_mcp_server(
            coordinator,
            ExecutionContextResolver(local_owner_id="stdio:test-client"),
        )

        async with Client(server) as client:
            result = await client.call_tool("begin_saga", {"metadata": "not-an-object"})
            self.assertTrue(result.is_error)
            self.assertIn("Invalid arguments", result.content[0].text)
            self.assertEqual(coordinator.store.pending_rollbacks(), [])

            registry_result = await client.call_tool("list_actions", {"unexpected": True})
            self.assertTrue(registry_result.is_error)
            self.assertIn("Invalid arguments", registry_result.content[0].text)


if __name__ == "__main__":
    unittest.main()
