import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

# Keep direct unittest discovery working from a source checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from semantic_saga_mcp.coordinator import Coordinator
from semantic_saga_mcp.execution import ExecutionContextResolver
from semantic_saga_mcp.mcp_server import build_mcp_server
from semantic_saga_mcp.store import SagaStore


class ExecutionContextTests(unittest.TestCase):
    def test_stdio_owner_is_stable_for_process(self):
        resolver = ExecutionContextResolver(local_owner_id="stdio:test")
        first = resolver.resolve(SimpleNamespace(request=None))
        second = resolver.resolve(SimpleNamespace(request=None))
        self.assertEqual(first.owner_id, "stdio:test")
        self.assertEqual(first.owner_id, second.owner_id)
        self.assertEqual(first.transport, "stdio")

    def test_authorization_value_is_hashed_not_persisted_as_owner(self):
        secret = "Bearer top-secret-token"
        request = SimpleNamespace(headers={"authorization": secret})
        context = ExecutionContextResolver().resolve(SimpleNamespace(request=request))
        self.assertTrue(context.owner_id.startswith("authorization:"))
        self.assertNotIn(secret, context.owner_id)
        self.assertEqual(context.identity_source, "authorization-digest")

    def test_proxy_identity_headers_are_ignored_by_default(self):
        request = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "alice",
            }
        )
        context = ExecutionContextResolver().resolve(SimpleNamespace(request=request))
        self.assertEqual(context.owner_id, "http:anonymous")
        self.assertEqual(context.identity_source, "anonymous")

    def test_trusted_proxy_identity_is_stable_and_tenant_scoped(self):
        resolver = ExecutionContextResolver(trust_proxy_headers=True)
        first_request = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "acme",
                "x-semantic-saga-principal": "alice",
            }
        )
        second_request = SimpleNamespace(
            headers={
                "x-semantic-saga-tenant": "other",
                "x-semantic-saga-principal": "alice",
            }
        )
        first = resolver.resolve(SimpleNamespace(request=first_request))
        repeated = resolver.resolve(SimpleNamespace(request=first_request))
        second = resolver.resolve(SimpleNamespace(request=second_request))
        self.assertEqual(first.owner_id, repeated.owner_id)
        self.assertNotEqual(first.owner_id, second.owner_id)
        self.assertEqual(first.identity_source, "trusted-proxy-header")

    def test_protected_remote_mode_requires_proxy_principal(self):
        resolver = ExecutionContextResolver(
            trust_proxy_headers=True,
            require_proxy_identity=True,
        )
        request = SimpleNamespace(headers={})
        with self.assertRaisesRegex(ValueError, "proxy identity is required"):
            resolver.resolve(SimpleNamespace(request=request))


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

            fetched = await client.call_tool("get_saga", {"saga_id": saga_id})
            self.assertFalse(fetched.is_error)
            self.assertEqual(fetched.structured_content["id"], saga_id)
            self.assertEqual(fetched.structured_content["status"], "ACTIVE")

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


if __name__ == "__main__":
    unittest.main()
