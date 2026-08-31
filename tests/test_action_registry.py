import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from semantic_saga_mcp.actions import ActionError
from semantic_saga_mcp.coordinator import Coordinator, SagaError
from semantic_saga_mcp.registry import ActionDefinition, ActionRegistry, ActionRegistryError
from semantic_saga_mcp.secrets import MappingSecretProvider
from semantic_saga_mcp.store import SagaStore, SQLiteSagaStore


class FakeResponse:
    def __init__(self, value=None):
        self.payload = b"" if value is None else json.dumps(value).encode("utf-8")
        self.headers = {"Content-Type": "application/json"}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def read(self):
        return self.payload


def http_definition(version: str, base: str = "https://v1.example") -> ActionDefinition:
    return ActionDefinition(
        action_id="provision",
        version=version,
        kind="http",
        semantic={
            "domain": "infrastructure",
            "operation": "create",
            "resource": "account",
            "reversibility": "full",
            "risk": "medium",
        },
        input_schema={
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string", "minLength": 1}},
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}},
            "additionalProperties": True,
        },
        forward={
            "url": f"{base}/accounts",
            "headers": {
                "Authorization": {"secret_ref": "secret://api", "prefix": "Bearer "}
            },
            "body": {"name": "${input.name}"},
        },
        compensation={
            "url": f"{base}/accounts/delete",
            "headers": {
                "Authorization": {"secret_ref": "secret://api", "prefix": "Bearer "}
            },
            "body": {"id": "${result.id}"},
        },
    )


class RuntimeV1:
    def execute(self, values, saga_id, step_id):
        return {"receipt": values["value"]}

    def compensate(self, values, result, saga_id, step_id):
        return None


class RuntimeV2:
    def execute(self, values, saga_id, step_id):
        return {"receipt": values["value"], "version": 2}

    def compensate(self, values, result, saga_id, step_id):
        return None


class ActionRegistryTests(unittest.TestCase):
    def setUp(self):
        self.provider = MappingSecretProvider({"secret://api": "top-secret"})

    def registry(self, definition: ActionDefinition) -> ActionRegistry:
        registry = ActionRegistry(secret_provider=self.provider)
        registry.register_definition(definition)
        return registry

    def test_step_persists_version_hash_and_secret_reference_before_side_effect(self):
        registry = self.registry(http_definition("1.0.0"))
        store = SagaStore()
        coordinator = Coordinator(store, registry)
        saga_id = coordinator.begin()["id"]

        with patch("urllib.request.urlopen", return_value=FakeResponse({"id": "acct-1"})) as send:
            step = coordinator.execute(saga_id, "provision", {"name": "alpha"})

        self.assertEqual(step["action_version"], "1.0.0")
        self.assertEqual(step["action_definition_hash"], registry.resolve("provision").definition.hash)
        serialized = json.dumps(step["action_definition"], sort_keys=True)
        self.assertIn("secret://api", serialized)
        self.assertNotIn("top-secret", serialized)
        request = send.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer top-secret")

    def test_historical_http_compensation_uses_persisted_v1_after_registry_moves_to_v2(self):
        store = SagaStore()
        first = Coordinator(store, self.registry(http_definition("1.0.0", "https://v1.example")))
        saga_id = first.begin()["id"]
        with patch("urllib.request.urlopen", return_value=FakeResponse({"id": "acct-1"})):
            first.execute(saga_id, "provision", {"name": "alpha"})

        second = Coordinator(store, self.registry(http_definition("2.0.0", "https://v2.example")))
        seen = []

        def send(request, timeout=None):
            seen.append(request.full_url)
            return FakeResponse()

        with patch("urllib.request.urlopen", side_effect=send):
            result = second.rollback(saga_id)

        self.assertEqual(result["status"], "ROLLED_BACK")
        self.assertEqual(seen, ["https://v1.example/accounts/delete"])
        self.assertEqual(result["steps"][0]["action_version"], "1.0.0")

    def test_tampered_definition_hash_fails_closed_without_compensation(self):
        registry = self.registry(http_definition("1.0.0"))
        store = SagaStore()
        coordinator = Coordinator(store, registry)
        saga_id = coordinator.begin()["id"]
        with patch("urllib.request.urlopen", return_value=FakeResponse({"id": "acct-1"})):
            step = coordinator.execute(saga_id, "provision", {"name": "alpha"})
        store.update_step(step["id"], action_definition_hash="0" * 64)

        with patch("urllib.request.urlopen") as send:
            result = coordinator.rollback(saga_id)

        send.assert_not_called()
        self.assertEqual(result["status"], "ROLLBACK_FAILED")
        self.assertEqual(result["steps"][0]["status"], "COMPENSATION_FAILED")
        self.assertIn("hash mismatch", result["steps"][0]["error"])

    def test_runtime_implementation_change_refuses_unsafe_compensation(self):
        store = SagaStore()
        v1 = ActionRegistry()
        v1.register_runtime("runtime", RuntimeV1(), version="1.0.0")
        first = Coordinator(store, v1)
        saga_id = first.begin()["id"]
        first.execute(saga_id, "runtime", {"value": 7})

        changed = ActionRegistry()
        changed.register_runtime("runtime", RuntimeV2(), version="1.0.0")
        result = Coordinator(store, changed).rollback(saga_id)

        self.assertEqual(result["status"], "ROLLBACK_FAILED")
        self.assertIn("implementation changed", result["steps"][0]["error"])

    def test_input_schema_blocks_network_and_does_not_create_step(self):
        registry = self.registry(http_definition("1.0.0"))
        coordinator = Coordinator(SagaStore(), registry)
        saga_id = coordinator.begin()["id"]

        with patch("urllib.request.urlopen") as send, self.assertRaisesRegex(SagaError, "schema validation failed"):
            coordinator.execute(saga_id, "provision", {"name": 123})

        send.assert_not_called()
        self.assertEqual(coordinator.get(saga_id)["steps"], [])
        self.assertEqual(coordinator.get(saga_id)["status"], "ACTIVE")

    def test_output_schema_failure_retains_receipt_then_compensates(self):
        registry = self.registry(http_definition("1.0.0"))
        coordinator = Coordinator(SagaStore(), registry)
        saga_id = coordinator.begin()["id"]
        responses = [FakeResponse({"id": 42}), FakeResponse()]

        with patch("urllib.request.urlopen", side_effect=responses), self.assertRaisesRegex(SagaError, "rollback attempted"):
            coordinator.execute(saga_id, "provision", {"name": "alpha"})

        result = coordinator.get(saga_id)
        self.assertEqual(result["status"], "ROLLED_BACK")
        self.assertEqual(result["steps"][0]["result"], {"id": 42})
        self.assertEqual(result["steps"][0]["status"], "COMPENSATED")

    def test_dry_run_never_resolves_or_prints_secret_value(self):
        messages = []
        registry = ActionRegistry(secret_provider=self.provider, dry_run=True, log=messages.append)
        registry.register_definition(http_definition("1.0.0"))
        coordinator = Coordinator(SagaStore(), registry)
        saga_id = coordinator.begin()["id"]

        with patch("urllib.request.urlopen") as send, self.assertRaises(SagaError):
            coordinator.execute(saga_id, "provision", {"name": "alpha"})

        send.assert_not_called()
        combined = "\n".join(messages)
        self.assertIn("<redacted>", combined)
        self.assertNotIn("top-secret", combined)

    def test_pre_registry_step_requires_explicit_legacy_recovery(self):
        store = SagaStore()
        registry = self.registry(http_definition("1.0.0"))
        coordinator = Coordinator(store, registry)
        saga = coordinator.begin()
        timestamp = saga["created_at"]
        store.create_step(
            {
                "id": "legacy-step",
                "saga_id": saga["id"],
                "action": "provision",
                "input": {"name": "old"},
                "status": "EXECUTING",
                "result": None,
                "error": None,
                "compensation_attempts": 0,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        )

        result = coordinator.rollback(saga["id"])
        self.assertEqual(result["status"], "ROLLBACK_FAILED")
        self.assertIn("predates immutable action snapshots", result["steps"][0]["error"])

        legacy_registry = ActionRegistry(secret_provider=self.provider, allow_legacy_recovery=True)
        legacy_registry.register_definition(http_definition("1.0.0"))
        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            recovered = Coordinator(store, legacy_registry).rollback(saga["id"])
        self.assertEqual(recovered["status"], "ROLLED_BACK")

    def test_sqlite_migration_round_trips_action_contract_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "sagas.db")
            store = SQLiteSagaStore(path)
            coordinator = Coordinator(store, self.registry(http_definition("1.0.0")))
            saga_id = coordinator.begin()["id"]
            with patch("urllib.request.urlopen", return_value=FakeResponse({"id": "acct-1"})):
                step = coordinator.execute(saga_id, "provision", {"name": "alpha"})

            restarted = SQLiteSagaStore(path)
            loaded = restarted.get_step(step["id"])
            self.assertEqual(loaded["action_version"], "1.0.0")
            self.assertEqual(loaded["action_definition_hash"], step["action_definition_hash"])
            self.assertEqual(loaded["action_definition"], step["action_definition"])

    def test_action_version_is_immutable_inside_registry(self):
        registry = ActionRegistry(secret_provider=self.provider)
        registry.register_definition(http_definition("1.0.0", "https://one.example"))
        with self.assertRaisesRegex(ActionRegistryError, "immutable"):
            registry.register_definition(http_definition("1.0.0", "https://changed.example"))


if __name__ == "__main__":
    unittest.main()
