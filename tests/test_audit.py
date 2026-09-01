import json
import tempfile
import unittest
from pathlib import Path

from semantic_saga_mcp.coordinator import SagaError
from semantic_saga_mcp.instrumented_coordinator import InstrumentedCoordinator
from semantic_saga_mcp.registry import ActionRegistry
from semantic_saga_mcp.store import SagaStore, SQLiteSagaStore


class EchoAction:
    def execute(self, values, saga_id, step_id):
        return {"receipt": "result-secret-value", "ok": True}

    def compensate(self, values, result, saga_id, step_id):
        return None


def registry():
    value = ActionRegistry()
    value.register_runtime(
        "echo",
        EchoAction(),
        version="1.0.0",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
    )
    return value


class AuditJournalTests(unittest.TestCase):
    def test_audit_chain_records_lifecycle_and_verifies(self):
        coordinator = InstrumentedCoordinator(SagaStore(), registry())
        saga_id = coordinator.begin(principal_id="alice")["id"]
        coordinator.execute(saga_id, "echo", {"value": 1})
        coordinator.commit(saga_id)

        result = coordinator.get_audit_events(saga_id)
        types = [event["event_type"] for event in result["events"]]
        self.assertEqual(types[0], "SAGA_STARTED")
        self.assertIn("ACTION_REQUESTED", types)
        self.assertIn("ACTION_ATTEMPT_SUMMARY", types)
        self.assertIn("ACTION_COMPLETED", types)
        self.assertEqual(types[-1], "SAGA_COMMITTED")

        integrity = coordinator.verify_audit_chain(saga_id)
        self.assertTrue(integrity["valid"])
        self.assertEqual(integrity["events_checked"], len(result["events"]))
        self.assertEqual(integrity["head_hash"], result["events"][-1]["event_hash"])
        for index, event in enumerate(result["events"]):
            expected_previous = None if index == 0 else result["events"][index - 1]["event_hash"]
            self.assertEqual(event["previous_hash"], expected_previous)

    def test_timeline_and_audit_do_not_persist_payload_values(self):
        coordinator = InstrumentedCoordinator(SagaStore(), registry())
        saga_id = coordinator.begin()["id"]
        coordinator.execute(
            saga_id,
            "echo",
            {"credential": "input-secret-value", "nested": {"token": "another-secret"}},
        )
        coordinator.checkpoint(
            saga_id,
            "private-review",
            {"private": "checkpoint-secret-value"},
        )

        serialized_audit = json.dumps(coordinator.get_audit_events(saga_id), sort_keys=True)
        serialized_timeline = json.dumps(coordinator.get_timeline(saga_id), sort_keys=True)
        for forbidden in (
            "input-secret-value",
            "another-secret",
            "result-secret-value",
            "checkpoint-secret-value",
        ):
            self.assertNotIn(forbidden, serialized_audit)
            self.assertNotIn(forbidden, serialized_timeline)

        timeline = coordinator.get_timeline(saga_id)
        self.assertNotIn("input", timeline["steps"][0])
        self.assertNotIn("result", timeline["steps"][0])
        self.assertNotIn("action_definition", timeline["steps"][0])

    def test_in_memory_tamper_is_detected(self):
        coordinator = InstrumentedCoordinator(SagaStore(), registry())
        saga_id = coordinator.begin()["id"]
        coordinator.checkpoint(saga_id, "before-change")
        self.assertTrue(coordinator.verify_audit_chain(saga_id)["valid"])

        coordinator.audit._memory[0]["status"] = "TAMPERED"  # type: ignore[attr-defined]
        result = coordinator.verify_audit_chain(saga_id)
        self.assertFalse(result["valid"])
        self.assertEqual(result["failed_sequence"], 1)

    def test_sqlite_audit_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "audit.db")
            first_store = SQLiteSagaStore(path)
            first = InstrumentedCoordinator(first_store, registry())
            saga_id = first.begin()["id"]
            first.checkpoint(saga_id, "durable")
            first_events = first.get_audit_events(saga_id)["events"]
            self.assertGreaterEqual(len(first_events), 2)

            second_store = SQLiteSagaStore(path)
            second = InstrumentedCoordinator(second_store, registry())
            second_events = second.get_audit_events(saga_id)["events"]
            self.assertEqual(
                [event["event_hash"] for event in first_events],
                [event["event_hash"] for event in second_events],
            )
            self.assertTrue(second.verify_audit_chain(saga_id)["valid"])

    def test_audit_and_timeline_follow_saga_ownership(self):
        coordinator = InstrumentedCoordinator(SagaStore(), registry())
        saga_id = coordinator.begin(session_id="tenant:a")["id"]
        self.assertTrue(coordinator.verify_audit_chain(saga_id, session_id="tenant:a")["valid"])
        with self.assertRaisesRegex(SagaError, "Saga not found"):
            coordinator.get_audit_events(saga_id, session_id="tenant:b")
        with self.assertRaisesRegex(SagaError, "Saga not found"):
            coordinator.get_timeline(saga_id, session_id="tenant:b")
        with self.assertRaisesRegex(SagaError, "Saga not found"):
            coordinator.verify_audit_chain(saga_id, session_id="tenant:b")

    def test_event_type_filter_does_not_change_chain_integrity(self):
        coordinator = InstrumentedCoordinator(SagaStore(), registry())
        saga_id = coordinator.begin()["id"]
        coordinator.checkpoint(saga_id, "one")
        filtered = coordinator.get_audit_events(
            saga_id,
            event_types={"CHECKPOINT_CREATED"},
        )["events"]
        self.assertEqual([event["event_type"] for event in filtered], ["CHECKPOINT_CREATED"])
        self.assertTrue(coordinator.verify_audit_chain(saga_id)["valid"])


if __name__ == "__main__":
    unittest.main()
