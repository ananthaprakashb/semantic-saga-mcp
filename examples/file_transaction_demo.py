"""Create three text files, fail the fourth step, and watch rollback remove them."""

from pathlib import Path

from semantic_saga_mcp.actions import FileTransactionTool
from semantic_saga_mcp.coordinator import Coordinator, SagaError
from semantic_saga_mcp.store import SagaStore


def main() -> None:
    root = Path("./saga-files")
    action = FileTransactionTool(root, print)
    coordinator = Coordinator(SagaStore(), {"create_text_file": action})
    saga_id = coordinator.begin({"demo": "file transaction"})["id"]

    try:
        for number in range(1, 4):
            coordinator.execute(
                saga_id,
                "create_text_file",
                {"path": f"demo-{number}.txt", "content": f"Saga demo file {number}\n"},
            )
        coordinator.execute(
            saga_id,
            "create_text_file",
            {"path": "demo-4.txt", "content": "This is never written.\n", "simulate_error": True},
        )
    except SagaError as exc:
        print(f"Expected failure: {exc}")

    print(f"Final saga status: {coordinator.get(saga_id)['status']}")


if __name__ == "__main__":
    main()
