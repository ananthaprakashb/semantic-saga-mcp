from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .actions import load_actions
from .coordinator import Coordinator, SagaError
from .store import SagaStore


TOOLS = [
    {"name": "begin_saga", "description": "Start a durable transactional workflow.", "inputSchema": {"type": "object", "properties": {"metadata": {"type": "object"}}}},
    {"name": "execute_saga_step", "description": "Execute a configured action and durably register its compensation.", "inputSchema": {"type": "object", "properties": {"saga_id": {"type": "string"}, "action": {"type": "string"}, "input": {"type": "object"}}, "required": ["saga_id", "action", "input"]}},
    {"name": "commit_saga", "description": "Commit a successfully completed saga; it can no longer be rolled back.", "inputSchema": {"type": "object", "properties": {"saga_id": {"type": "string"}}, "required": ["saga_id"]}},
    {"name": "rollback_saga", "description": "Compensate saga steps in reverse order. Safe compensation endpoints receive stable idempotency keys.", "inputSchema": {"type": "object", "properties": {"saga_id": {"type": "string"}}, "required": ["saga_id"]}},
    {"name": "get_saga", "description": "Inspect saga and step status, results, and rollback failures.", "inputSchema": {"type": "object", "properties": {"saga_id": {"type": "string"}}, "required": ["saga_id"]}},
]


class McpServer:
    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator

    def dispatch(self, message: dict[str, Any]) -> dict[str, Any] | None:
        request_id, method = message.get("id"), message.get("method")
        if request_id is None:  # notification
            return None
        try:
            if method == "initialize":
                result = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": False}}, "serverInfo": {"name": "semantic-saga-mcp", "version": "0.1.0"}}
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": TOOLS}
            elif method == "tools/call":
                result = self._call(message.get("params", {}))
            else:
                return self._error(request_id, -32601, f"Method not found: {method}")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except (SagaError, KeyError, TypeError, ValueError) as exc:
            return self._error(request_id, -32602, str(exc))
        except Exception as exc:
            return self._error(request_id, -32603, f"Internal error: {exc}")

    def _call(self, params: dict[str, Any]) -> dict[str, Any]:
        name, args = params["name"], params.get("arguments", {})
        if name == "begin_saga": value = self.coordinator.begin(args.get("metadata"))
        elif name == "execute_saga_step": value = self.coordinator.execute(args["saga_id"], args["action"], args["input"])
        elif name == "commit_saga": value = self.coordinator.commit(args["saga_id"])
        elif name == "rollback_saga": value = self.coordinator.rollback(args["saga_id"])
        elif name == "get_saga": value = self.coordinator.get(args["saga_id"])
        else: raise ValueError(f"Unknown tool: {name}")
        return {"content": [{"type": "text", "text": json.dumps(value, separators=(",", ":"))}], "structuredContent": value}

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}

    def run(self) -> None:
        # MCP stdio uses one JSON-RPC message per line. Never write diagnostics to stdout.
        for line in sys.stdin:
            try:
                response = self.dispatch(json.loads(line))
                if response is not None:
                    print(json.dumps(response, separators=(",", ":")), flush=True)
            except json.JSONDecodeError as exc:
                print(json.dumps(self._error(None, -32700, f"Parse error: {exc}")), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable MCP saga coordinator")
    parser.add_argument("--actions", default=os.getenv("SAGA_ACTIONS_FILE"), help="JSON action definitions")
    parser.add_argument("--database", default=os.getenv("SAGA_DATABASE", "./semantic-saga.db"))
    args = parser.parse_args()
    if not args.actions:
        parser.error("--actions or SAGA_ACTIONS_FILE is required")
    McpServer(Coordinator(SagaStore(args.database), load_actions(args.actions))).run()


if __name__ == "__main__":
    main()
