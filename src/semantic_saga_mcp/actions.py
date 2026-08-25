from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol


class ActionError(RuntimeError):
    pass


class Action(Protocol):
    def execute(self, values: dict[str, Any], saga_id: str, step_id: str) -> Any: ...
    def compensate(self, values: dict[str, Any], result: Any, saga_id: str, step_id: str) -> Any: ...


_TOKEN = re.compile(r"^\$\{(input|result|saga|step)((?:\.[A-Za-z0-9_-]+)*)\}$")


def render(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, list):
        return [render(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render(item, context) for key, item in value.items()}
    if not isinstance(value, str):
        return value
    match = _TOKEN.match(value)
    if not match:
        return value
    current: Any = context[match.group(1)]
    for part in match.group(2).split("."):
        if part:
            if not isinstance(current, dict) or part not in current:
                raise ActionError(f"Template value not found: {value}")
            current = current[part]
    return current


@dataclass(frozen=True)
class HttpRequest:
    url: str
    method: str = "POST"
    body: Any = None
    headers: dict[str, str] | None = None
    timeout_seconds: float = 30

    def send(self, context: dict[str, Any], idempotency_key: str) -> Any:
        url = render(self.url, context)
        body = render(self.body, context)
        headers = {str(k): str(render(v, context)) for k, v in (self.headers or {}).items()}
        headers.setdefault("Idempotency-Key", idempotency_key)
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers.setdefault("Content-Type", "application/json")
        request = urllib.request.Request(url, data=data, headers=headers, method=self.method.upper())
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read()
                if not payload:
                    return None
                if "application/json" in response.headers.get("Content-Type", ""):
                    return json.loads(payload)
                return payload.decode()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:1000]
            raise ActionError(f"HTTP {exc.code} from action endpoint: {detail}") from exc
        except urllib.error.URLError as exc:
            raise ActionError(f"Action endpoint unavailable: {exc.reason}") from exc


@dataclass(frozen=True)
class HttpAction:
    forward: HttpRequest
    rollback: HttpRequest

    def execute(self, values: dict[str, Any], saga_id: str, step_id: str) -> Any:
        return self.forward.send(_context(values, None, saga_id, step_id), step_id)

    def compensate(self, values: dict[str, Any], result: Any, saga_id: str, step_id: str) -> Any:
        return self.rollback.send(_context(values, result, saga_id, step_id), f"{step_id}:rollback")


def _context(values: dict[str, Any], result: Any, saga_id: str, step_id: str) -> dict[str, Any]:
    return {"input": values, "result": result, "saga": {"id": saga_id}, "step": {"id": step_id}}


def load_actions(path: str) -> dict[str, HttpAction]:
    with open(path, encoding="utf-8") as handle:
        definitions = json.load(handle)
    actions: dict[str, HttpAction] = {}
    for name, definition in definitions.items():
        actions[name] = HttpAction(HttpRequest(**definition["forward"]), HttpRequest(**definition["rollback"]))
    return actions
