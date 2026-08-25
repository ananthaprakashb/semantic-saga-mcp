from __future__ import annotations

import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol


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


def _render_preview(value: Any, context: dict[str, Any]) -> Any:
    """Render known values while retaining unavailable result placeholders."""
    try:
        return render(value, context)
    except (ActionError, KeyError, TypeError):
        return value


@dataclass(frozen=True)
class HttpRequest:
    url: str
    method: str = "POST"
    body: Any = None
    headers: dict[str, str] | None = None
    timeout_seconds: float = 30

    def preview(self, context: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        headers = {
            str(key): "<redacted>" if str(key).lower() in {"authorization", "cookie", "proxy-authorization", "x-api-key"} else str(_render_preview(value, context))
            for key, value in (self.headers or {}).items()
        }
        headers.setdefault("Idempotency-Key", idempotency_key)
        return {
            "method": self.method.upper(),
            "url": _render_preview(self.url, context),
            "headers": headers,
            "body": _render_preview(self.body, context),
        }

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


@dataclass(frozen=True)
class DryRunHttpAction:
    """Preview both requests, force rollback, and perform no network I/O."""

    action: HttpAction
    log: Callable[[str], None]

    def execute(self, values: dict[str, Any], saga_id: str, step_id: str) -> Any:
        command = self.action.forward.preview(_context(values, None, saga_id, step_id), step_id)
        self.log(f"[dry-run] forward: {json.dumps(command, sort_keys=True)}")
        raise ActionError("dry-run simulated failure")

    def compensate(self, values: dict[str, Any], result: Any, saga_id: str, step_id: str) -> None:
        command = self.action.rollback.preview(_context(values, result, saga_id, step_id), f"{step_id}:rollback")
        self.log(f"[dry-run] compensation: {json.dumps(command, sort_keys=True)}")


@dataclass(frozen=True)
class FileTransactionTool:
    """Built-in local action that creates and compensates text files."""

    root: Path
    log: Callable[[str], None] | None = None

    def _path(self, value: Any) -> Path:
        if not isinstance(value, str) or not value or Path(value).suffix.lower() != ".txt":
            raise ActionError("File path must be a non-empty .txt path")
        root = self.root.expanduser().resolve()
        path = (root / value).resolve()
        if path == root or root not in path.parents:
            raise ActionError("File path must remain inside the configured file root")
        return path

    def execute(self, values: dict[str, Any], saga_id: str, step_id: str) -> dict[str, str]:
        path = self._path(values.get("path"))
        content = values.get("content")
        if not isinstance(content, str):
            raise ActionError("File content must be a string")
        if values.get("simulate_error", False):
            raise ActionError("simulated file transaction failure")
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("x", encoding="utf-8", errors="strict", newline="") as handle:
                handle.write(content)
        except FileExistsError as exc:
            raise ActionError(f"Text file already exists: {path}") from exc
        except OSError as exc:
            raise ActionError(f"Unable to create text file: {exc}") from exc
        if self.log:
            self.log(f"[file-transaction] created: {path}")
        return {"path": str(path)}

    def compensate(self, values: dict[str, Any], result: Any, saga_id: str, step_id: str) -> None:
        # A failed forward action has no receipt and may have encountered a file
        # owned by somebody else. Never delete unless this step reported success.
        if not isinstance(result, dict) or result.get("path") is None:
            return
        path = self._path(values.get("path"))
        if path != Path(result["path"]).resolve():
            raise ActionError("File compensation receipt does not match the requested path")
        existed = path.exists()
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise ActionError(f"Unable to delete text file: {exc}") from exc
        if self.log and existed:
            self.log(f"[file-transaction] deleted: {path}")


def _context(values: dict[str, Any], result: Any, saga_id: str, step_id: str) -> dict[str, Any]:
    return {"input": values, "result": result, "saga": {"id": saga_id}, "step": {"id": step_id}}


def load_actions(path: str, *, dry_run: bool = False, log: Callable[[str], None] | None = None) -> dict[str, Action]:
    with open(path, encoding="utf-8") as handle:
        definitions = json.load(handle)
    actions: dict[str, Action] = {}
    logger = log or (lambda message: print(message, file=sys.stderr, flush=True))
    for name, definition in definitions.items():
        action = HttpAction(HttpRequest(**definition["forward"]), HttpRequest(**definition["rollback"]))
        actions[name] = DryRunHttpAction(action, logger) if dry_run else action
    return actions
