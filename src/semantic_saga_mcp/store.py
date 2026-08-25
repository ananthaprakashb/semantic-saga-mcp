from __future__ import annotations

import copy
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SagaStoreProtocol(Protocol):
    """Storage contract for saga state.

    Redis, PostgreSQL, and other adapters only need to implement these domain
    operations; the coordinator does not depend on SQL or a particular driver.
    Each mutating method must make its change durable before returning when the
    adapter is intended to provide crash recovery.
    """

    def create_saga(self, saga: dict[str, Any]) -> None: ...
    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None: ...
    def update_saga(self, saga_id: str, **changes: Any) -> None: ...
    def create_step(self, step: dict[str, Any]) -> dict[str, Any]: ...
    def get_step(self, step_id: str) -> dict[str, Any] | None: ...
    def list_steps(self, saga_id: str, statuses: set[str] | None = None, *, reverse: bool = False) -> list[dict[str, Any]]: ...
    def update_step(self, step_id: str, **changes: Any) -> None: ...
    def pending_rollbacks(self) -> list[tuple[str, str]]: ...


class SagaStore:
    """Thread-safe, dependency-free in-memory store used by default.

    This adapter is ideal for tests and ephemeral servers. Use a durable adapter
    (such as :class:`SQLiteSagaStore`) when rollback must survive a restart.
    """

    def __init__(self) -> None:
        self._sagas: dict[str, dict[str, Any]] = {}
        self._steps: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def create_saga(self, saga: dict[str, Any]) -> None:
        with self._lock:
            self._sagas[saga["id"]] = copy.deepcopy(saga)

    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            saga = self._sagas.get(saga_id)
            return copy.deepcopy(saga) if saga and saga["session_id"] == session_id else None

    def update_saga(self, saga_id: str, **changes: Any) -> None:
        with self._lock:
            self._sagas[saga_id].update(copy.deepcopy(changes))

    def create_step(self, step: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            sequences = [item["sequence"] for item in self._steps.values() if item["saga_id"] == step["saga_id"]]
            stored = copy.deepcopy(step)
            stored["sequence"] = max(sequences, default=0) + 1
            self._steps[stored["id"]] = stored
            return copy.deepcopy(stored)

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        with self._lock:
            return copy.deepcopy(self._steps.get(step_id))

    def list_steps(self, saga_id: str, statuses: set[str] | None = None, *, reverse: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            rows = [copy.deepcopy(row) for row in self._steps.values() if row["saga_id"] == saga_id and (statuses is None or row["status"] in statuses)]
        return sorted(rows, key=lambda row: row["sequence"], reverse=reverse)

    def update_step(self, step_id: str, **changes: Any) -> None:
        with self._lock:
            self._steps[step_id].update(copy.deepcopy(changes))

    def pending_rollbacks(self) -> list[tuple[str, str]]:
        with self._lock:
            return [(s["id"], s["session_id"]) for s in self._sagas.values() if s["status"] in {"FAILED", "ROLLING_BACK", "ROLLBACK_FAILED"} or (s["status"] == "ACTIVE" and any(step["saga_id"] == s["id"] and step["status"] == "EXECUTING" for step in self._steps.values()))]


class SQLiteSagaStore:
    """Durable SQLite implementation of :class:`SagaStoreProtocol`."""

    def __init__(self, path: str) -> None:
        Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sagas (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, status TEXT NOT NULL, metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT);
            CREATE TABLE IF NOT EXISTS steps (id TEXT PRIMARY KEY, saga_id TEXT NOT NULL, sequence INTEGER NOT NULL, action TEXT NOT NULL, input TEXT NOT NULL, status TEXT NOT NULL, result TEXT, error TEXT, compensation_attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(saga_id, sequence));
        """)
        columns = {row[1] for row in self._db.execute("PRAGMA table_info(sagas)")}
        if "session_id" not in columns:
            self._db.execute("ALTER TABLE sagas ADD COLUMN session_id TEXT NOT NULL DEFAULT 'default'")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_sagas_session ON sagas(session_id, id)")
        self._db.commit()

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        for key in ("metadata", "input", "result"):
            if key in value and value[key] is not None:
                value[key] = json.loads(value[key])
        return value

    def create_saga(self, saga: dict[str, Any]) -> None:
        self._execute("INSERT INTO sagas VALUES (?, ?, ?, ?, ?, ?, ?)", (saga["id"], saga["session_id"], saga["status"], json.dumps(saga["metadata"]), saga["created_at"], saga["updated_at"], saga.get("error")))

    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM sagas WHERE id=? AND session_id=?", (saga_id, session_id))

    def update_saga(self, saga_id: str, **changes: Any) -> None:
        self._update("sagas", saga_id, changes)

    def create_step(self, step: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            sequence = self._db.execute("SELECT COALESCE(MAX(sequence), 0) + 1 FROM steps WHERE saga_id=?", (step["saga_id"],)).fetchone()[0]
            self._db.execute("INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (step["id"], step["saga_id"], sequence, step["action"], json.dumps(step["input"]), step["status"], json.dumps(step.get("result")) if step.get("result") is not None else None, step.get("error"), step.get("compensation_attempts", 0), step["created_at"], step["updated_at"]))
            self._db.commit()
        return self.get_step(step["id"])  # type: ignore[return-value]

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM steps WHERE id=?", (step_id,))

    def list_steps(self, saga_id: str, statuses: set[str] | None = None, *, reverse: bool = False) -> list[dict[str, Any]]:
        sql, values = "SELECT * FROM steps WHERE saga_id=?", [saga_id]
        if statuses:
            sql += f" AND status IN ({','.join('?' for _ in statuses)})"
            values.extend(sorted(statuses))
        sql += " ORDER BY sequence " + ("DESC" if reverse else "ASC")
        with self._lock:
            return [self._decode(row) for row in self._db.execute(sql, values).fetchall()]  # type: ignore[misc]

    def update_step(self, step_id: str, **changes: Any) -> None:
        self._update("steps", step_id, changes)

    def pending_rollbacks(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._db.execute("SELECT DISTINCT s.id, s.session_id FROM sagas s LEFT JOIN steps p ON p.saga_id=s.id WHERE s.status IN ('FAILED','ROLLING_BACK','ROLLBACK_FAILED') OR (s.status='ACTIVE' AND p.status='EXECUTING')").fetchall()
        return [(row[0], row[1]) for row in rows]

    def _execute(self, sql: str, values: tuple[Any, ...]) -> None:
        with self._lock:
            self._db.execute(sql, values)
            self._db.commit()

    def _one(self, sql: str, values: tuple[Any, ...]) -> dict[str, Any] | None:
        with self._lock:
            return self._decode(self._db.execute(sql, values).fetchone())

    def _update(self, table: str, row_id: str, changes: dict[str, Any]) -> None:
        encoded = {key: json.dumps(value) if key in {"metadata", "input", "result"} and value is not None else value for key, value in changes.items()}
        assignments = ", ".join(f"{key}=?" for key in encoded)
        self._execute(f"UPDATE {table} SET {assignments} WHERE id=?", (*encoded.values(), row_id))
