from __future__ import annotations

import copy
import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


class LeaseLostError(RuntimeError):
    """A stale worker attempted to mutate a saga after losing its lease."""


@runtime_checkable
class SagaStoreProtocol(Protocol):
    """Storage contract for durable and distributed saga state.

    A horizontally scaled adapter must atomically allocate per-saga step
    sequences, lease recovery work to one worker, and reject writes carrying a
    stale fencing token. SQLite and the in-memory store implement the same
    contract so coordinator behavior remains identical in local development.
    """

    def create_saga(self, saga: dict[str, Any]) -> None: ...
    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None: ...
    def update_saga(self, saga_id: str, *, fence_token: int | None = None, **changes: Any) -> None: ...
    def create_step(self, step: dict[str, Any], *, fence_token: int | None = None) -> dict[str, Any]: ...
    def get_step(self, step_id: str) -> dict[str, Any] | None: ...
    def list_steps(self, saga_id: str, statuses: set[str] | None = None, *, reverse: bool = False) -> list[dict[str, Any]]: ...
    def update_step(self, step_id: str, *, fence_token: int | None = None, **changes: Any) -> None: ...
    def pending_rollbacks(self) -> list[tuple[str, str]]: ...
    def acquire_saga_lease(self, saga_id: str, session_id: str, worker_id: str, lease_seconds: float) -> int | None: ...
    def renew_saga_lease(self, saga_id: str, worker_id: str, fence_token: int, lease_seconds: float) -> bool: ...
    def release_saga_lease(self, saga_id: str, worker_id: str, fence_token: int) -> None: ...
    def claim_pending_rollbacks(self, worker_id: str, lease_seconds: float, limit: int = 100) -> list[tuple[str, str, int]]: ...


def _is_pending(saga: dict[str, Any], steps: list[dict[str, Any]]) -> bool:
    return saga["status"] in {"FAILED", "ROLLING_BACK", "ROLLBACK_FAILED"} or (
        saga["status"] == "ACTIVE" and any(step["status"] == "EXECUTING" for step in steps)
    )


class SagaStore:
    """Thread-safe, dependency-free store for tests and ephemeral servers."""

    def __init__(self) -> None:
        self._sagas: dict[str, dict[str, Any]] = {}
        self._steps: dict[str, dict[str, Any]] = {}
        self._next_sequence: dict[str, int] = {}
        self._leases: dict[str, tuple[str, float, int]] = {}
        self._fences: dict[str, int] = {}
        self._lock = threading.RLock()

    def create_saga(self, saga: dict[str, Any]) -> None:
        with self._lock:
            self._sagas[saga["id"]] = copy.deepcopy(saga)
            self._next_sequence[saga["id"]] = 1
            self._fences[saga["id"]] = 0

    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            saga = self._sagas.get(saga_id)
            return copy.deepcopy(saga) if saga and saga["session_id"] == session_id else None

    def _assert_fence(self, saga_id: str, fence_token: int | None) -> None:
        if fence_token is not None and self._fences.get(saga_id) != fence_token:
            raise LeaseLostError(f"Stale fencing token for saga {saga_id}")

    def update_saga(self, saga_id: str, *, fence_token: int | None = None, **changes: Any) -> None:
        with self._lock:
            self._assert_fence(saga_id, fence_token)
            self._sagas[saga_id].update(copy.deepcopy(changes))

    def create_step(self, step: dict[str, Any], *, fence_token: int | None = None) -> dict[str, Any]:
        with self._lock:
            self._assert_fence(step["saga_id"], fence_token)
            stored = copy.deepcopy(step)
            stored["sequence"] = self._next_sequence[step["saga_id"]]
            self._next_sequence[step["saga_id"]] += 1
            self._steps[stored["id"]] = stored
            return copy.deepcopy(stored)

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        with self._lock:
            return copy.deepcopy(self._steps.get(step_id))

    def list_steps(self, saga_id: str, statuses: set[str] | None = None, *, reverse: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            rows = [
                copy.deepcopy(row)
                for row in self._steps.values()
                if row["saga_id"] == saga_id and (statuses is None or row["status"] in statuses)
            ]
        return sorted(rows, key=lambda row: row["sequence"], reverse=reverse)

    def update_step(self, step_id: str, *, fence_token: int | None = None, **changes: Any) -> None:
        with self._lock:
            step = self._steps[step_id]
            self._assert_fence(step["saga_id"], fence_token)
            step.update(copy.deepcopy(changes))

    def pending_rollbacks(self) -> list[tuple[str, str]]:
        with self._lock:
            result = []
            for saga in self._sagas.values():
                steps = [row for row in self._steps.values() if row["saga_id"] == saga["id"]]
                if _is_pending(saga, steps):
                    result.append((saga["id"], saga["session_id"]))
            return result

    def acquire_saga_lease(self, saga_id: str, session_id: str, worker_id: str, lease_seconds: float) -> int | None:
        with self._lock:
            saga = self._sagas.get(saga_id)
            if not saga or saga["session_id"] != session_id:
                return None
            current = self._leases.get(saga_id)
            now_value = time.monotonic()
            if current is not None and current[1] > now_value:
                return None
            token = self._fences[saga_id] + 1
            self._fences[saga_id] = token
            self._leases[saga_id] = (worker_id, now_value + lease_seconds, token)
            return token

    def renew_saga_lease(self, saga_id: str, worker_id: str, fence_token: int, lease_seconds: float) -> bool:
        with self._lock:
            current = self._leases.get(saga_id)
            now_value = time.monotonic()
            if not current or current[0] != worker_id or current[2] != fence_token or current[1] < now_value:
                return False
            self._leases[saga_id] = (worker_id, now_value + lease_seconds, fence_token)
            return True

    def release_saga_lease(self, saga_id: str, worker_id: str, fence_token: int) -> None:
        with self._lock:
            current = self._leases.get(saga_id)
            if current and current[0] == worker_id and current[2] == fence_token:
                self._leases.pop(saga_id, None)

    def claim_pending_rollbacks(self, worker_id: str, lease_seconds: float, limit: int = 100) -> list[tuple[str, str, int]]:
        claims: list[tuple[str, str, int]] = []
        with self._lock:
            candidates = list(self.pending_rollbacks())[:limit]
            for saga_id, session_id in candidates:
                token = self.acquire_saga_lease(saga_id, session_id, worker_id, lease_seconds)
                if token is not None:
                    claims.append((saga_id, session_id, token))
        return claims


class SQLiteSagaStore:
    """Durable single-node SQLite implementation of :class:`SagaStoreProtocol`."""

    def __init__(self, path: str) -> None:
        Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sagas (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                tenant_id TEXT,
                creator_principal_id TEXT,
                status TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT,
                next_sequence INTEGER NOT NULL DEFAULT 1,
                lease_owner TEXT,
                lease_until REAL,
                fence_token INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS steps (
                id TEXT PRIMARY KEY,
                saga_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                action TEXT NOT NULL,
                input TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                error TEXT,
                compensation_attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                fence_token INTEGER NOT NULL DEFAULT 0,
                UNIQUE(saga_id, sequence)
            );
        """)
        self._ensure_column("sagas", "session_id", "TEXT NOT NULL DEFAULT 'default'")
        self._ensure_column("sagas", "tenant_id", "TEXT")
        self._ensure_column("sagas", "creator_principal_id", "TEXT")
        self._ensure_column("sagas", "next_sequence", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column("sagas", "lease_owner", "TEXT")
        self._ensure_column("sagas", "lease_until", "REAL")
        self._ensure_column("sagas", "fence_token", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("steps", "fence_token", "INTEGER NOT NULL DEFAULT 0")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_sagas_session ON sagas(session_id, id)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_sagas_recovery ON sagas(status, lease_until)")
        self._db.execute(
            """UPDATE sagas SET next_sequence=COALESCE(
                (SELECT MAX(sequence) + 1 FROM steps WHERE steps.saga_id=sagas.id), 1
            ) WHERE next_sequence < COALESCE(
                (SELECT MAX(sequence) + 1 FROM steps WHERE steps.saga_id=sagas.id), 1
            )"""
        )
        self._db.commit()

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        columns = {row[1] for row in self._db.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self._db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        for key in ("metadata", "input", "result"):
            if key in value and value[key] is not None:
                value[key] = json.loads(value[key])
        for internal in ("next_sequence", "lease_owner", "lease_until", "fence_token"):
            value.pop(internal, None)
        return value

    def create_saga(self, saga: dict[str, Any]) -> None:
        self._execute(
            """INSERT INTO sagas
            (id,session_id,tenant_id,creator_principal_id,status,metadata,created_at,updated_at,error,next_sequence,lease_owner,lease_until,fence_token)
            VALUES (?,?,?,?,?,?,?,?,?,1,NULL,NULL,0)""",
            (
                saga["id"], saga["session_id"], saga.get("tenant_id"), saga.get("creator_principal_id"),
                saga["status"], json.dumps(saga["metadata"]), saga["created_at"], saga["updated_at"], saga.get("error"),
            ),
        )

    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM sagas WHERE id=? AND session_id=?", (saga_id, session_id))

    def _assert_fence_locked(self, saga_id: str, fence_token: int | None) -> None:
        if fence_token is None:
            return
        row = self._db.execute("SELECT fence_token FROM sagas WHERE id=?", (saga_id,)).fetchone()
        if row is None or row[0] != fence_token:
            raise LeaseLostError(f"Stale fencing token for saga {saga_id}")

    def update_saga(self, saga_id: str, *, fence_token: int | None = None, **changes: Any) -> None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._assert_fence_locked(saga_id, fence_token)
                self._update_locked("sagas", saga_id, changes)
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def create_step(self, step: dict[str, Any], *, fence_token: int | None = None) -> dict[str, Any]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._assert_fence_locked(step["saga_id"], fence_token)
                row = self._db.execute("SELECT next_sequence FROM sagas WHERE id=?", (step["saga_id"],)).fetchone()
                if row is None:
                    raise KeyError(step["saga_id"])
                sequence = row[0]
                self._db.execute("UPDATE sagas SET next_sequence=next_sequence+1 WHERE id=?", (step["saga_id"],))
                self._db.execute(
                    """INSERT INTO steps
                    (id,saga_id,sequence,action,input,status,result,error,compensation_attempts,created_at,updated_at,fence_token)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        step["id"], step["saga_id"], sequence, step["action"], json.dumps(step["input"]), step["status"],
                        json.dumps(step.get("result")) if step.get("result") is not None else None, step.get("error"),
                        step.get("compensation_attempts", 0), step["created_at"], step["updated_at"], fence_token or 0,
                    ),
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
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

    def update_step(self, step_id: str, *, fence_token: int | None = None, **changes: Any) -> None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute("SELECT saga_id FROM steps WHERE id=?", (step_id,)).fetchone()
                if row is None:
                    raise KeyError(step_id)
                self._assert_fence_locked(row[0], fence_token)
                self._update_locked("steps", step_id, changes)
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def pending_rollbacks(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._db.execute(
                """SELECT DISTINCT s.id, s.session_id FROM sagas s
                WHERE s.status IN ('FAILED','ROLLING_BACK','ROLLBACK_FAILED')
                   OR (s.status='ACTIVE' AND EXISTS (SELECT 1 FROM steps p WHERE p.saga_id=s.id AND p.status='EXECUTING'))"""
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def acquire_saga_lease(self, saga_id: str, session_id: str, worker_id: str, lease_seconds: float) -> int | None:
        now_value = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    """UPDATE sagas
                    SET lease_owner=?, lease_until=?, fence_token=fence_token+1
                    WHERE id=? AND session_id=? AND (lease_until IS NULL OR lease_until<=?)""",
                    (worker_id, now_value + lease_seconds, saga_id, session_id, now_value),
                )
                if cursor.rowcount != 1:
                    self._db.rollback()
                    return None
                token = self._db.execute("SELECT fence_token FROM sagas WHERE id=?", (saga_id,)).fetchone()[0]
                self._db.commit()
                return token
            except Exception:
                self._db.rollback()
                raise

    def renew_saga_lease(self, saga_id: str, worker_id: str, fence_token: int, lease_seconds: float) -> bool:
        now_value = time.time()
        with self._lock:
            cursor = self._db.execute(
                """UPDATE sagas SET lease_until=?
                WHERE id=? AND lease_owner=? AND fence_token=? AND lease_until>=?""",
                (now_value + lease_seconds, saga_id, worker_id, fence_token, now_value),
            )
            self._db.commit()
            return cursor.rowcount == 1

    def release_saga_lease(self, saga_id: str, worker_id: str, fence_token: int) -> None:
        self._execute(
            "UPDATE sagas SET lease_owner=NULL, lease_until=NULL WHERE id=? AND lease_owner=? AND fence_token=?",
            (saga_id, worker_id, fence_token),
        )

    def claim_pending_rollbacks(self, worker_id: str, lease_seconds: float, limit: int = 100) -> list[tuple[str, str, int]]:
        now_value = time.time()
        claims: list[tuple[str, str, int]] = []
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                rows = self._db.execute(
                    """SELECT s.id, s.session_id FROM sagas s
                    WHERE (s.status IN ('FAILED','ROLLING_BACK','ROLLBACK_FAILED')
                       OR (s.status='ACTIVE' AND EXISTS (SELECT 1 FROM steps p WHERE p.saga_id=s.id AND p.status='EXECUTING')))
                      AND (s.lease_until IS NULL OR s.lease_until<=?)
                    ORDER BY s.updated_at LIMIT ?""",
                    (now_value, limit),
                ).fetchall()
                for row in rows:
                    self._db.execute(
                        "UPDATE sagas SET lease_owner=?, lease_until=?, fence_token=fence_token+1 WHERE id=?",
                        (worker_id, now_value + lease_seconds, row[0]),
                    )
                    token = self._db.execute("SELECT fence_token FROM sagas WHERE id=?", (row[0],)).fetchone()[0]
                    claims.append((row[0], row[1], token))
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
        return claims

    def _execute(self, sql: str, values: tuple[Any, ...]) -> None:
        with self._lock:
            self._db.execute(sql, values)
            self._db.commit()

    def _one(self, sql: str, values: tuple[Any, ...]) -> dict[str, Any] | None:
        with self._lock:
            return self._decode(self._db.execute(sql, values).fetchone())

    def _update_locked(self, table: str, row_id: str, changes: dict[str, Any]) -> None:
        if not changes:
            return
        encoded = {
            key: json.dumps(value) if key in {"metadata", "input", "result"} and value is not None else value
            for key, value in changes.items()
        }
        assignments = ", ".join(f"{key}=?" for key in encoded)
        self._db.execute(f"UPDATE {table} SET {assignments} WHERE id=?", (*encoded.values(), row_id))


class PostgresSagaStore:
    """Horizontally scalable PostgreSQL store with leases and fencing tokens."""

    _SAGA_UPDATE_FIELDS = {"status", "metadata", "updated_at", "error", "tenant_id", "creator_principal_id"}
    _STEP_UPDATE_FIELDS = {"status", "result", "error", "compensation_attempts", "updated_at"}

    def __init__(self, dsn: str, *, min_pool_size: int = 1, max_pool_size: int = 10) -> None:
        try:
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool
        except ImportError as exc:  # pragma: no cover - dependency is packaged, defensive only
            raise RuntimeError("PostgreSQL support requires psycopg and psycopg-pool") from exc

        self._dict_row = dict_row
        self._pool = ConnectionPool(
            conninfo=dsn,
            min_size=min_pool_size,
            max_size=max_pool_size,
            kwargs={"row_factory": dict_row},
            open=True,
        )
        self._pool.wait(timeout=15)
        self._initialize_schema()

    def close(self) -> None:
        self._pool.close()

    def _initialize_schema(self) -> None:
        statements = """
        CREATE TABLE IF NOT EXISTS sagas (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            tenant_id TEXT,
            creator_principal_id TEXT,
            status TEXT NOT NULL,
            metadata JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            error TEXT,
            next_sequence BIGINT NOT NULL DEFAULT 1,
            lease_owner TEXT,
            lease_until TIMESTAMPTZ,
            fence_token BIGINT NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS steps (
            id TEXT PRIMARY KEY,
            saga_id TEXT NOT NULL REFERENCES sagas(id) ON DELETE CASCADE,
            sequence BIGINT NOT NULL,
            action TEXT NOT NULL,
            input JSONB NOT NULL,
            status TEXT NOT NULL,
            result JSONB,
            error TEXT,
            compensation_attempts INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            fence_token BIGINT NOT NULL DEFAULT 0,
            UNIQUE(saga_id, sequence)
        );
        CREATE INDEX IF NOT EXISTS idx_sagas_session ON sagas(session_id, id);
        CREATE INDEX IF NOT EXISTS idx_sagas_recovery ON sagas(status, lease_until, updated_at);
        CREATE INDEX IF NOT EXISTS idx_steps_saga_status ON steps(saga_id, status, sequence);
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(statements)
            conn.commit()

    @staticmethod
    def _public(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        for key in ("created_at", "updated_at"):
            if isinstance(value.get(key), datetime):
                value[key] = value[key].isoformat()
        for internal in ("next_sequence", "lease_owner", "lease_until", "fence_token"):
            value.pop(internal, None)
        return value

    @staticmethod
    def _json(value: Any) -> Any:
        from psycopg.types.json import Jsonb
        return Jsonb(value)

    def create_saga(self, saga: dict[str, Any]) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                """INSERT INTO sagas
                (id,session_id,tenant_id,creator_principal_id,status,metadata,created_at,updated_at,error)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    saga["id"], saga["session_id"], saga.get("tenant_id"), saga.get("creator_principal_id"),
                    saga["status"], self._json(saga["metadata"]), saga["created_at"], saga["updated_at"], saga.get("error"),
                ),
            )
            conn.commit()

    def get_saga(self, saga_id: str, session_id: str) -> dict[str, Any] | None:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT * FROM sagas WHERE id=%s AND session_id=%s", (saga_id, session_id)).fetchone()
        return self._public(row)

    def update_saga(self, saga_id: str, *, fence_token: int | None = None, **changes: Any) -> None:
        unknown = set(changes) - self._SAGA_UPDATE_FIELDS
        if unknown:
            raise ValueError(f"Unsupported saga update fields: {sorted(unknown)}")
        if not changes:
            return
        encoded = {key: self._json(value) if key == "metadata" and value is not None else value for key, value in changes.items()}
        assignments = ", ".join(f"{key}=%s" for key in encoded)
        values = list(encoded.values()) + [saga_id]
        where = "id=%s"
        if fence_token is not None:
            where += " AND fence_token=%s"
            values.append(fence_token)
        with self._pool.connection() as conn:
            cursor = conn.execute(f"UPDATE sagas SET {assignments} WHERE {where}", values)
            if fence_token is not None and cursor.rowcount != 1:
                conn.rollback()
                raise LeaseLostError(f"Stale fencing token for saga {saga_id}")
            conn.commit()

    def create_step(self, step: dict[str, Any], *, fence_token: int | None = None) -> dict[str, Any]:
        with self._pool.connection() as conn:
            try:
                where = "id=%s"
                values: list[Any] = [step["saga_id"]]
                if fence_token is not None:
                    where += " AND fence_token=%s"
                    values.append(fence_token)
                row = conn.execute(
                    f"UPDATE sagas SET next_sequence=next_sequence+1 WHERE {where} RETURNING next_sequence-1 AS sequence",
                    values,
                ).fetchone()
                if row is None:
                    raise LeaseLostError(f"Stale fencing token for saga {step['saga_id']}")
                sequence = row["sequence"]
                result = conn.execute(
                    """INSERT INTO steps
                    (id,saga_id,sequence,action,input,status,result,error,compensation_attempts,created_at,updated_at,fence_token)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                    (
                        step["id"], step["saga_id"], sequence, step["action"], self._json(step["input"]), step["status"],
                        self._json(step.get("result")) if step.get("result") is not None else None, step.get("error"),
                        step.get("compensation_attempts", 0), step["created_at"], step["updated_at"], fence_token or 0,
                    ),
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._public(result)  # type: ignore[return-value]

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT * FROM steps WHERE id=%s", (step_id,)).fetchone()
        return self._public(row)

    def list_steps(self, saga_id: str, statuses: set[str] | None = None, *, reverse: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM steps WHERE saga_id=%s"
        values: list[Any] = [saga_id]
        if statuses:
            sql += " AND status = ANY(%s)"
            values.append(sorted(statuses))
        sql += " ORDER BY sequence " + ("DESC" if reverse else "ASC")
        with self._pool.connection() as conn:
            rows = conn.execute(sql, values).fetchall()
        return [self._public(row) for row in rows]  # type: ignore[misc]

    def update_step(self, step_id: str, *, fence_token: int | None = None, **changes: Any) -> None:
        unknown = set(changes) - self._STEP_UPDATE_FIELDS
        if unknown:
            raise ValueError(f"Unsupported step update fields: {sorted(unknown)}")
        if not changes:
            return
        encoded = {key: self._json(value) if key == "result" and value is not None else value for key, value in changes.items()}
        assignments = ", ".join(f"{key}=%s" for key in encoded)
        values = list(encoded.values()) + [step_id]
        if fence_token is None:
            sql = f"UPDATE steps SET {assignments} WHERE id=%s"
        else:
            sql = f"""UPDATE steps p SET {assignments}
                FROM sagas s WHERE p.id=%s AND s.id=p.saga_id AND s.fence_token=%s"""
            values.append(fence_token)
        with self._pool.connection() as conn:
            cursor = conn.execute(sql, values)
            if fence_token is not None and cursor.rowcount != 1:
                conn.rollback()
                raise LeaseLostError(f"Stale fencing token for step {step_id}")
            conn.commit()

    def pending_rollbacks(self) -> list[tuple[str, str]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT s.id, s.session_id FROM sagas s
                WHERE s.status IN ('FAILED','ROLLING_BACK','ROLLBACK_FAILED')
                   OR (s.status='ACTIVE' AND EXISTS (SELECT 1 FROM steps p WHERE p.saga_id=s.id AND p.status='EXECUTING'))"""
            ).fetchall()
        return [(row["id"], row["session_id"]) for row in rows]

    def acquire_saga_lease(self, saga_id: str, session_id: str, worker_id: str, lease_seconds: float) -> int | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                """UPDATE sagas SET
                    lease_owner=%s,
                    lease_until=clock_timestamp() + (%s * interval '1 second'),
                    fence_token=fence_token+1
                WHERE id=%s AND session_id=%s
                  AND (lease_until IS NULL OR lease_until<=clock_timestamp())
                RETURNING fence_token""",
                (worker_id, lease_seconds, saga_id, session_id),
            ).fetchone()
            conn.commit()
        return int(row["fence_token"]) if row else None

    def renew_saga_lease(self, saga_id: str, worker_id: str, fence_token: int, lease_seconds: float) -> bool:
        with self._pool.connection() as conn:
            cursor = conn.execute(
                """UPDATE sagas SET lease_until=clock_timestamp() + (%s * interval '1 second')
                WHERE id=%s AND lease_owner=%s AND fence_token=%s AND lease_until>=clock_timestamp()""",
                (lease_seconds, saga_id, worker_id, fence_token),
            )
            conn.commit()
        return cursor.rowcount == 1

    def release_saga_lease(self, saga_id: str, worker_id: str, fence_token: int) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE sagas SET lease_owner=NULL, lease_until=NULL WHERE id=%s AND lease_owner=%s AND fence_token=%s",
                (saga_id, worker_id, fence_token),
            )
            conn.commit()

    def claim_pending_rollbacks(self, worker_id: str, lease_seconds: float, limit: int = 100) -> list[tuple[str, str, int]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """WITH candidates AS (
                    SELECT s.id FROM sagas s
                    WHERE (s.status IN ('FAILED','ROLLING_BACK','ROLLBACK_FAILED')
                       OR (s.status='ACTIVE' AND EXISTS (SELECT 1 FROM steps p WHERE p.saga_id=s.id AND p.status='EXECUTING')))
                      AND (s.lease_until IS NULL OR s.lease_until<=clock_timestamp())
                    ORDER BY s.updated_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                )
                UPDATE sagas s SET
                    lease_owner=%s,
                    lease_until=clock_timestamp() + (%s * interval '1 second'),
                    fence_token=s.fence_token+1
                FROM candidates c WHERE s.id=c.id
                RETURNING s.id, s.session_id, s.fence_token""",
                (limit, worker_id, lease_seconds),
            ).fetchall()
            conn.commit()
        return [(row["id"], row["session_id"], int(row["fence_token"])) for row in rows]
