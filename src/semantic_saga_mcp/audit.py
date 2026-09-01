from __future__ import annotations

import copy
import hashlib
import json
import threading
import uuid
from datetime import datetime
from typing import Any, Protocol


def _canonical(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash_event(previous_hash: str | None, event: dict[str, Any]) -> str:
    envelope = {"previous_hash": previous_hash, "event": event}
    return hashlib.sha256(_canonical(envelope).encode("utf-8")).hexdigest()


class AuditJournalProtocol(Protocol):
    def append(self, event: dict[str, Any]) -> dict[str, Any]: ...
    def list(self, saga_id: str, *, limit: int = 500, event_types: set[str] | None = None) -> list[dict[str, Any]]: ...
    def verify(self, saga_id: str) -> dict[str, Any]: ...


class StoreAuditJournal:
    """Append-only, per-saga hash-chained audit journal.

    The journal intentionally stores no action input/result payloads. It reuses
    the configured saga store's persistence backend: in-memory for SagaStore,
    the same SQLite database connection for SQLiteSagaStore, and the same
    PostgreSQL pool for PostgresSagaStore.
    """

    def __init__(self, store: Any) -> None:
        self.store = store
        self._memory: list[dict[str, Any]] = []
        self._memory_sequence = 0
        self._memory_lock = threading.RLock()
        if hasattr(store, "_db") and hasattr(store, "_lock"):
            self.kind = "sqlite"
            self._initialize_sqlite()
        elif hasattr(store, "_pool"):
            self.kind = "postgres"
            self._initialize_postgres()
        else:
            self.kind = "memory"

    def _initialize_sqlite(self) -> None:
        with self.store._lock:
            self.store._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    saga_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor_principal_id TEXT,
                    actor_type TEXT,
                    action TEXT,
                    action_version TEXT,
                    step_id TEXT,
                    node_id TEXT,
                    status TEXT,
                    data TEXT NOT NULL,
                    trace_id TEXT,
                    span_id TEXT,
                    previous_hash TEXT,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_saga_sequence ON audit_events(saga_id, sequence);
                CREATE INDEX IF NOT EXISTS idx_audit_saga_type ON audit_events(saga_id, event_type, sequence);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_saga_hash ON audit_events(saga_id, event_hash);
                """
            )
            self.store._db.commit()

    def _initialize_postgres(self) -> None:
        statements = """
        CREATE TABLE IF NOT EXISTS audit_events (
            sequence BIGSERIAL PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            saga_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            actor_principal_id TEXT,
            actor_type TEXT,
            action TEXT,
            action_version TEXT,
            step_id TEXT,
            node_id TEXT,
            status TEXT,
            data JSONB NOT NULL,
            trace_id TEXT,
            span_id TEXT,
            previous_hash TEXT,
            event_hash TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_audit_saga_sequence ON audit_events(saga_id, sequence);
        CREATE INDEX IF NOT EXISTS idx_audit_saga_type ON audit_events(saga_id, event_type, sequence);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_saga_hash ON audit_events(saga_id, event_hash);
        """
        with self.store._pool.connection() as conn:
            conn.execute(statements)
            conn.commit()

    @staticmethod
    def _body(event: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "id",
            "saga_id",
            "event_type",
            "actor_principal_id",
            "actor_type",
            "action",
            "action_version",
            "step_id",
            "node_id",
            "status",
            "data",
            "trace_id",
            "span_id",
            "created_at",
        }
        unknown = set(event) - allowed
        if unknown:
            raise ValueError(f"Unsupported audit event fields: {sorted(unknown)}")
        value = {key: copy.deepcopy(event.get(key)) for key in allowed}
        value["id"] = value.get("id") or str(uuid.uuid4())
        if not value.get("saga_id") or not value.get("event_type") or not value.get("created_at"):
            raise ValueError("Audit event requires saga_id, event_type, and created_at")
        data = value.get("data")
        if data is None:
            value["data"] = {}
        elif not isinstance(data, dict):
            raise ValueError("Audit event data must be an object")
        return value

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        value = dict(row)
        if isinstance(value.get("created_at"), datetime):
            value["created_at"] = value["created_at"].isoformat()
        if isinstance(value.get("data"), str):
            value["data"] = json.loads(value["data"])
        return value

    def append(self, event: dict[str, Any]) -> dict[str, Any]:
        body = self._body(event)
        if self.kind == "sqlite":
            with self.store._lock:
                self.store._db.execute("BEGIN IMMEDIATE")
                try:
                    row = self.store._db.execute(
                        "SELECT event_hash FROM audit_events WHERE saga_id=? ORDER BY sequence DESC LIMIT 1",
                        (body["saga_id"],),
                    ).fetchone()
                    previous_hash = row[0] if row else None
                    event_hash = _hash_event(previous_hash, body)
                    cursor = self.store._db.execute(
                        """INSERT INTO audit_events
                        (id,saga_id,event_type,actor_principal_id,actor_type,action,action_version,step_id,node_id,status,data,trace_id,span_id,previous_hash,event_hash,created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            body["id"], body["saga_id"], body["event_type"], body.get("actor_principal_id"),
                            body.get("actor_type"), body.get("action"), body.get("action_version"), body.get("step_id"),
                            body.get("node_id"), body.get("status"), json.dumps(body["data"], separators=(",", ":")),
                            body.get("trace_id"), body.get("span_id"), previous_hash, event_hash, body["created_at"],
                        ),
                    )
                    sequence = int(cursor.lastrowid)
                    self.store._db.commit()
                except Exception:
                    self.store._db.rollback()
                    raise
            return {"sequence": sequence, **body, "previous_hash": previous_hash, "event_hash": event_hash}

        if self.kind == "postgres":
            from psycopg.types.json import Jsonb

            with self.store._pool.connection() as conn:
                try:
                    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (body["saga_id"],))
                    row = conn.execute(
                        "SELECT event_hash FROM audit_events WHERE saga_id=%s ORDER BY sequence DESC LIMIT 1",
                        (body["saga_id"],),
                    ).fetchone()
                    previous_hash = row["event_hash"] if row else None
                    event_hash = _hash_event(previous_hash, body)
                    row = conn.execute(
                        """INSERT INTO audit_events
                        (id,saga_id,event_type,actor_principal_id,actor_type,action,action_version,step_id,node_id,status,data,trace_id,span_id,previous_hash,event_hash,created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                        (
                            body["id"], body["saga_id"], body["event_type"], body.get("actor_principal_id"),
                            body.get("actor_type"), body.get("action"), body.get("action_version"), body.get("step_id"),
                            body.get("node_id"), body.get("status"), Jsonb(body["data"]), body.get("trace_id"),
                            body.get("span_id"), previous_hash, event_hash, body["created_at"],
                        ),
                    ).fetchone()
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            return self._public(row)

        with self._memory_lock:
            previous_hash = None
            for row in reversed(self._memory):
                if row["saga_id"] == body["saga_id"]:
                    previous_hash = row["event_hash"]
                    break
            self._memory_sequence += 1
            stored = {
                "sequence": self._memory_sequence,
                **body,
                "previous_hash": previous_hash,
                "event_hash": _hash_event(previous_hash, body),
            }
            self._memory.append(copy.deepcopy(stored))
            return copy.deepcopy(stored)

    def list(self, saga_id: str, *, limit: int = 500, event_types: set[str] | None = None) -> list[dict[str, Any]]:
        if not 1 <= limit <= 5000:
            raise ValueError("audit event limit must be between 1 and 5000")
        if self.kind == "sqlite":
            sql = "SELECT * FROM audit_events WHERE saga_id=?"
            values: list[Any] = [saga_id]
            if event_types:
                sql += f" AND event_type IN ({','.join('?' for _ in event_types)})"
                values.extend(sorted(event_types))
            sql += " ORDER BY sequence ASC LIMIT ?"
            values.append(limit)
            with self.store._lock:
                rows = [dict(row) for row in self.store._db.execute(sql, values).fetchall()]
            return [self._public(row) for row in rows]

        if self.kind == "postgres":
            sql = "SELECT * FROM audit_events WHERE saga_id=%s"
            values = [saga_id]
            if event_types:
                sql += " AND event_type = ANY(%s)"
                values.append(sorted(event_types))
            sql += " ORDER BY sequence ASC LIMIT %s"
            values.append(limit)
            with self.store._pool.connection() as conn:
                rows = conn.execute(sql, values).fetchall()
            return [self._public(row) for row in rows]

        with self._memory_lock:
            rows = [
                copy.deepcopy(row)
                for row in self._memory
                if row["saga_id"] == saga_id and (not event_types or row["event_type"] in event_types)
            ]
        return rows[:limit]

    def _all(self, saga_id: str) -> list[dict[str, Any]]:
        """Read the complete chain for integrity verification, without API paging limits."""
        if self.kind == "sqlite":
            with self.store._lock:
                rows = [
                    dict(row)
                    for row in self.store._db.execute(
                        "SELECT * FROM audit_events WHERE saga_id=? ORDER BY sequence ASC",
                        (saga_id,),
                    ).fetchall()
                ]
            return [self._public(row) for row in rows]
        if self.kind == "postgres":
            with self.store._pool.connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM audit_events WHERE saga_id=%s ORDER BY sequence ASC",
                    (saga_id,),
                ).fetchall()
            return [self._public(row) for row in rows]
        with self._memory_lock:
            return [copy.deepcopy(row) for row in self._memory if row["saga_id"] == saga_id]

    def verify(self, saga_id: str) -> dict[str, Any]:
        rows = self._all(saga_id)
        previous_hash: str | None = None
        for row in rows:
            body = {
                key: row.get(key)
                for key in (
                    "id", "saga_id", "event_type", "actor_principal_id", "actor_type", "action",
                    "action_version", "step_id", "node_id", "status", "data", "trace_id", "span_id", "created_at"
                )
            }
            expected = _hash_event(previous_hash, body)
            if row.get("previous_hash") != previous_hash or row.get("event_hash") != expected:
                return {
                    "saga_id": saga_id,
                    "valid": False,
                    "events_checked": len(rows),
                    "failed_sequence": row.get("sequence"),
                }
            previous_hash = row["event_hash"]
        return {
            "saga_id": saga_id,
            "valid": True,
            "events_checked": len(rows),
            "head_hash": previous_hash,
        }
