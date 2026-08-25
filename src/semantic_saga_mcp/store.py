from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class SagaStore:
    """Small durable event store. Every mutation is committed before returning."""

    def __init__(self, path: str = ":memory:") -> None:
        if path != ":memory:":
            Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sagas (
              id TEXT PRIMARY KEY, status TEXT NOT NULL, metadata TEXT NOT NULL,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT
            );
            CREATE TABLE IF NOT EXISTS steps (
              id TEXT PRIMARY KEY, saga_id TEXT NOT NULL, sequence INTEGER NOT NULL,
              action TEXT NOT NULL, input TEXT NOT NULL, status TEXT NOT NULL,
              result TEXT, error TEXT, compensation_attempts INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
              UNIQUE(saga_id, sequence), FOREIGN KEY(saga_id) REFERENCES sagas(id)
            );
        """)
        self._db.commit()

    def execute(self, sql: str, values: tuple[Any, ...] = ()) -> None:
        with self._lock:
            self._db.execute(sql, values)
            self._db.commit()

    def one(self, sql: str, values: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute(sql, values).fetchone()
        return dict(row) if row else None

    def all(self, sql: str, values: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._db.execute(sql, values).fetchall()]

    @staticmethod
    def decode(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row:
            for key in ("metadata", "input", "result"):
                if key in row and row[key] is not None:
                    row[key] = json.loads(row[key])
        return row
