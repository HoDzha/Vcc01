from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import Lock
from typing import Any

from app.db import ColumnInfo


logger = logging.getLogger("sqlite_backup")


@dataclass(frozen=True)
class SqliteBackupConfig:
    file_path: Path
    table_name: str = "users"


class SqliteBackupClient:
    def __init__(self, config: SqliteBackupConfig) -> None:
        self._config = config
        self._lock = Lock()

    @classmethod
    def with_default_path(cls, table_name: str = "users") -> "SqliteBackupClient":
        project_root = Path(__file__).resolve().parent.parent
        return cls(SqliteBackupConfig(file_path=project_root / "backup.sqlite3", table_name=table_name))

    @property
    def file_path(self) -> Path:
        return self._config.file_path

    @staticmethod
    def _to_sqlite_type(pg_data_type: str) -> str:
        data_type = pg_data_type.lower()
        if data_type in {"smallint", "integer", "bigint"}:
            return "INTEGER"
        if data_type in {"numeric", "decimal", "real", "double precision"}:
            return "REAL"
        if data_type in {"boolean"}:
            return "INTEGER"
        if data_type in {"date", "timestamp without time zone", "timestamp with time zone"}:
            return "TEXT"
        return "TEXT"

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat(sep=" ", timespec="seconds")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, bool):
            return int(value)
        return value

    def _ensure_table(self, connection: sqlite3.Connection, columns: list[ColumnInfo]) -> None:
        if not columns:
            raise ValueError("No columns provided for SQLite backup table creation.")

        definitions: list[str] = []
        for column in columns:
            column_sql = f'"{column.column_name}" {self._to_sqlite_type(column.data_type)}'
            definitions.append(column_sql)

        sql = f'CREATE TABLE IF NOT EXISTS "{self._config.table_name}" ({", ".join(definitions)})'
        connection.execute(sql)

    def backup_row(self, row_data: dict[str, Any], columns: list[ColumnInfo]) -> None:
        if not row_data:
            return

        with self._lock:
            with sqlite3.connect(self._config.file_path) as connection:
                self._ensure_table(connection, columns)

                ordered_columns = [column.column_name for column in columns if column.column_name in row_data]
                if not ordered_columns:
                    logger.warning("No matching columns to backup into SQLite.")
                    return

                placeholders = ", ".join("?" for _ in ordered_columns)
                columns_sql = ", ".join(f'"{name}"' for name in ordered_columns)
                values = [self._normalize_value(row_data[name]) for name in ordered_columns]

                insert_sql = (
                    f'INSERT INTO "{self._config.table_name}" ({columns_sql}) VALUES ({placeholders})'
                )
                connection.execute(insert_sql, values)
                connection.commit()

        logger.info(
            "SQLite backup appended table=%s file=%s columns=%s",
            self._config.table_name,
            self._config.file_path.name,
            len(ordered_columns),
        )
