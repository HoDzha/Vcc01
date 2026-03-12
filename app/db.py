from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.sql import SQL, Identifier

from app.config import DatabaseConfig


logger = logging.getLogger("database")


@dataclass(frozen=True)
class ColumnInfo:
    table_schema: str
    table_name: str
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: str | None
    ordinal_position: int
    is_identity: bool


class DatabaseClient:
    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._has_logged_successful_connection = False

    def _connect(self):
        logger.debug(
            "Connecting to PostgreSQL host=%s port=%s db=%s sslmode=%s",
            self._config.host,
            self._config.port,
            self._config.database,
            self._config.sslmode,
        )
        try:
            connection = psycopg2.connect(
                host=self._config.host,
                port=self._config.port,
                dbname=self._config.database,
                user=self._config.user,
                password=self._config.password,
                sslmode=self._config.sslmode,
            )
        except Exception:
            logger.exception("PostgreSQL connection failed.")
            raise

        if not self._has_logged_successful_connection:
            logger.info(
                "Database connection established host=%s db=%s",
                self._config.host,
                self._config.database,
            )
            self._has_logged_successful_connection = True

        return connection

    def execute_query(self, query: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return list(cursor.fetchall())
                return []

    def get_schema_columns(self, schema: str = "public") -> list[ColumnInfo]:
        query = """
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES' AS is_nullable,
                c.column_default,
                c.ordinal_position,
                c.is_identity = 'YES' AS is_identity
            FROM information_schema.columns AS c
            WHERE c.table_schema = %s
            ORDER BY c.table_name, c.ordinal_position;
        """
        rows = self.execute_query(query, (schema,))
        return [ColumnInfo(**row) for row in rows]

    def get_table_columns(self, table_name: str, schema: str = "public") -> list[ColumnInfo]:
        query = """
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES' AS is_nullable,
                c.column_default,
                c.ordinal_position,
                c.is_identity = 'YES' AS is_identity
            FROM information_schema.columns AS c
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position;
        """
        rows = self.execute_query(query, (schema, table_name))
        return [ColumnInfo(**row) for row in rows]

    def get_table_column_names(self, table_name: str, schema: str = "public") -> list[str]:
        return [column.column_name for column in self.get_table_columns(table_name=table_name, schema=schema)]

    def ensure_requests_table(self, schema: str = "public", table_name: str = "it_team_requests") -> None:
        query = SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                task TEXT NOT NULL,
                priority VARCHAR(20) NOT NULL,
                author TEXT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'Новая',
                telegram_user_id BIGINT,
                telegram_username TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        ).format(Identifier(schema), Identifier(table_name))

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

        logger.info("Ensured requests table exists: %s.%s", schema, table_name)

    def insert_request_record(
        self,
        task: str,
        priority: str,
        author: str,
        status: str,
        telegram_user_id: int,
        telegram_username: str | None,
        schema: str = "public",
        table_name: str = "it_team_requests",
    ) -> dict[str, Any]:
        insert_sql = SQL(
            """
            INSERT INTO {}.{} (task, priority, author, status, telegram_user_id, telegram_username)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, task, priority, author, status, telegram_user_id, telegram_username, created_at, updated_at;
            """
        ).format(Identifier(schema), Identifier(table_name))

        params = (task, priority, author, status, telegram_user_id, telegram_username or "")

        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(insert_sql, params)
                row = cursor.fetchone()

        if row is None:
            raise ValueError("Не удалось сохранить заявку в БД.")

        logger.info("Saved IT request id=%s user_id=%s", row["id"], telegram_user_id)
        return dict(row)

    def get_recent_requests(
        self,
        limit: int = 10,
        schema: str = "public",
        table_name: str = "it_team_requests",
    ) -> list[dict[str, Any]]:
        query = SQL(
            """
            SELECT id, task, priority, author, status, created_at
            FROM {}.{}
            ORDER BY id DESC
            LIMIT %s;
            """
        ).format(Identifier(schema), Identifier(table_name))

        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def get_all_table_rows(
        self,
        schema: str = "public",
        table_name: str = "it_team_requests",
    ) -> list[dict[str, Any]]:
        query = SQL("SELECT * FROM {}.{} ORDER BY id ASC").format(
            Identifier(schema),
            Identifier(table_name),
        )

        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def get_schema_overview_text(self, schema: str = "public") -> str:
        columns = self.get_schema_columns(schema=schema)
        if not columns:
            return f"В схеме '{schema}' таблицы не найдены."

        lines: list[str] = []
        current_table = ""

        for column in columns:
            if column.table_name != current_table:
                current_table = column.table_name
                lines.append(f"\nТаблица: {current_table}")

            nullable = "NULL" if column.is_nullable else "NOT NULL"
            default = f", default={column.column_default}" if column.column_default else ""
            lines.append(f"  - {column.column_name}: {column.data_type}, {nullable}{default}")

        return "\n".join(lines).strip()
