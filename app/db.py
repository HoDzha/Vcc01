from __future__ import annotations

import logging
import random
import string
from datetime import date, datetime, timedelta
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
        logger.debug("Executing query text_len=%s params_count=%s", len(query), len(params or ()))
        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    rows = list(cursor.fetchall())
                    logger.debug("Query returned rows=%s", len(rows))
                    return rows
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
        result = [ColumnInfo(**row) for row in rows]
        logger.debug("Loaded table columns schema=%s table=%s count=%s", schema, table_name, len(result))
        return result

    def get_table_column_names(self, table_name: str, schema: str = "public") -> list[str]:
        names = [column.column_name for column in self.get_table_columns(table_name=table_name, schema=schema)]
        logger.info("Table columns fetched schema=%s table=%s count=%s", schema, table_name, len(names))
        return names

    @staticmethod
    def _random_text(length: int = 10) -> str:
        alphabet = string.ascii_letters
        return "".join(random.choice(alphabet) for _ in range(length))

    @staticmethod
    def _random_value(column: ColumnInfo) -> Any:
        name = column.column_name.lower()
        data_type = column.data_type.lower()

        if name == "city":
            cities = [
                "Москва",
                "Санкт-Петербург",
                "Новосибирск",
                "Екатеринбург",
                "Казань",
                "Нижний Новгород",
                "Челябинск",
                "Самара",
                "Ростов-на-Дону",
                "Уфа",
            ]
            return random.choice(cities)

        if name == "profession":
            professions = [
                "Разработчик",
                "Дизайнер",
                "Маркетолог",
                "Аналитик",
                "Учитель",
                "Инженер",
                "Врач",
                "Менеджер",
                "Экономист",
                "Юрист",
            ]
            return random.choice(professions)

        if name == "hobby":
            hobbies = [
                "Чтение",
                "Путешествия",
                "Спорт",
                "Музыка",
                "Игры",
                "Рисование",
                "Фотография",
                "Кулинария",
                "Программирование",
                "Фильмы и сериалы",
            ]
            return random.choice(hobbies)

        if name == "random_number":
            return random.randint(1, 1000)

        if name == "random_score":
            return round(1 + random.random() * 9, 1)

        if name == "is_active":
            return random.choice([True, False])

        if name == "random_color":
            colors = [
                "Красный",
                "Зелёный",
                "Синий",
                "Жёлтый",
                "Фиолетовый",
                "Оранжевый",
                "Розовый",
                "Чёрный",
                "Белый",
                "Бирюзовый",
            ]
            return random.choice(colors)

        if "card" in name:
            return random.randint(1000, 9999)

        if data_type in {"integer", "bigint", "smallint"}:
            return random.randint(1, 10000)
        if data_type in {"real", "double precision", "numeric", "decimal"}:
            return round(random.uniform(100.0, 5000.0), 2)
        if data_type in {"text", "character varying", "character"}:
            return f"random_{DatabaseClient._random_text(8)}"
        if data_type == "date":
            days_ago = random.randint(1, 3650)
            return date.today() - timedelta(days=days_ago)
        if "timestamp" in data_type:
            return datetime.utcnow()
        if data_type == "boolean":
            return random.choice([True, False])

        return None

    @staticmethod
    def _find_column(columns: list[ColumnInfo], candidates: set[str]) -> ColumnInfo | None:
        by_exact_name = {column.column_name.lower(): column for column in columns}
        for candidate in candidates:
            found = by_exact_name.get(candidate)
            if found:
                return found

        for column in columns:
            name = column.column_name.lower()
            if any(candidate in name for candidate in candidates):
                return column

        return None

    @staticmethod
    def _is_autogenerated(column: ColumnInfo) -> bool:
        default_expr = (column.column_default or "").lower()
        return column.is_identity or default_expr.startswith("nextval(")

    def insert_survey_record(
        self,
        user_id: int,
        username: str | None,
        full_name: str,
        birth_date: date,
        schema: str = "public",
        table_name: str = "users",
    ) -> dict[str, Any]:
        logger.info("Saving survey record schema=%s table=%s user_id=%s", schema, table_name, user_id)
        columns = self.get_table_columns(table_name=table_name, schema=schema)
        if not columns:
            raise ValueError(f"Таблица {schema}.{table_name} не найдена.")

        user_id_column = self._find_column(columns, {"user_id"})
        username_column = self._find_column(columns, {"username"})
        full_name_column = self._find_column(columns, {"full_name", "fio", "name"})
        birth_date_column = self._find_column(
            columns,
            {"birthdate", "birtdate", "birth_date", "date_of_birth", "birthday", "birth"},
        )

        if user_id_column is None:
            raise ValueError("Не найдена колонка для user_id в таблице.")
        if username_column is None:
            raise ValueError("Не найдена колонка для username в таблице.")
        if full_name_column is None:
            raise ValueError("Не найдена колонка для ФИО в таблице.")
        if birth_date_column is None:
            raise ValueError("Не найдена колонка для даты рождения в таблице.")

        payload: dict[str, Any] = {
            user_id_column.column_name: user_id,
            username_column.column_name: username or "",
            full_name_column.column_name: full_name,
            birth_date_column.column_name: birth_date,
        }

        for column in columns:
            if self._is_autogenerated(column):
                continue
            if column.column_name in payload:
                continue
            payload[column.column_name] = self._random_value(column)

        if not payload:
            raise ValueError("Нет данных для вставки в таблицу.")

        column_names = list(payload.keys())
        values = [payload[name] for name in column_names]

        insert_sql = SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            Identifier(schema),
            Identifier(table_name),
            SQL(", ").join(Identifier(name) for name in column_names),
            SQL(", ").join(SQL("%s") for _ in values),
        )

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(insert_sql, values)

        logger.info("Survey record saved user_id=%s columns=%s", user_id, len(column_names))
        return payload

    def get_recent_survey_rows(
        self,
        limit: int = 5,
        schema: str = "public",
        table_name: str = "users",
    ) -> list[dict[str, Any]]:
        logger.debug("Loading recent survey rows limit=%s schema=%s table=%s", limit, schema, table_name)
        columns = self.get_table_columns(table_name=table_name, schema=schema)
        if not columns:
            return []

        order_column = self._find_column(columns, {"id"})
        order_sql = (
            SQL(" ORDER BY {} DESC").format(Identifier(order_column.column_name))
            if order_column
            else SQL("")
        )

        query = SQL("SELECT * FROM {}.{}{} LIMIT %s").format(
            Identifier(schema),
            Identifier(table_name),
            order_sql,
        )

        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (limit,))
                rows = list(cursor.fetchall())
                logger.info("Recent survey rows loaded count=%s", len(rows))
                return rows

    def get_all_table_rows(
        self,
        schema: str = "public",
        table_name: str = "users",
    ) -> list[dict[str, Any]]:
        logger.info("Loading all table rows schema=%s table=%s", schema, table_name)
        columns = self.get_table_columns(table_name=table_name, schema=schema)
        if not columns:
            return []

        order_column = self._find_column(columns, {"id"})
        order_sql = (
            SQL(" ORDER BY {} ASC").format(Identifier(order_column.column_name))
            if order_column
            else SQL("")
        )

        query = SQL("SELECT * FROM {}.{}{}").format(
            Identifier(schema),
            Identifier(table_name),
            order_sql,
        )

        with self._connect() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                rows = list(cursor.fetchall())
                logger.info("All table rows loaded count=%s", len(rows))
                return rows

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
            lines.append(
                f"  - {column.column_name}: {column.data_type}, {nullable}{default}"
            )

        return "\n".join(lines).strip()
