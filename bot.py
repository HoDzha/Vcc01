import logging
from pathlib import Path
from threading import Thread

import telebot
from dotenv import load_dotenv
from telebot import types

from app.config import get_bot_config, get_database_config
from app.db import DatabaseClient
from app.excel import ExcelBackupClient
from app.sqlite_backup import SqliteBackupClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger_main = logging.getLogger("main")
logger_handlers = logging.getLogger("handlers")
logger_form = logging.getLogger("request_form")
logger_backup = logging.getLogger("excel_backup")
logger_sqlite = logging.getLogger("sqlite_backup")

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

bot_config = get_bot_config()
db_config = get_database_config()
db_client = DatabaseClient(db_config)

DB_SCHEMA = "public"
DB_TABLE = "it_team_requests"

excel_client = ExcelBackupClient.with_default_path()
sqlite_client = SqliteBackupClient.with_default_path(table_name=DB_TABLE)

bot = telebot.TeleBot(bot_config.token, parse_mode="HTML")
form_sessions: dict[int, dict[str, str]] = {}

PRIORITIES = ["Низкий", "Средний", "Высокий", "Критический"]
STATUSES = ["Новая", "В работе", "На паузе", "Выполнена"]

BTN_CREATE_REQUEST = "🛠️ Новая заявка"
BTN_MY_REQUESTS = "📋 Последние заявки"
BTN_MAIN_MENU = "🏠 Главное меню"
BTN_CANCEL_FORM = "❌ Отменить"


def build_main_menu_keyboard() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        types.KeyboardButton(BTN_CREATE_REQUEST),
        types.KeyboardButton(BTN_MY_REQUESTS),
        types.KeyboardButton(BTN_MAIN_MENU),
    )
    return keyboard


def build_cancel_keyboard() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add(types.KeyboardButton(BTN_CANCEL_FORM))
    return keyboard


def build_priority_keyboard() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for priority in PRIORITIES:
        keyboard.add(types.KeyboardButton(priority))
    keyboard.add(types.KeyboardButton(BTN_CANCEL_FORM))
    return keyboard


def build_status_keyboard() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for status in STATUSES:
        keyboard.add(types.KeyboardButton(status))
    keyboard.add(types.KeyboardButton(BTN_CANCEL_FORM))
    return keyboard


@bot.message_handler(commands=["start", "help"])
def handle_start(message: telebot.types.Message) -> None:
    text = (
        "Привет! Я бот заявок для IT-команды.\n\n"
        "Команды:\n"
        "/request - создать заявку\n"
        "/list - показать последние заявки\n"
        "/schema - показать структуру БД\n"
        "/backup_status - выгрузить таблицу в Excel\n"
        "/cancel - отменить заполнение"
    )
    bot.send_message(message.chat.id, text, reply_markup=build_main_menu_keyboard())


@bot.message_handler(commands=["request"])
def handle_request_start(message: telebot.types.Message) -> None:
    user_id = message.from_user.id
    form_sessions[user_id] = {"step": "task"}
    bot.send_message(
        message.chat.id,
        "Шаг 1/4. Опишите задачу для IT-команды:",
        reply_markup=build_cancel_keyboard(),
    )


@bot.message_handler(commands=["cancel"])
def handle_cancel(message: telebot.types.Message) -> None:
    user_id = message.from_user.id
    if user_id in form_sessions:
        form_sessions.pop(user_id, None)
        bot.send_message(message.chat.id, "Заполнение отменено.", reply_markup=build_main_menu_keyboard())
        return

    bot.send_message(message.chat.id, "Активной формы нет.", reply_markup=build_main_menu_keyboard())


@bot.message_handler(commands=["schema"])
def handle_schema(message: telebot.types.Message) -> None:
    try:
        schema_text = db_client.get_schema_overview_text(schema=DB_SCHEMA)
        if len(schema_text) > 4000:
            schema_text = schema_text[:3950] + "\n\n... (сообщение сокращено)"
        bot.send_message(message.chat.id, f"<b>Структура БД:</b>\n<pre>{schema_text}</pre>")
    except Exception as exc:
        logger_handlers.exception("Failed to load schema")
        bot.send_message(message.chat.id, f"Не удалось получить схему БД: <code>{exc}</code>")


@bot.message_handler(commands=["list"])
def handle_list(message: telebot.types.Message) -> None:
    try:
        rows = db_client.get_recent_requests(limit=10, schema=DB_SCHEMA, table_name=DB_TABLE)
        if not rows:
            bot.send_message(message.chat.id, "Заявок пока нет.", reply_markup=build_main_menu_keyboard())
            return

        lines = ["📋 Последние заявки:"]
        for row in rows:
            lines.append(
                f"#{row['id']} | {row['priority']} | {row['status']}\n"
                f"Автор: {row['author']}\n"
                f"Задача: {row['task']}"
            )
        bot.send_message(message.chat.id, "\n\n".join(lines), reply_markup=build_main_menu_keyboard())
    except Exception as exc:
        logger_handlers.exception("Failed to get requests list")
        bot.send_message(message.chat.id, f"Не удалось получить список заявок: <code>{exc}</code>")


@bot.message_handler(commands=["backup_status"])
def handle_backup_status(message: telebot.types.Message) -> None:
    chat_id = message.chat.id
    bot.send_message(chat_id, "Запускаю выгрузку заявок в Excel...")

    def run_full_backup() -> None:
        try:
            column_order = db_client.get_table_column_names(table_name=DB_TABLE, schema=DB_SCHEMA)
            rows = db_client.get_all_table_rows(table_name=DB_TABLE, schema=DB_SCHEMA)
            excel_client.replace_with_rows(rows=rows, column_order=column_order)
            bot.send_message(
                chat_id,
                "✅ Резервная копия обновлена.\n"
                f"Таблица: <code>{DB_SCHEMA}.{DB_TABLE}</code>\n"
                f"Строк: <b>{len(rows)}</b>\n"
                f"Файл: <code>{excel_client.file_path.name}</code>",
                reply_markup=build_main_menu_keyboard(),
            )
        except Exception as exc:
            logger_backup.exception("Failed to backup table")
            bot.send_message(
                chat_id,
                f"Не удалось сделать backup: <code>{exc}</code>",
                reply_markup=build_main_menu_keyboard(),
            )

    Thread(target=run_full_backup, daemon=True).start()


@bot.message_handler(func=lambda message: (message.text or "").strip() == BTN_CREATE_REQUEST)
def handle_request_button(message: telebot.types.Message) -> None:
    handle_request_start(message)


@bot.message_handler(func=lambda message: (message.text or "").strip() == BTN_MY_REQUESTS)
def handle_list_button(message: telebot.types.Message) -> None:
    handle_list(message)


@bot.message_handler(func=lambda message: (message.text or "").strip() == BTN_MAIN_MENU)
def handle_main_menu_button(message: telebot.types.Message) -> None:
    handle_start(message)


@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in form_sessions)
def handle_form_steps(message: telebot.types.Message) -> None:
    user_id = message.from_user.id
    state = form_sessions.get(user_id)
    if state is None:
        return

    text = (message.text or "").strip()
    if text == BTN_CANCEL_FORM:
        form_sessions.pop(user_id, None)
        bot.send_message(message.chat.id, "Заполнение отменено.", reply_markup=build_main_menu_keyboard())
        return

    step = state.get("step")
    logger_form.info("Received step=%s from user=%s", step, user_id)

    if step == "task":
        if len(text) < 5:
            bot.send_message(message.chat.id, "Опишите задачу подробнее (минимум 5 символов).")
            return

        state["task"] = text
        state["step"] = "priority"
        bot.send_message(message.chat.id, "Шаг 2/4. Выберите приоритет:", reply_markup=build_priority_keyboard())
        return

    if step == "priority":
        if text not in PRIORITIES:
            bot.send_message(
                message.chat.id,
                "Выберите приоритет кнопкой ниже.",
                reply_markup=build_priority_keyboard(),
            )
            return

        state["priority"] = text
        state["step"] = "author"
        bot.send_message(message.chat.id, "Шаг 3/4. Укажите автора заявки:", reply_markup=build_cancel_keyboard())
        return

    if step == "author":
        if len(text) < 2:
            bot.send_message(message.chat.id, "Введите корректное имя автора.")
            return

        state["author"] = text
        state["step"] = "status"
        bot.send_message(message.chat.id, "Шаг 4/4. Выберите статус:", reply_markup=build_status_keyboard())
        return

    if step == "status":
        if text not in STATUSES:
            bot.send_message(message.chat.id, "Выберите статус кнопкой ниже.", reply_markup=build_status_keyboard())
            return

        state["status"] = text
        tg_user = message.from_user

        try:
            saved_row = db_client.insert_request_record(
                task=state["task"],
                priority=state["priority"],
                author=state["author"],
                status=state["status"],
                telegram_user_id=tg_user.id,
                telegram_username=tg_user.username,
                schema=DB_SCHEMA,
                table_name=DB_TABLE,
            )

            def backup_to_sqlite() -> None:
                try:
                    columns = db_client.get_table_columns(table_name=DB_TABLE, schema=DB_SCHEMA)
                    sqlite_client.backup_row(row_data=saved_row, columns=columns)
                except Exception:
                    logger_sqlite.exception("Failed to backup request to SQLite")

            Thread(target=backup_to_sqlite, daemon=True).start()

            bot.send_message(
                message.chat.id,
                "✅ Заявка создана!\n\n"
                f"ID: <b>{saved_row['id']}</b>\n"
                f"Задача: {saved_row['task']}\n"
                f"Приоритет: {saved_row['priority']}\n"
                f"Автор: {saved_row['author']}\n"
                f"Статус: {saved_row['status']}",
                reply_markup=build_main_menu_keyboard(),
            )
        except Exception as exc:
            logger_form.exception("Failed to create request")
            bot.send_message(message.chat.id, f"Не удалось создать заявку: <code>{exc}</code>")
        finally:
            form_sessions.pop(user_id, None)


def main() -> None:
    logger_main.info("Starting bot for table %s.%s", DB_SCHEMA, DB_TABLE)
    db_client.ensure_requests_table(schema=DB_SCHEMA, table_name=DB_TABLE)
    bot.infinity_polling(skip_pending=True)


if __name__ == "__main__":
    main()
