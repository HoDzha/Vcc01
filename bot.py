import logging
from threading import Thread
from datetime import datetime
import html

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
logger_survey = logging.getLogger("survey")
logger_backup = logging.getLogger("excel_backup")
logger_sqlite = logging.getLogger("sqlite_backup")

load_dotenv()

bot_config = get_bot_config()
db_config = get_database_config()
db_client = DatabaseClient(db_config)

DB_SCHEMA = "public"
DB_TABLE = "users"

excel_client = ExcelBackupClient.with_default_path()
sqlite_client = SqliteBackupClient.with_default_path(table_name=DB_TABLE)

logger_main.info("Telegram bot initialization started")
logger_main.info("Configured database target: %s.%s", DB_SCHEMA, DB_TABLE)
logger_main.info("Configured Excel backup file: %s", excel_client.file_path.name)
logger_main.info("Configured SQLite backup file: %s", sqlite_client.file_path.name)

bot = telebot.TeleBot(bot_config.token, parse_mode="HTML")
form_sessions: dict[int, dict[str, str]] = {}

BTN_START_FORM = "📝 Пройти еще ..."
BTN_MY_FORMS = "📚 Мои анкеты"
BTN_MAIN_MENU = "🏠 Главное меню"
BTN_CANCEL_FORM = "❌ Отменить опрос"


def build_main_menu_keyboard() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    keyboard.add(
        types.KeyboardButton(BTN_START_FORM),
        types.KeyboardButton(BTN_MY_FORMS),
        types.KeyboardButton(BTN_MAIN_MENU),
    )
    return keyboard


def build_cancel_keyboard() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add(types.KeyboardButton(BTN_CANCEL_FORM))
    return keyboard


@bot.message_handler(commands=["start", "help"])
def handle_start(message: telebot.types.Message) -> None:
    logger_handlers.info("User %s requested /start or /help", message.from_user.id if message.from_user else "unknown")
    text = (
        "Привет! Я помогу заполнить короткую анкету.\n\n"
        "Команды:\n"
        "/form - начать опрос\n"
        "/schema - структура таблиц БД\n"
        "/backup_status - выгрузить всю таблицу в Excel\n"
        "/cancel - отменить текущий опрос"
    )
    bot.send_message(message.chat.id, text, reply_markup=build_main_menu_keyboard())


@bot.message_handler(commands=["schema"])
def handle_schema(message: telebot.types.Message) -> None:
    logger_handlers.info("User %s requested /schema", message.from_user.id if message.from_user else "unknown")
    try:
        schema_text = db_client.get_schema_overview_text(schema="public")
        if len(schema_text) > 4000:
            schema_text = schema_text[:3950] + "\n\n... (сообщение сокращено)"
        bot.send_message(message.chat.id, f"<b>Структура БД:</b>\n<pre>{schema_text}</pre>")
    except Exception as exc:
        logging.exception("Failed to read database schema.")
        bot.send_message(
            message.chat.id,
            f"Не удалось получить структуру БД: <code>{exc}</code>",
        )


@bot.message_handler(commands=["form"])
def handle_form_start(message: telebot.types.Message) -> None:
    user_id = message.from_user.id
    logger_handlers.info("User %s started form", user_id)
    form_sessions[user_id] = {"step": "full_name"}
    bot.send_message(
        message.chat.id,
        "Вопрос 1 из 2:\n"
        "Как тебя зовут? (Введите ваше ФИО)\n\n"
        "Пример: Иванов Иван Иванович",
        reply_markup=build_cancel_keyboard(),
    )


@bot.message_handler(commands=["backup_status"])
def handle_backup_status(message: telebot.types.Message) -> None:
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else "unknown"
    logger_backup.info("User %s requested /backup_status", user_id)
    bot.send_message(chat_id, "Запускаю полную выгрузку из БД в Excel...")

    def run_full_backup() -> None:
        try:
            logger_backup.info("Full backup started for %s.%s", DB_SCHEMA, DB_TABLE)
            column_order = db_client.get_table_column_names(table_name=DB_TABLE, schema=DB_SCHEMA)
            rows = db_client.get_all_table_rows(table_name=DB_TABLE, schema=DB_SCHEMA)
            excel_client.replace_with_rows(rows=rows, column_order=column_order)
            logger_backup.info("Full backup completed rows=%s", len(rows))
            bot.send_message(
                chat_id,
                "✅ Резервная копия обновлена.\n"
                f"Таблица: <code>{DB_SCHEMA}.{DB_TABLE}</code>\n"
                f"Строк экспортировано: <b>{len(rows)}</b>\n"
                f"Файл: <code>{excel_client.file_path.name}</code>",
                reply_markup=build_main_menu_keyboard(),
            )
        except Exception as exc:
            logger_backup.exception("Failed to run full Excel backup.")
            bot.send_message(
                chat_id,
                f"Не удалось обновить резервную копию: <code>{exc}</code>",
                reply_markup=build_main_menu_keyboard(),
            )

    Thread(target=run_full_backup, daemon=True).start()


@bot.message_handler(commands=["cancel"])
def handle_form_cancel(message: telebot.types.Message) -> None:
    user_id = message.from_user.id
    logger_handlers.info("User %s requested /cancel", user_id)
    if user_id in form_sessions:
        form_sessions.pop(user_id, None)
        bot.send_message(
            message.chat.id,
            "Опрос отменен.",
            reply_markup=build_main_menu_keyboard(),
        )
        return

    bot.send_message(
        message.chat.id,
        "Активного опроса нет.",
        reply_markup=build_main_menu_keyboard(),
    )


@bot.message_handler(func=lambda message: (message.text or "").strip() == BTN_START_FORM)
def handle_form_button(message: telebot.types.Message) -> None:
    logger_handlers.info("User %s clicked start form button", message.from_user.id if message.from_user else "unknown")
    handle_form_start(message)


@bot.message_handler(func=lambda message: (message.text or "").strip() == BTN_MAIN_MENU)
def handle_main_menu_button(message: telebot.types.Message) -> None:
    logger_handlers.info("User %s opened main menu", message.from_user.id if message.from_user else "unknown")
    handle_start(message)


@bot.message_handler(func=lambda message: (message.text or "").strip() == BTN_MY_FORMS)
def handle_my_forms(message: telebot.types.Message) -> None:
    logger_handlers.info("User %s requested recent forms", message.from_user.id if message.from_user else "unknown")
    try:
        rows = db_client.get_recent_survey_rows(limit=5, schema=DB_SCHEMA, table_name=DB_TABLE)
        if not rows:
            bot.send_message(
                message.chat.id,
                "Пока нет сохраненных анкет.",
                reply_markup=build_main_menu_keyboard(),
            )
            return

        lines = ["📚 <b>Последние анкеты:</b>"]
        for row in rows:
            full_name = html.escape(str(row.get("full_name") or "—"))
            birth_date = row.get("birtdate") or row.get("birthdate") or row.get("birth_date") or "—"
            city = html.escape(str(row.get("city") or "—"))
            profession = html.escape(str(row.get("profession") or "—"))
            hobby = html.escape(str(row.get("hobby") or "—"))
            random_number = row.get("random_number", "—")
            random_score = row.get("random_score", "—")
            is_active = row.get("is_active", "—")
            random_color = html.escape(str(row.get("random_color") or "—"))

            lines.append(
                "• <b>{}</b>\n"
                "  🎂 {}\n"
                "  🏙 {}\n"
                "  💼 {}\n"
                "  🎯 {}\n"
                "  🔢 {} | ⭐ {} | ✅ {}\n"
                "  🎨 {}".format(
                    full_name,
                    birth_date,
                    city,
                    profession,
                    hobby,
                    random_number,
                    random_score,
                    is_active,
                    random_color,
                )
            )

        bot.send_message(
            message.chat.id,
            "\n\n".join(lines),
            reply_markup=build_main_menu_keyboard(),
        )
    except Exception as exc:
        logger_handlers.exception("Failed to read recent surveys.")
        bot.send_message(
            message.chat.id,
            f"Не удалось получить анкеты: <code>{exc}</code>",
            reply_markup=build_main_menu_keyboard(),
        )


@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in form_sessions)
def handle_form_steps(message: telebot.types.Message) -> None:
    user_id = message.from_user.id
    state = form_sessions.get(user_id)
    if state is None:
        return

    step = state.get("step")
    text = (message.text or "").strip()
    logger_survey.info("Received form input from user=%s step=%s", user_id, step)

    if text == BTN_CANCEL_FORM:
        form_sessions.pop(user_id, None)
        bot.send_message(
            message.chat.id,
            "Опрос отменен.",
            reply_markup=build_main_menu_keyboard(),
        )
        return

    if step == "full_name":
        if len(text) < 3:
            logger_survey.warning("Invalid full name from user=%s", user_id)
            bot.send_message(message.chat.id, "Введите корректное ФИО.")
            return

        state["full_name"] = text
        state["step"] = "birth_date"
        logger_survey.info("Accepted full name for user=%s", user_id)
        bot.send_message(
            message.chat.id,
            "✅ Принято: <b>{}</b>\n\n"
            "Вопрос 2 из 2:\n"
            "Какая ваша дата рождения?\n\n"
            "Введите в формате: ДД.ММ.ГГГГ\n"
            "Пример: 01.01.2001".format(text),
            reply_markup=build_cancel_keyboard(),
        )
        return

    if step == "birth_date":
        try:
            birth_date = datetime.strptime(text, "%d.%m.%Y").date()
        except ValueError:
            logger_survey.warning("Invalid birth date format from user=%s value=%s", user_id, text)
            bot.send_message(
                message.chat.id,
                "Неверный формат даты.\nВведите: ДД.ММ.ГГГГ (например 01.01.2001)",
                reply_markup=build_cancel_keyboard(),
            )
            return

        full_name = state.get("full_name", "").strip()
        if not full_name:
            form_sessions.pop(user_id, None)
            bot.send_message(message.chat.id, "Сессия опроса сброшена. Запусти /form заново.")
            return

        try:
            tg_user = message.from_user
            tg_user_id = tg_user.id
            tg_username = tg_user.username or ""

            saved_row = db_client.insert_survey_record(
                user_id=tg_user_id,
                username=tg_username,
                full_name=full_name,
                birth_date=birth_date,
                schema=DB_SCHEMA,
                table_name=DB_TABLE,
            )
            logger_survey.info("Survey data saved to DB for user=%s", tg_user_id)

            def backup_to_sqlite() -> None:
                try:
                    logger_sqlite.info("Background SQLite backup started user=%s", tg_user_id)
                    columns = db_client.get_table_columns(table_name=DB_TABLE, schema=DB_SCHEMA)
                    sqlite_client.backup_row(row_data=saved_row, columns=columns)
                    logger_sqlite.info("Background SQLite backup completed user=%s", tg_user_id)
                except Exception:
                    logger_sqlite.exception("Failed to backup survey row to SQLite.")

            Thread(target=backup_to_sqlite, daemon=True).start()

            bot.send_message(
                message.chat.id,
                "🎉 <b>Опрос завершен!</b>\n\n"
                "✅ <b>Ваши данные:</b>\n"
                f"👤 ФИО: {full_name}\n"
                f"🎂 Дата рождения: {text}\n\n"
                "Данные успешно сохранены в базу данных!",
                reply_markup=build_main_menu_keyboard(),
            )
        except Exception as exc:
            logger_survey.exception("Failed to save form data for user=%s", user_id)
            bot.send_message(
                message.chat.id,
                f"Не удалось сохранить данные: <code>{exc}</code>",
                reply_markup=build_main_menu_keyboard(),
            )
        finally:
            form_sessions.pop(user_id, None)


def main() -> None:
    logger_main.info("Starting bot polling")
    try:
        me = bot.get_me()
        logger_main.info("Bot initialized as @%s (%s)", me.username, me.first_name)
    except Exception:
        logger_main.exception("Unable to get bot profile during startup.")
    bot.infinity_polling(skip_pending=True)


if __name__ == "__main__":
    main()
