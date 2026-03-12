# VCc01 Telegram Bot

Телеграм-бот для сбора анкет пользователей с сохранением данных в удаленную PostgreSQL базу.

## Возможности

- Опрос из 2 шагов: ФИО и дата рождения.
- Сохранение анкет в PostgreSQL (`public.users`).
- Автоматический backup каждой новой записи в локальный SQLite (`backup.sqlite3`).
- Выгрузка всей SQL-таблицы в Excel только по явной команде `/backup_status` (`users_backup.xlsx`).
- Просмотр схемы БД через `/schema`.
- Просмотр последних анкет через кнопку `📚 Мои анкеты`.

## Структура проекта

- `bot.py` — основной запуск и обработчики Telegram.
- `app/config.py` — чтение конфигурации из переменных окружения.
- `app/db.py` — работа с PostgreSQL.
- `app/sqlite_backup.py` — автоматический backup в SQLite.
- `app/excel.py` — выгрузка данных в Excel.
- `check_db.py` — утилита проверки подключения и схемы БД.
- `add_users_fields.sql` — SQL-скрипт для добавления колонок в `public.users`.

## Требования

- Python 3.11+
- PostgreSQL (доступный по сети)
- Telegram Bot Token

## Установка

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Конфигурация

1. Скопируйте шаблон:

```bash
copy .env.example .env
```

2. Заполните `.env`:

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token

DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_SSLMODE=prefer
```

## Подготовка таблицы users

Выполните SQL из файла `add_users_fields.sql` в вашей БД, чтобы добавить нужные поля в `public.users`.

## Запуск

```bash
python bot.py
```

## Команды бота

- `/start`, `/help` — главное меню и список команд.
- `/form` — начать опрос.
- `/cancel` — отменить текущий опрос.
- `/schema` — показать структуру таблиц БД.
- `/backup_status` — выгрузить всю таблицу `public.users` в `users_backup.xlsx`.

## Backup-механика

- После каждой успешной записи в PostgreSQL бот автоматически делает копию этой записи в `backup.sqlite3`.
- Выгрузка в Excel выполняется только по явной команде `/backup_status`.

## Диагностика

Проверка подключения к БД и чтения схемы:

```bash
python check_db.py
```
