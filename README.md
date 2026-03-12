# IT Team Requests Telegram Bot

Телеграм-бот для сбора заявок в IT-команду с сохранением в PostgreSQL.

## Что умеет

- Создание заявки через пошаговую форму (4 шага).
- Сохранение заявок в таблицу `public.it_team_requests`.
- Просмотр последних 10 заявок.
- Просмотр схемы БД.
- Выгрузка всей таблицы в Excel (`users_backup.xlsx`).
- Автоматический backup каждой новой заявки в SQLite (`backup.sqlite3`).

## Структура таблицы

Основные рабочие поля:

| Задача | Приоритет | Автор | Статус |
|--------|-----------|-------|--------|

SQL для создания таблицы: `create_requests_table.sql`.

## Установка

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Конфигурация

Создайте `.env` в корне проекта:

```env
TELEGRAM_BOT_TOKEN=...

DB_HOST=...
DB_PORT=5432
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
DB_SSLMODE=prefer
```

## Запуск

```bash
python bot.py
```

При старте бот сам проверяет и создает таблицу `public.it_team_requests`, если ее еще нет.

## Команды

- `/start`, `/help` — главное меню.
- `/request` — создать заявку.
- `/list` — показать последние заявки.
- `/schema` — структура БД.
- `/backup_status` — выгрузка таблицы в Excel.
- `/cancel` — отменить заполнение формы.
