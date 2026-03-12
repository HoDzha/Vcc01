-- Добавление полей для работы бота в существующую таблицу users

ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS user_id       BIGINT,
    ADD COLUMN IF NOT EXISTS username      TEXT,
    ADD COLUMN IF NOT EXISTS full_name     TEXT,
    ADD COLUMN IF NOT EXISTS birth_date    DATE,
    ADD COLUMN IF NOT EXISTS city          TEXT,
    ADD COLUMN IF NOT EXISTS profession    TEXT,
    ADD COLUMN IF NOT EXISTS hobby         TEXT,
    ADD COLUMN IF NOT EXISTS random_number INTEGER,
    ADD COLUMN IF NOT EXISTS random_score  NUMERIC(3, 1),
    ADD COLUMN IF NOT EXISTS is_active     BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS random_color  TEXT;

-- Комментарии к полям таблицы users
COMMENT ON COLUMN public.users.user_id       IS 'ID пользователя в Telegram';
COMMENT ON COLUMN public.users.username      IS 'Username пользователя в Telegram';
COMMENT ON COLUMN public.users.full_name     IS 'ФИО пользователя (ответ на вопрос 1)';
COMMENT ON COLUMN public.users.birth_date    IS 'Дата рождения в формате ДД.ММ.ГГГГ (ответ на вопрос 2)';
COMMENT ON COLUMN public.users.city          IS 'Случайно выбранный город';
COMMENT ON COLUMN public.users.profession    IS 'Случайно выбранная профессия';
COMMENT ON COLUMN public.users.hobby         IS 'Случайно выбранное хобби';
COMMENT ON COLUMN public.users.random_number IS 'Случайное число от 1 до 1000';
COMMENT ON COLUMN public.users.random_score  IS 'Случайная оценка от 1.0 до 10.0';
COMMENT ON COLUMN public.users.is_active     IS 'Случайный булевый флаг';
COMMENT ON COLUMN public.users.random_color  IS 'Случайно выбранный цвет';

