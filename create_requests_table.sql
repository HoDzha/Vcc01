CREATE TABLE IF NOT EXISTS public.it_team_requests (
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

COMMENT ON TABLE public.it_team_requests IS 'Заявки для IT-команды';
COMMENT ON COLUMN public.it_team_requests.task IS 'Задача';
COMMENT ON COLUMN public.it_team_requests.priority IS 'Приоритет';
COMMENT ON COLUMN public.it_team_requests.author IS 'Автор';
COMMENT ON COLUMN public.it_team_requests.status IS 'Статус';
