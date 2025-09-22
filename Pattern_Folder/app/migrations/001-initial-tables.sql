-- SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
-- Copyright (C) 2025 Петунин Лев Михайлович

-- НЕ ЗАБЫВАТЬ ВЫДАТЬ ПРАВА ВСЕМ ПОЛЬЗОВАТЕЛЯМ КОТОРЫМ ОНИ НУЖНЫ 
--И DB_ADMIN


-- Создание таблицы пользователей
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    userlogin VARCHAR(20) NOT NULL UNIQUE,
    password_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER 
ON TABLE users TO "DB_ADMIN";
GRANT ALL PRIVILEGES ON users TO "DB_ADMIN";


-- Комментарии к таблице users
COMMENT ON TABLE users IS 'Основная таблица для хранения информации о пользователях системы';
COMMENT ON COLUMN users.user_id IS 'Уникальный идентификатор пользователя в формате UUID';
COMMENT ON COLUMN users.userlogin IS 'Уникальный логин пользователя (максимальная длина 20 символов)';
COMMENT ON COLUMN users.password_hash IS 'Хэш пароля пользователя, созданный алгоритмом SHA-256 (фиксированная длина 64 символа)';
COMMENT ON COLUMN users.created_at IS 'Дата и время создания учетной записи пользователя (часовой пояс UTC)';
COMMENT ON COLUMN users.updated_at IS 'Дата и время последнего обновления информации о пользователе (часовой пояс UTC)';


-- Создание таблицы сессий
CREATE TABLE sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    access_token TEXT NOT NULL,
    refresh_token_hash VARCHAR(64) NOT NULL,
    user_agent VARCHAR(200),
    ip_address VARCHAR(45),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_revoked BOOLEAN NOT NULL DEFAULT FALSE,
    last_used_at TIMESTAMP WITH TIME ZONE
);

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER 
ON TABLE sessions TO "DB_ADMIN";
GRANT ALL PRIVILEGES ON sessions TO "DB_ADMIN";


-- Комментарии к таблице sessions
COMMENT ON TABLE sessions IS 'Таблица для хранения активных сессий пользователей';
COMMENT ON COLUMN sessions.session_id IS 'Уникальный идентификатор сессии в формате UUID (генерируется автоматически)';
COMMENT ON COLUMN sessions.user_id IS 'Ссылка на пользователя в таблице users (каскадное удаление при удалении пользователя)';
COMMENT ON COLUMN sessions.access_token IS 'JWT токен доступа (максимальная длина 500 символов)';
COMMENT ON COLUMN sessions.refresh_token_hash IS 'Хэш токена обновления (SHA-256, 64 символа)';
COMMENT ON COLUMN sessions.user_agent IS 'Информация о браузере/устройстве пользователя (максимальная длина 200 символов)';
COMMENT ON COLUMN sessions.ip_address IS 'IP-адрес пользователя (поддерживает IPv6, максимальная длина 45 символов)';
COMMENT ON COLUMN sessions.created_at IS 'Дата и время создания сессии (часовой пояс UTC)';
COMMENT ON COLUMN sessions.expires_at IS 'Дата и время истечения срока действия сессии (часовой пояс UTC)';
COMMENT ON COLUMN sessions.is_revoked IS 'Флаг отзыва сессии (true - отозвана, false - активна)';
COMMENT ON COLUMN sessions.last_used_at IS 'Дата и время последнего использования сессии (часовой пояс UTC)';

-- Создание индексов
CREATE INDEX idx_users_login ON users(userlogin);
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_access_token ON sessions(access_token);
CREATE INDEX idx_sessions_expires ON sessions(expires_at) WHERE NOT is_revoked;