# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

"""
app.py - Основной файл запуска Flask приложения

Этот файл является точкой входа в приложение и выполняет следующие функции:
1. Настраивает систему логирования
2. Инициализирует конфигурацию приложения
3. Устанавливает соединение с базой данных
4. Запускает миграции базы данных в фоновом режиме
5. Регистрирует эндпоинты для проверки здоровья (healthz/readyz)
6. Запускает Flask сервер

Приложение разработано для работы в Kubernetes-окружении, о чем свидетельствуют
эндпоинты healthz и readyz, используемые для проверки жизнеспособности и готовности pod'ов.

Архитектура приложения построена на модульном принципе с разделением ответственности:
- maintenance: утилиты для обслуживания (логирование, конфигурация, БД, миграции)
- k8s: Kubernetes-специфичные функции (health checks)
"""

from flask import Flask
import threading

# Настройка системы логирования
from maintenance.logging_config import setup_logging
logger = setup_logging()
logger.info("Приложение запускается")

# Импорт модулей приложения
from k8s.healthz import healthz_bp  # Blueprint для проверки здоровья приложения
from k8s.readyz import readyz_bp    # Blueprint для проверки готовности приложения
from maintenance.config_read import get_config_reader  # Чтение конфигурации
from maintenance.database_connector import initialize_database, wait_for_database_connection  # Работа с БД
from maintenance.migration import run_migrations  # Миграции базы данных

# Создание экземпляра Flask приложения
app = Flask(__name__)

# Инициализация конфигурации приложения
try:
    config_reader = get_config_reader()
    logger.info("ConfigReader успешно инициализирован")
except Exception as e:
    logger.error(f"Ошибка инициализации ConfigReader: {e}")

# Инициализация базы данных
try:
    logger.info("Инициализация базы данных...")
    initialize_database()
    # Альтернативный вариант с ожиданием подключения (закомментирован):
    # wait_for_database_connection(max_retries=10, retry_delay=5.0)
    logger.info("База данных успешно инициализирована")
except Exception as e:
    logger.error(f"Ошибка инициализации базы данных: {e}")

# Функция для запуска миграций в фоновом режиме
def run_migrations_background():
    """
    Запускает миграции базы данных в отдельном потоке.
    
    Эта функция выполняется асинхронно, чтобы не блокировать основной поток приложения
    во время запуска.
    """
    try:
        logger.info("Запуск миграций базы данных...")
        applied_migrations = run_migrations()
        if applied_migrations:
            logger.info(f"Миграции успешно применены: {applied_migrations}")
        else:
            logger.info("Нет новых миграций для применения")
    except Exception as e:
        logger.error(f"Ошибка выполнения миграций: {e}")

# Запуск миграций в отдельном потоке с демонизацией
# daemon=True означает, что поток будет автоматически завершен при завершении основного потока
migration_thread = threading.Thread(target=run_migrations_background, daemon=True)
migration_thread.start()

# Регистрация Blueprint'ов для эндпоинтов проверки здоровья
app.register_blueprint(healthz_bp)  # Эндпоинт /healthz для проверки жизнеспособности
app.register_blueprint(readyz_bp)   # Эндпоинт /readyz для проверки готовности к работе

# Точка входа при запуске файла напрямую
if __name__ == '__main__':
    logger.info("Приложение запущено")
    # Запуск Flask сервера на всех интерфейсах (0.0.0.0) на порту 5000
    app.run(host="0.0.0.0", port=5000)