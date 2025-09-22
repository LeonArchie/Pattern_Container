# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

from flask import Blueprint, jsonify
import logging
import os

# Импорт модулей для проверки БД
from maintenance.database_connector import is_database_healthy, is_database_initialized
# Импорт модуля миграций
from maintenance.migration import is_migration_complete, check_migrations_status
# Импорт функции проверки готовности сервиса конфигураций
from maintenance.config_read import is_config_service_ready

logger = logging.getLogger(__name__)
readyz_bp = Blueprint('readyz', __name__)

def _check_config_service_readiness():
    """
    Проверка готовности сервиса конфигураций через config_read
    """
    try:
        is_ready = is_config_service_ready()
        if is_ready:
            logger.debug("Сервис конфигураций готов")
            return True
        else:
            logger.warning("Сервис конфигураций не готов")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке готовности сервиса конфигураций: {e}")
        return False

def _check_database_readiness():
    """
    Проверка готовности базы данных
    """
    try:
        # Проверяем, инициализирована ли база данных
        if not is_database_initialized():
            logger.warning("База данных не инициализирована")
            return False
        
        # Проверяем здоровье базы данных
        if is_database_healthy():
            logger.debug("База данных готова")
            return True
        else:
            logger.warning("База данных не готова")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке готовности базы данных: {e}")
        return False

def _check_migrations_status():
    """
    Проверка статуса миграций базы данных
    """
    try:
        # Используем только одну функцию для проверки
        migrations_complete, status_message, pending_migrations = check_migrations_status()
        
        if migrations_complete:
            logger.debug(f"Миграции базы данных завершены: {status_message}")
            return True
        else:
            logger.warning(f"Миграции базы данных не завершены: {status_message}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса миграций: {e}")
        return False

@readyz_bp.route('/readyz', methods=['GET'])
def readyz():
    logger.debug("Проверка готовности сервиса")
    
    # Проверяем готовность сервиса конфигураций
    config_ready = _check_config_service_readiness()
    
    # Проверяем готовность базы данных
    db_ready = _check_database_readiness()
    
    # Проверяем статус миграций
    migrations_ready = _check_migrations_status()
    
    # Определяем общий статус
    all_ready = config_ready and db_ready and migrations_ready
    
    # Формируем ответ
    if all_ready:
        response_data = {"status": True}
        return jsonify(response_data), 200
    else:
        response_data = {"status": False}
        return jsonify(response_data), 503  # Service Unavailable
    
# Основные принципы работы этого endpoint:
#
# 1. Отличие от /healthz:
#    - /healthz проверяет "живость" сервиса (liveness)
#    - /readyz проверяет готовность обрабатывать запросы (readiness)
#
# 2. Типичные сценарии использования:
#    - Kubernetes использует для управления подами трафика
#    - Балансировщики нагрузки для исключения/включения нод
#    - В оркестраторах при rolling updates
#
# 3. В продакшн-среде следует добавить проверки:
#    - Доступность подключений к БД
#    - Загрузка CPU/памяти
#    - Наличие свободного места на диске
#    - Состояние кэшей
#
# 4. Оптимизации:
#    - Минимизировать внешние зависимости проверок
#    - Кэшировать результаты, если проверки ресурсоемкие
#    - Добавлять timeout для внешних проверок
#
# 5. Безопасность:
#    - Не должен раскрывать sensitive-информацию
#    - Можно добавить базовую аутентификацию
#    - Рекомендуется закрыть от публичного доступа