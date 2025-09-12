# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

"""
readyz.py - Blueprint для эндпоинта проверки готовности сервиса (/readyz)

Этот модуль реализует endpoint /readyz, который проверяет готовность сервиса
к обработке запросов. В отличие от /healthz (проверка "живости"), /readyz
проверяет, что все зависимости сервиса готовы к работе.

Проверяемые компоненты:
1. Сервис конфигураций - доступность внешнего сервиса конфигураций
2. База данных - подключение и инициализация БД
3. Миграции - статус выполнения миграций базы данных

Использование в Kubernetes:
- Readyz используется для управления подами трафика
- При статусе 503 Service Unavailable pod исключается из балансировки нагрузки
- Позволяет обеспечить zero-downtime деплойменты
"""

from flask import Blueprint, jsonify
import logging
import requests
import os

# Импорт модулей для проверки БД
from maintenance.database_connector import is_database_healthy, is_database_initialized
# Импорт модуля миграций
from maintenance.migration import is_migration_complete, check_migrations_status

logger = logging.getLogger(__name__)
readyz_bp = Blueprint('readyz', __name__)

def _get_config_service_url():
    """
    Получение URL сервиса конфигураций из global.conf
    
    Возвращает:
        str: URL сервиса конфигураций или None если не найден
        
    Функция читает конфигурационный файл global.conf и извлекает значение
    параметра URL_CONFIG_MODULES, который должен содержать базовый URL
    сервиса конфигураций.
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.join(current_dir, '..', 'global.conf')
        config_file_path = os.path.normpath(config_file_path)
        with open(config_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('URL_CONFIG_MODULES='):
                    url = line.split('=', 1)[1].strip()
                    if url:
                        return url
        return None
    except Exception as e:
        logger.error(f"Ошибка чтения URL_CONFIG_MODULES: {e}")
        return None

def _check_config_service_readiness():
    """
    Проверка готовности сервиса конфигураций
    
    Возвращает:
        Tuple[bool, str]: (статус_готовности, сообщение_ошибки)
        
    Функция выполняет HTTP-запрос к эндпоинту /readyz сервиса конфигураций
    и проверяет, что сервис возвращает статус "ready".
    
    Особенности:
    - Таймаут запроса: 3 секунды
    - Обрабатывает различные типы сетевых ошибок
    - Логирует детальную информацию об ошибках
    """
    config_url = _get_config_service_url()
    if not config_url:
        logger.warning("URL_CONFIG_MODULES не найден в global.conf")
        return False, "Config service URL not configured"
    
    try:
        readyz_url = f"{config_url}/readyz"  # Формируем полный URL для проверки готовности
        logger.debug(f"Проверка readyz сервиса конфигураций: {readyz_url}")
        
        # Выполняем HTTP-запрос с таймаутом для избежания блокировки
        response = requests.get(readyz_url, timeout=3)
        response.raise_for_status()  # Проверяем HTTP-статус ответа
        
        data = response.json()
        if data.get('status') == 'ready':
            logger.debug("Сервис конфигураций готов")
            return True, "Config service is ready"
        else:
            logger.warning(f"Сервис конфигураций вернул неожиданный статус готовности: {data}")
            return False, f"Config service returned unexpected readiness status: {data.get('status')}"
            
    except requests.exceptions.Timeout:
        logger.error("Таймаут при проверке готовности сервиса конфигураций")
        return False, "Config service readiness timeout"
    except requests.exceptions.ConnectionError:
        logger.error("Ошибка подключения к сервису конфигураций при проверке готовности")
        return False, "Config service readiness connection error"
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса готовности к сервису конфигураций: {e}")
        return False, f"Config service readiness request error: {str(e)}"
    except Exception as e:
        logger.error(f"Неожиданная ошибка при проверке готовности сервиса конфигураций: {e}")
        return False, f"Unexpected error checking config service readiness: {str(e)}"

def _check_database_readiness():
    """
    Проверка готовности базы данных
    
    Возвращает:
        Tuple[bool, str]: (статус_готовности, сообщение_ошибки)
        
    Функция проверяет два аспекта готовности БД:
    1. Инициализация - созданы ли необходимые таблицы и структуры
    2. Здоровье - доступность и работоспособность подключения к БД
    
    Использует функции из database_connector для проверки состояния БД.
    """
    try:
        # Проверяем, инициализирована ли база данных (созданы системные таблицы)
        if not is_database_initialized():
            logger.warning("База данных не инициализирована")
            return False, "Database not initialized"
        
        # Проверяем здоровье базы данных (доступность подключения)
        if is_database_healthy():
            logger.debug("База данных готова")
            return True, "Database is ready"
        else:
            logger.warning("База данных не готова")
            return False, "Database is not healthy"
            
    except Exception as e:
        logger.error(f"Ошибка при проверке готовности базы данных: {e}")
        return False, f"Database readiness check error: {str(e)}"

def _check_migrations_status():
    """
    Проверка статуса миграций базы данных
    
    Возвращает:
        Tuple[bool, str]: (статус_готовности, сообщение_ошибки)
        
    Функция проверяет, завершены ли все миграции базы данных.
    Если миграции не завершены, возвращает информацию о количестве
    ожидающих миграций.
    
    Использует функции из модуля migration для проверки статуса.
    """
    try:
        if is_migration_complete():
            logger.debug("Миграции базы данных завершены")
            return True, "Database migrations are complete"
        else:
            # Получаем детальную информацию о статусе миграций
            migrations_complete, pending_migrations, failed_migrations = check_migrations_status()
            if migrations_complete:
                logger.debug("Миграции базы данных завершены")
                return True, "Database migrations are complete"
            else:
                logger.warning(f"Миграции базы данных не завершены. Ожидающие: {pending_migrations}")
                return False, f"Database migrations pending: {len(pending_migrations)} migrations waiting"
                
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса миграций: {e}")
        return False, f"Migrations status check error: {str(e)}"

@readyz_bp.route('/readyz', methods=['GET'])
def readyz():
    """
    Endpoint для проверки готовности сервиса
    
    Возвращает:
        JSON-ответ со статусом 200 (готов) или 503 (не готов)
        
    Этот эндпоинт выполняет комплексную проверку всех зависимостей сервиса:
    1. Сервис конфигураций - доступность внешнего сервиса
    2. База данных - подключение и инициализация
    3. Миграции - статус выполнения миграций БД
    
    Kubernetes использует этот эндпоинт для определения, готов ли pod
    к приему трафика. При статусе 503 pod исключается из балансировки нагрузки.
    """
    logger.debug("Проверка готовности сервиса")
    
    # Проверяем готовность сервиса конфигураций
    config_ready, config_message = _check_config_service_readiness()
    
    # Проверяем готовность базы данных
    db_ready, db_message = _check_database_readiness()
    
    # Проверяем статус миграций
    migrations_ready, migrations_message = _check_migrations_status()
    
    # Формируем детальный ответ с информацией о каждом компоненте
    checks = {
        "config_service": {
            "status": "ready" if config_ready else "not_ready",
            "message": config_message
        },
        "database": {
            "status": "ready" if db_ready else "not_ready",
            "message": db_message
        },
        "migrations": {
            "status": "ready" if migrations_ready else "not_ready",
            "message": migrations_message
        }
    }
    
    # Определяем общий статус готовности (все компоненты должны быть готовы)
    all_ready = config_ready and db_ready and migrations_ready
    
    if all_ready:
        response_data = {
            "status": "ready",
        }
        logger.info("Сервис готов к обработке запросов")
        return jsonify(response_data), 200  # HTTP 200 OK
    else:
        response_data = {
            "status": "not_ready",
        }
        logger.warning(f"Сервис не готов: config_service={config_ready}, database={db_ready}, migrations={migrations_ready}")
        return jsonify(response_data), 503  # HTTP 503 Service Unavailable
    
# Основные принципы работы этого endpoint:
#
# 1. Назначение эндпоинта /readyz:
#    - Проверка готовности сервиса обрабатывать запросы (readiness probe)
#    - Определение, можно ли направлять трафик на данный экземпляр
#    - Проверка всех критических зависимостей сервиса
#
# 2. Отличие от /healthz:
#    - /healthz: проверяет "живость" процесса (нужен ли перезапуск)
#    - /readyz: проверяет готовность принимать трафик (можно ли балансировать)
#    - Kubernetes перезапускает контейнер если /healthz возвращает ошибку
#    - Kubernetes временно исключает из балансировки если /readyz возвращает ошибку
#
# 3. Критерии проверки для /readyz:
#    - Все критические зависимости доступны (БД, внешние сервисы)
#    - Сервис проинициализирован и настроен
#    - Ресурсы в пределах нормы (память, CPU, дисковое пространство)
#    - Нет временных проблем, мешающих обработке запросов
#
# 4. Типичные сценарии использования:
#    - Kubernetes: управление трафиком между pod'ами
#    - Балансировщики нагрузки: исключение/включение нод в ротацию
#    - Rolling updates: постепенное обновление версий без downtime
#    - Canary deployments: постепенное направление трафика на новую версию
#
# 5. Проверяемые зависимости:
#    - Сервис конфигураций (внешняя зависимость)
#    - База данных (критическая внутренняя зависимость)
#    - Другие внешние сервисы при необходимости
#    - Кэши, очереди сообщений, файловые хранилища
#
# 6. Рекомендации по реализации:
#    - Проверки должны быть быстрыми (таймаут обычно 1-5 секунд)
#    - Включать timeout для внешних проверок
#    - Возвращать детальную информацию для диагностики
#    - Логировать результаты проверок
#
# 7. Оптимизации:
#    - Минимизировать количество внешних проверок
#    - Кэшировать результаты ресурсоемких проверок
#    - Использовать асинхронные проверки для параллельного выполнения
#    - Добавлять circuit breakers для часто падающих зависимостей
#
# 8. Безопасность:
#    - Не раскрывать чувствительную информацию в ответах
#    - Ограничивать доступ к эндпоинту (firewall, authentication)
#    - Валидировать входящие запросы если требуется аутентификация
#
# 9. Мониторинг:
#    - Частота проверок: обычно 2-10 секунд
#    - Порог неудачных проверок: 1-3 подряд
#    - Порог успешных проверок: 1-2 подряд для возврата в работу
#    - Мониторить latency проверок готовности
#
# 10. В продакшн-среде можно добавить проверки:
#     - Доступность подключений к БД (пул соединений)
#     - Загрузка CPU и памяти процесса
#     - Наличие свободного места на диске
#     - Состояние кэшей (hit/miss ratio)
#     - Latency внешних API
#     - Rate limiting и throttling
#
# 11. Интеграция с Kubernetes:
#     - В Deployment указывается:
#       readinessProbe:
#         httpGet:
#           path: /readyz
#           port: 5000
#         initialDelaySeconds: 5
#         periodSeconds: 5
#         timeoutSeconds: 3
#         failureThreshold: 2
#         successThreshold: 1
#
# 12. Best Practices:
#     - Разделять проверки на критичные и некритичные
#     - Использовать разные таймауты для разных типов проверок
#     - Добавлять graceful degradation при частичных отказах
#     - Реализовывать retry logic для временных сбоев