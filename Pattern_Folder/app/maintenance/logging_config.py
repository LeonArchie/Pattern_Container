# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

"""
logging_config.py - Модуль настройки системы логирования для приложения

Этот модуль предоставляет конфигурацию для структурированного логирования
в формате JSON, что особенно полезно для работы в контейнеризованных средах
и интеграции с системами сбора логов (ELK Stack, Loki, Splunk и др.).

Основные возможности:
- Форматирование логов в JSON для легкого парсинга
- Вывод в stdout (рекомендуется для Docker/Kubernetes)
- Включение метаданных: временные метки, уровень, модуль, функция, строка
- Поддержка исключений с traceback

Использование:
    from maintenance.logging_config import setup_logging
    logger = setup_logging()
    logger.info("Сообщение для логирования")

Формат выходных данных:
{
    "timestamp": "2023-10-05T14:32:15.123456Z",
    "level": "INFO",
    "message": "Текст сообщения",
    "logger": "имя_логгера",
    "module": "имя_модуля",
    "function": "имя_функции",
    "line": 42,
    "exception": "текст исключения (при наличии)"
}
"""

# Импорт необходимых модулей
import logging  # Стандартный модуль логирования Python
import json     # Для форматирования логов в JSON
import sys      # Для работы с системными потоками ввода/вывода
from datetime import datetime  # Для временных меток

class StructuredFormatter(logging.Formatter):
    """
    Кастомный форматтер для структурированных логов в формате JSON.
    
    Наследуется от базового класса logging.Formatter и переопределяет
    метод format для преобразования записей лога в JSON-объекты.
    
    Особенности:
    - Автоматическое добавление временных меток в UTC
    - Включение контекстной информации (модуль, функция, строка)
    - Поддержка исключений с полным traceback
    - Сериализация в JSON с поддержкой Unicode
    """
    
    def format(self, record):
        """
        Преобразует запись лога в структурированный JSON-объект.
        
        :param record: Запись лога, содержащая всю информацию о событии
        :return: JSON-строка с структурированными данными лога
        """
        # Базовая структура лога с обязательными полями
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",  # Время в UTC в ISO-формате с Z-суффиксом
            "level": record.levelname,      # Уровень логирования (INFO, WARNING, ERROR, CRITICAL)
            "message": record.getMessage(),  # Текст сообщения (обработанные аргументы)
            "logger": record.name,           # Имя логгера (обычно имя модуля)
            "module": record.module,         # Имя модуля, где произошло событие
            "function": record.funcName,     # Имя функции, где произошло событие
            "line": record.lineno,           # Номер строки кода, где произошло событие
        }
        
        # Если есть информация об исключении, добавляем её в лог
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Сериализуем в JSON с поддержкой Unicode (ensure_ascii=False для кириллицы)
        # и возвращаем строку
        return json.dumps(log_data, ensure_ascii=False)

def setup_logging():
    """
    Настройка системы логирования для приложения.
    
    Функция выполняет базовую настройку логирования:
    - Устанавливает уровень логирования INFO
    - Создает обработчик для вывода в stdout
    - Применяет кастомный JSON-форматтер
    - Очищает существующие обработчики для предотвращения дублирования
    
    :return: Настроенный root-логгер
    """
    # Получаем root-логгер (базовый логгер для всего приложения)
    logger = logging.getLogger()
    
    # Устанавливаем уровень логирования
    # INFO включает логи уровня INFO, WARNING, ERROR, CRITICAL
    # DEBUG не включается для production-окружения
    logger.setLevel(logging.INFO)
    
    # Создаем обработчик, который выводит логи в stdout
    # Это рекомендуется для Docker/Kubernetes окружений, где
    # контейнеры перенаправляют stdout в системы централизованного логирования
    handler = logging.StreamHandler(sys.stdout)
    
    # Устанавливаем наш кастомный JSON-форматтер для обработчика
    handler.setFormatter(StructuredFormatter())
    
    # Очищаем существующие обработчики, если они есть
    # Это предотвращает дублирование логов при многократном вызове setup_logging
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Добавляем наш обработчик к root-логгеру
    # Все дочерние логгеры будут наследовать эту конфигурацию
    logger.addHandler(handler)

    return logger

# Основные принципы работы:
# Формат вывода:
#    {
#      "timestamp": "2023-10-05T14:32:15.123456Z",  # Время события в UTC
#      "level": "INFO",                             # Уровень серьезности
#      "message": "Сервис запущен",                 # Текст сообщения
#      "logger": "app",                             # Имя логгера
#      "module": "app",                             # Модуль источника
#      "function": "main",                          # Функция источника
#      "line": 42,                                  # Номер строки
#      "exception": "Traceback (most recent call last):..."  # Информация об исключении
#    }
#