# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

# Импорт необходимых модулей
import logging  # Стандартный модуль логирования Python
import json     # Для форматирования логов в JSON
import sys      # Для работы с системными потоками ввода/вывода
from datetime import datetime  # Для временных меток

class StructuredFormatter(logging.Formatter):
    """
    Кастомный форматтер для структурированных логов в формате JSON.
    Наследуется от базового класса logging.Formatter.
    """
    def format(self, record):
        """
        Преобразует запись лога в структурированный JSON-объект.
        
        :param record: Запись лога, содержащая всю информацию о событии
        :return: JSON-строка с структурированными данными лога
        """
        # Базовая структура лога
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",  # Время в UTC в ISO-формате
            "level": record.levelname,      # Уровень логирования (INFO, WARNING и т.д.)
            "message": record.getMessage(),  # Текст сообщения
            "logger": record.name,           # Имя логгера
            "module": record.module,         # Имя модуля
            "function": record.funcName,     # Имя функции
            "line": record.lineno,           # Номер строки
        }
        
        # Если есть информация об исключении, добавляем её в лог
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Сериализуем в JSON с поддержкой Unicode (ensure_ascii=False)
        return json.dumps(log_data, ensure_ascii=False)

def setup_logging():
    """
    Настройка системы логирования для приложения.
    
    :return: Настроенный root-логгер
    """
    # Получаем root-логгер
    logger = logging.getLogger()
    
    # Устанавливаем уровень логирования (INFO включает INFO, WARNING, ERROR, CRITICAL)
    logger.setLevel(logging.INFO)
    
    # Создаем обработчик, который выводит логи в stdout
    # (рекомендуется для Docker/Kubernetes)
    handler = logging.StreamHandler(sys.stdout)
    
    # Устанавливаем наш кастомный форматтер
    handler.setFormatter(StructuredFormatter())
    
    # Очищаем существующие обработчики, если они есть
    # (предотвращает дублирование логов)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Добавляем наш обработчик к root-логгеру
    logger.addHandler(handler)

    return logger

# Основные принципы работы:
#
# 1. Преимущества структурированных логов:
#    - Легко парсятся системами сбора логов (ELK, Loki, Splunk)
#    - Позволяют делать сложные фильтрации и агрегации
#    - Сохраняют контекст событий
#
# 2. Формат вывода:
#    {
#      "timestamp": "2023-10-05T14:32:15.123456Z",
#      "level": "INFO",
#      "message": "Сервис запущен",
#      "logger": "app",
#      "module": "app",
#      "function": "main",
#      "line": 42,
#      "exception": "..."
#    }
#
# 3. Рекомендации по использованию:
#    - В production можно добавить дополнительные поля:
#      * "service": имя сервиса
#      * "version": версия приложения
#      * "environment": окружение (prod, stage, dev)
#    - Для Kubernetes можно добавить поля Pod/Deployment
#
# 4. Производительность:
#    - Форматирование в JSON добавляет небольшие накладные расходы
#    - Вывод в stdout не блокирует основной поток
#
# 5. Интеграция:
#    - Совместим с Docker (логи выводятся в stdout)
#    - Работает с Kubernetes-логированием
#    - Поддерживается всеми основными системами мониторинга