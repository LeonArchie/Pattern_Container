# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

import requests
import logging
from datetime import datetime
from typing import Optional

# Кэшированные параметры
_module_name: Optional[str] = None
_audit_url: Optional[str] = None
_logger: Optional[logging.Logger] = None

def _load_config():
    """
    Загружает конфигурационные параметры из global.conf и кэширует их.
    """
    global _module_name, _audit_url
    
    try:
        with open('global.conf', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Парсим параметры из файла
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key == 'NAME_APP':
                        _module_name = value
                    elif key == 'URL_AUDIT_MODULES':
                        _audit_url = value
        
        # Проверяем, что все параметры загружены
        if not _module_name:
            raise ValueError("Параметр NAME_APP не найден в global.conf")
        if not _audit_url:
            raise ValueError("Параметр URL_AUDIT_MODULES не найден в global.conf")
            
        logging.info(f"Конфигурация аудита загружена: module={_module_name}, url={_audit_url}")
        
    except FileNotFoundError:
        logging.error("Файл global.conf не найден")
        raise
    except Exception as e:
        logging.error(f"Ошибка загрузки конфигурации: {e}")
        raise

def _ensure_initialized():
    """
    Проверяет инициализацию модуля, при необходимости загружает конфигурацию.
    """
    global _module_name, _audit_url, _logger
    
    if _module_name is None or _audit_url is None:
        _load_config()
    
    if _logger is None:
        _logger = logging.getLogger(__name__)

def audit(object_id: str, initiator_id: str, message: str) -> None:
    """
    Отправляет событие аудита в сервис аудита.
    
    :param object_id: ID объекта, с которым связано событие
    :param initiator_id: ID инициатора события
    :param message: Текст сообщения аудита
    """
    _ensure_initialized()
    
    try:
        # Формируем JSON сообщение
        audit_data = {
            "module_name": _module_name,
            "object_id": object_id,
            "initiator_id": initiator_id,
            "message": message,
            "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        # Формируем полный URL с добавлением /v1/create
        full_url = f"{_audit_url}/v1/create"
        
        # Отправляем POST запрос
        response = requests.post(
            full_url,
            json=audit_data,
            headers={'Content-Type': 'application/json'},
            timeout=10  # Таймаут 10 секунд
        )
        
        # Логируем результат
        if 200 <= response.status_code < 300:  # Все успешные коды 2xx
            _logger.info(f"Событие аудита успешно отправлено: {message}")
        else:
            _logger.error(
                f"Ошибка отправки события аудита. Код: {response.status_code}, "
                f"Ответ: {response.text}, Данные: {audit_data}"
            )
            
    except requests.exceptions.RequestException as e:
        _logger.error(f"Сетевая ошибка при отправке события аудита: {e}")
    except Exception as e:
        _logger.error(f"Неожиданная ошибка при отправке события аудита: {e}")

# Инициализация модуля при импорте
try:
    _ensure_initialized()
except Exception:
    # Подавляем ошибки инициализации при импорте, чтобы не ломать приложение
    # Ошибки проявятся при первом вызове audit()
    pass