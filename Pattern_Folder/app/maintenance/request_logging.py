# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

import json
import time
import logging
from datetime import datetime
from flask import request
from typing import Dict, Any, Optional

# Настройка логгера
logger = logging.getLogger(__name__)

# Глобальная переменная для хранения времени начала обработки запроса
_request_start_time: Optional[float] = None

def _filter_sensitive_data(headers: Dict[str, str]) -> Dict[str, str]:
    """
    Фильтрация чувствительных данных из заголовков с подробным логированием
    
    Параметры:
        headers (Dict[str, str]): Исходные заголовки запроса
        
    Возвращает:
        Dict[str, str]: Заголовки с отфильтрованными чувствительными данными
    """
    sensitive_keys = ['authorization', 'cookie', 'token', 'set-cookie', 'x-api-key']
    logger.debug(f"Фильтрация чувствительных данных. Ключи для фильтрации: {sensitive_keys}")
    
    filtered = {}
    for k, v in headers.items():
        if any(sensitive in k.lower() for sensitive in sensitive_keys):
            filtered[k] = '***FILTERED***'
            logger.debug(f"Отфильтрован чувствительный заголовок: {k}")
        else:
            filtered[k] = v
    
    logger.debug(f"Заголовки после фильтрации: {filtered}")
    return filtered

def _get_request_body() -> Optional[Dict[str, Any]]:
    """
    Безопасное извлечение тела запроса с детальным логированием
    
    Возвращает:
        Optional[Dict[str, Any]]: Тело запроса или None если извлечь не удалось
    """
    try:
        if not request.data:
            logger.debug("Тело запроса пустое")
            return None
            
        content_type = request.content_type or ''
        logger.debug(f"Извлечение тела запроса. Content-Type: {content_type}")
        
        if 'application/json' in content_type:
            body = request.get_json(silent=False)
            logger.debug(f"Успешно извлечено JSON тело: {json.dumps(body, indent=2)}")
            return body
        elif 'multipart/form-data' in content_type:
            logger.debug("Тело запроса содержит multipart/form-data, пропускаем детализацию")
            return {'multipart_data': True}
        elif 'application/x-www-form-urlencoded' in content_type:
            logger.debug(f"Форма данных: {dict(request.form)}")
            return dict(request.form)
        else:
            body = request.data.decode('utf-8', errors='replace')
            logger.debug(f"Тело запроса (raw): {body[:1000]}...")  # Ограничение длины
            return {'raw_body': body}
            
    except Exception as e:
        logger.warning(f"Ошибка извлечения тела запроса: {str(e)}", exc_info=True)
        return None

def _get_response_body(response) -> Optional[Dict[str, Any]]:
    """
    Безопасное извлечение тела ответа с детальным логированием
    
    Параметры:
        response: Объект ответа Flask
        
    Возвращает:
        Optional[Dict[str, Any]]: Тело ответа или None если извлечь не удалось
    """
    try:
        content_type = response.content_type or ''
        logger.debug(f"Извлечение тела ответа. Content-Type: {content_type}")
        
        if 'application/json' in content_type:
            body = json.loads(response.get_data(as_text=True))
            logger.debug(f"Успешно извлечено JSON тело ответа: {json.dumps(body, indent=2)}")
            return body
        elif 'text/' in content_type:
            text = response.get_data(as_text=True)
            logger.debug(f"Текстовый ответ (первые 1000 символов): {text[:1000]}...")
            return {'text_response': text}
        else:
            logger.debug(f"Бинарный ответ. Длина: {response.content_length} байт")
            return None
            
    except Exception as e:
        logger.warning(f"Ошибка извлечения тела ответа: {str(e)}", exc_info=True)
        return None

def log_request_info():
    """
    Подробное логирование входящего запроса с таймингом
    """
    global _request_start_time
    _request_start_time = time.time()
    
    try:
        # Фильтрация заголовков
        filtered_headers = _filter_sensitive_data(dict(request.headers))
        
        # Формирование базовой информации
        request_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'method': request.method,
            'path': request.path,
            'endpoint': request.endpoint,
            'remote_addr': request.remote_addr,
            'user_agent': request.user_agent.string,
            'headers': filtered_headers,
            'query_params': dict(request.args),
            'content_type': request.content_type,
            'content_length': request.content_length,
        }
        
        # Добавление тела запроса
        request_body = _get_request_body()
        if request_body:
            request_info['request_body'] = request_body
        
        logger.info(
            f"Входящий запрос:\n{json.dumps(request_info, indent=2, ensure_ascii=False)}",
            extra={'request_info': request_info}
        )
        
    except Exception as e:
        logger.error(f"Ошибка логирования запроса: {str(e)}", exc_info=True)

def log_request_response(response):
    """
    Подробное логирование ответа с таймингом и статистикой
    
    Параметры:
        response: Объект ответа Flask
        
    Возвращает:
        response: Исходный объект ответа
    """
    try:
        global _request_start_time
        processing_time = (time.time() - _request_start_time) * 1000 if _request_start_time else None
        
        # Фильтрация заголовков
        filtered_headers = _filter_sensitive_data(dict(request.headers))
        
        # Формирование базовой информации
        response_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'method': request.method,
            'path': request.path,
            'status_code': response.status_code,
            'processing_time_ms': round(processing_time, 2) if processing_time else None,
            'remote_addr': request.remote_addr,
            'headers': filtered_headers,
            'query_params': dict(request.args),
            'response_content_type': response.content_type,
            'response_content_length': response.content_length,
        }
        
        # Добавление тел запроса и ответа
        request_body = _get_request_body()
        if request_body:
            response_info['request_body'] = request_body
            
        response_body = _get_response_body(response)
        if response_body:
            response_info['response_body'] = response_body
        
        # Логирование в зависимости от статуса ответа
        if response.status_code >= 500:
            logger.error(
                f"Ошибка сервера:\n{json.dumps(response_info, indent=2, ensure_ascii=False)}",
                extra={'response_info': response_info}
            )
        elif response.status_code >= 400:
            logger.warning(
                f"Ошибка клиента:\n{json.dumps(response_info, indent=2, ensure_ascii=False)}",
                extra={'response_info': response_info}
            )
        else:
            logger.info(
                f"Успешный ответ:\n{json.dumps(response_info, indent=2, ensure_ascii=False)}",
                extra={'response_info': response_info}
            )
            
    except Exception as e:
        logger.error(f"Ошибка логирования ответа: {str(e)}", exc_info=True)
    
    return response