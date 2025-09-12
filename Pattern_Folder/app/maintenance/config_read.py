# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

import requests
import logging
import os
from typing import Optional, Any, Dict
from functools import lru_cache

# Настройка логгера
logger = logging.getLogger(__name__)

class ConfigReader:
    """Класс для чтения конфигурационных параметров из удаленного сервиса"""
    
    def __init__(self, config_file_path: str = None):
        """
        Инициализация ConfigReader
        
        :param config_file_path: путь к файлу global.conf
        """
        if config_file_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_file_path = os.path.join(base_dir, "global.conf")
        
        self.config_file_path = config_file_path
        logger.info(f"Инициализация ConfigReader с файлом: {self.config_file_path}")
        self.base_url = self._read_config_url()
        logger.info(f"Базовый URL сервиса конфигураций: {self.base_url}")
        
        # Кеш для хранения параметров конфигурации
        self._cache: Dict[str, Any] = {}
        
        # Кеш для статуса готовности сервиса конфигураций
        self._config_service_ready_cache: Optional[bool] = None
    
    def _read_config_url(self) -> str:
        """
        Чтение URL_CONFIG_MODULES из файла global.conf
        
        :return: базовый URL сервиса конфигураций
        :raises: ValueError если URL не найден
        """
        try:
            logger.info(f"Попытка чтения конфигурационного файла: {self.config_file_path}")
            
            if not os.path.exists(self.config_file_path):
                error_msg = f"Файл конфигурации не найден: {self.config_file_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            with open(self.config_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.debug(f"Содержимое global.conf:\n{content}")
                
                # Сбрасываем указатель файла для повторного чтения
                f.seek(0)
                
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    logger.debug(f"Строка {line_num}: {line}")
                    
                    if line.startswith('URL_CONFIG_MODULES='):
                        url = line.split('=', 1)[1].strip()
                        if url:
                            logger.info(f"Найден URL_CONFIG_MODULES в строке {line_num}: {url}")
                            return url
                        else:
                            logger.warning(f"Пустой URL_CONFIG_MODULES в строке {line_num}")
            
            error_msg = "URL_CONFIG_MODULES не найден в global.conf"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        except FileNotFoundError:
            error_msg = f"Файл конфигурации не найден: {self.config_file_path}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Ошибка чтения конфигурации: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def is_config_service_ready(self) -> bool:
        """
        Проверка готовности сервиса конфигураций с кешированием
        
        :return: True если сервис готов, False если нет
        """
        # Если есть кешированное значение, возвращаем его
        if self._config_service_ready_cache is not None:
            logger.debug(f"Использование кешированного статуса готовности сервиса: {self._config_service_ready_cache}")
            return self._config_service_ready_cache
        
        try:
            readyz_url = f"{self.base_url}/readyz"
            logger.debug(f"Проверка готовности сервиса конфигураций: {readyz_url}")
            
            # Выполняем GET-запрос с коротким таймаутом
            response = requests.get(readyz_url, timeout=3)
            
            # Проверяем только статус код, тело ответа не учитываем
            is_ready = response.status_code == 200
            logger.info(f"Сервис конфигураций {'готов' if is_ready else 'не готов'}, статус код: {response.status_code}")
            
            # Кешируем результат
            self._config_service_ready_cache = is_ready
            return is_ready
            
        except requests.exceptions.Timeout:
            logger.error("Таймаут при проверке готовности сервиса конфигураций")
            self._config_service_ready_cache = False
            return False
            
        except requests.exceptions.ConnectionError:
            logger.error("Ошибка подключения к сервису конфигураций при проверке готовности")
            self._config_service_ready_cache = False
            return False
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса готовности к сервису конфигураций: {e}")
            self._config_service_ready_cache = False
            return False
            
        except Exception as e:
            logger.error(f"Неожиданная ошибка при проверке готовности сервиса конфигураций: {e}")
            self._config_service_ready_cache = False
            return False
    
    def read_config(self, file_name: str, parameter_path: str) -> Optional[Any]:
        """
        Чтение параметра конфигурации из удаленного сервиса с кешированием
        
        :param file_name: имя конфигурационного файла (без расширения)
        :param parameter_path: путь к параметру в файле
        :return: значение параметра или None если не найдено
        """
        logger.info(f"Запрос конфигурации: файл='{file_name}', параметр='{parameter_path}'")
        
        # Формируем ключ для кеша
        cache_key = f"{file_name}/{parameter_path}"
        
        # Проверяем наличие значения в кеше
        if cache_key in self._cache:
            logger.info(f"Найдено в кеше: {cache_key}")
            return self._cache[cache_key]
        
        try:
            # Формируем полный путь для запроса
            full_path = f"{file_name}/{parameter_path}"
            url = f"{self.base_url}/v1/read/{full_path}"
            
            logger.info(f"Формирование URL запроса: {url}")
            
            # Выполняем GET-запрос
            logger.debug(f"Выполнение GET запроса к: {url}")
            response = requests.get(url, timeout=10)
            
            # Логируем статус ответа
            logger.info(f"Ответ от сервера: HTTP {response.status_code}")
            logger.debug(f"Заголовки ответа: {dict(response.headers)}")
            
            response.raise_for_status()  # Вызовет исключение для кодов 4xx/5xx
            
            # Парсим JSON ответ
            data = response.json()
            logger.debug(f"Полный ответ JSON: {data}")
            
            # Проверяем структуру ответа
            if 'value' in data:
                value = data['value']
                logger.info(f"Успешно получено значение параметра: {value} (тип: {type(value).__name__})")
                
                # Сохраняем в кеш
                self._cache[cache_key] = value
                logger.debug(f"Значение сохранено в кеш с ключом: {cache_key}")
                
                return value
            else:
                logger.warning(f"Неожиданная структура ответа, ключ 'value' отсутствует: {data}")
                return None
                
        except requests.exceptions.Timeout:
            error_msg = f"Таймаут запроса к сервису конфигураций: {url}"
            logger.error(error_msg)
            return None
            
        except requests.exceptions.ConnectionError:
            error_msg = f"Ошибка подключения к сервису конфигураций: {url}"
            logger.error(error_msg)
            return None
            
        except requests.exceptions.HTTPError as e:
            # Логируем статус код и тело ответа при HTTP ошибках
            status_code = e.response.status_code
            response_text = e.response.text
            error_msg = f"HTTP ошибка: {status_code} - {e.response.reason}. Тело ответа: {response_text}"
            logger.error(error_msg)
            return None
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Ошибка сетевого запроса: {str(e)}"
            logger.error(error_msg)
            return None
            
        except ValueError as e:
            error_msg = f"Ошибка парсинга JSON ответа: {str(e)}"
            logger.error(error_msg)
            return None
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка при чтении конфигурации: {str(e)}"
            logger.error(error_msg)
            return None
    
    def clear_cache(self):
        """
        Очистка кеша конфигурационных параметров и статуса готовности
        """
        logger.info("Очистка кеша конфигурационных параметров и статуса готовности")
        self._cache.clear()
        self._config_service_ready_cache = None
    
    def get_cache_size(self) -> int:
        """
        Получение текущего размера кеша
        
        :return: количество элементов в кеше
        """
        return len(self._cache)

# Глобальный экземпляр для удобства использования
_config_reader = None

def get_config_reader() -> ConfigReader:
    """
    Получение глобального экземпляра ConfigReader (синглтон)
    
    :return: экземпляр ConfigReader
    """
    global _config_reader
    if _config_reader is None:
        logger.info("Создание нового экземпляра ConfigReader")
        _config_reader = ConfigReader()
    else:
        logger.debug("Использование существующего экземпляра ConfigReader")
    return _config_reader

@lru_cache(maxsize=128)
def read_config_param(file_name: str, parameter_path: str) -> Optional[Any]:
    """
    Упрощенная функция для чтения параметра конфигурации с кешированием
    
    :param file_name: имя конфигурационного файла
    :param parameter_path: путь к параметру
    :return: значение параметра
    """
    logger.info(f"Вызов read_config_param: {file_name}/{parameter_path}")
    reader = get_config_reader()
    return reader.read_config(file_name, parameter_path)

def is_config_service_ready() -> bool:
    """
    Проверка готовности сервиса конфигураций
    
    :return: True если сервис готов, False если нет
    """
    logger.info("Проверка готовности сервиса конфигураций")
    reader = get_config_reader()
    return reader.is_config_service_ready()

def clear_config_cache():
    """
    Очистка кеша конфигурационных параметров
    """
    logger.info("Очистка глобального кеша конфигурационных параметров")
    reader = get_config_reader()
    reader.clear_cache()
    read_config_param.cache_clear()

def get_config_cache_size() -> int:
    """
    Получение текущего размера кеша конфигурационных параметров
    
    :return: количество элементов в кеше
    """
    reader = get_config_reader()
    return reader.get_cache_size()