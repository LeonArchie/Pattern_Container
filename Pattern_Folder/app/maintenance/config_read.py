# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

"""
config_read.py - Модуль для чтения конфигурационных параметров из удаленного сервиса

Этот модуль предоставляет функциональность для:
1. Чтения базового URL сервиса конфигураций из локального файла global.conf
2. Выполнения запросов к удаленному сервису конфигураций для получения параметров
3. Обработки ошибок и таймаутов при работе с сетевыми запросами
4. Предоставления единой точки доступа к конфигурации через синглтон-паттерн

Основные компоненты:
- Класс ConfigReader: основной класс для работы с конфигурацией
- Функция get_config_reader(): предоставляет глобальный экземпляр ConfigReader
- Функция read_config_param(): упрощенный интерфейс для чтения параметров

Сервис конфигураций ожидает запросы в формате:
{base_url}/v1/read/{file_name}/{parameter_path}

Пример использования:
    value = read_config_param("database", "host")
"""

import requests
import logging
import os
from typing import Optional, Any

# Настройка логгера для этого модуля
logger = logging.getLogger(__name__)

class ConfigReader:
    """Класс для чтения конфигурационных параметров из удаленного сервиса"""
    
    def __init__(self, config_file_path: str = None):
        """
        Инициализация ConfigReader
        
        :param config_file_path: путь к файлу global.conf. Если не указан,
                                используется путь по умолчанию относительно
                                расположения модуля
        """
        if config_file_path is None:
            # Определение пути к файлу global.conf относительно расположения модуля
            # Два уровня вверх от текущего файла
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_file_path = os.path.join(base_dir, "global.conf")
        
        self.config_file_path = config_file_path
        logger.info(f"Инициализация ConfigReader с файлом: {self.config_file_path}")
        
        # Чтение базового URL из конфигурационного файла
        self.base_url = self._read_config_url()
        logger.info(f"Базовый URL сервиса конфигураций: {self.base_url}")
    
    def _read_config_url(self) -> str:
        """
        Чтение URL_CONFIG_MODULES из файла global.conf
        
        Метод ищет в файле строку, начинающуюся с 'URL_CONFIG_MODULES='
        и извлекает значение URL после знака равенства.
        
        :return: базовый URL сервиса конфигураций
        :raises: FileNotFoundError если файл не существует
        :raises: ValueError если URL не найден в файле
        """
        try:
            logger.info(f"Попытка чтения конфигурационного файла: {self.config_file_path}")
            
            # Проверка существования файла конфигурации
            if not os.path.exists(self.config_file_path):
                error_msg = f"Файл конфигурации не найден: {self.config_file_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            # Открытие и чтение файла конфигурации
            with open(self.config_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.debug(f"Содержимое global.conf:\n{content}")
                
                # Сбрасываем указатель файла для повторного чтения построчно
                f.seek(0)
                
                # Построчный поиск параметра URL_CONFIG_MODULES
                for line_num, line in enumerate(f, 1):
                    line = line.strip()  # Удаление пробелов и переносов
                    logger.debug(f"Строка {line_num}: {line}")
                    
                    # Поиск строки с нужным параметром
                    if line.startswith('URL_CONFIG_MODULES='):
                        # Разделение строки по первому знаку равенства
                        url = line.split('=', 1)[1].strip()
                        if url:
                            logger.info(f"Найден URL_CONFIG_MODULES в строке {line_num}: {url}")
                            return url
                        else:
                            logger.warning(f"Пустой URL_CONFIG_MODULES в строке {line_num}")
            
            # Если параметр не был найден в файле
            error_msg = "URL_CONFIG_MODULES не найден в global.conf"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        except FileNotFoundError:
            # Повторное возбуждение исключения для обработки на верхнем уровне
            error_msg = f"Файл конфигурации не найден: {self.config_file_path}"
            logger.error(error_msg)
            raise
        except Exception as e:
            # Обработка любых других ошибок при чтении конфигурации
            error_msg = f"Ошибка чтения конфигурации: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def read_config(self, file_name: str, parameter_path: str) -> Optional[Any]:
        """
        Чтение параметра конфигурации из удаленного сервиса
        
        Выполняет HTTP GET запрос к сервису конфигураций для получения
        значения указанного параметра.
        
        :param file_name: имя конфигурационного файла (без расширения)
        :param parameter_path: путь к параметру в файле (например, "database/host")
        :return: значение параметра или None если не найдено или произошла ошибка
        """
        logger.info(f"Запрос конфигурации: файл='{file_name}', параметр='{parameter_path}'")
        
        try:
            # Формируем полный путь для запроса
            full_path = f"{file_name}/{parameter_path}"
            url = f"{self.base_url}/v1/read/{full_path}"
            
            logger.info(f"Формирование URL запроса: {url}")
            
            # Выполняем GET-запрос с таймаутом 10 секунд
            logger.debug(f"Выполнение GET запроса к: {url}")
            response = requests.get(url, timeout=10)
            
            # Логируем статус ответа
            logger.info(f"Ответ от сервера: HTTP {response.status_code}")
            logger.debug(f"Заголовки ответа: {dict(response.headers)}")
            
            # Проверяем статус ответа (вызывает исключение для кодов 4xx/5xx)
            response.raise_for_status()
            
            # Парсим JSON ответ
            data = response.json()
            logger.debug(f"Полный ответ JSON: {data}")
            
            # Проверяем структуру ответа (ожидаем ключ 'value')
            if 'value' in data:
                value = data['value']
                logger.info(f"Успешно получено значение параметра: {value} (тип: {type(value).__name__})")
                return value
            else:
                logger.warning(f"Неожиданная структура ответа, ключ 'value' отсутствует: {data}")
                return None
                
        except requests.exceptions.Timeout:
            # Обработка таймаута запроса
            error_msg = f"Таймаут запроса к сервису конфигураций: {url}"
            logger.error(error_msg)
            return None
            
        except requests.exceptions.ConnectionError:
            # Обработка ошибки подключения
            error_msg = f"Ошибка подключения к сервису конфигураций: {url}"
            logger.error(error_msg)
            return None
            
        except requests.exceptions.HTTPError as e:
            # Обработка HTTP ошибок (4xx, 5xx)
            error_msg = f"HTTP ошибка: {e.response.status_code} - {e.response.reason}"
            logger.error(error_msg)
            logger.debug(f"Тело ответа при ошибке: {e.response.text}")
            return None
            
        except requests.exceptions.RequestException as e:
            # Обработка других ошибок сетевых запросов
            error_msg = f"Ошибка сетевого запроса: {str(e)}"
            logger.error(error_msg)
            return None
            
        except ValueError as e:
            # Обработка ошибок парсинга JSON
            error_msg = f"Ошибка парсинга JSON ответа: {str(e)}"
            logger.error(error_msg)
            return None
            
        except Exception as e:
            # Обработка любых других неожиданных ошибок
            error_msg = f"Неожиданная ошибка при чтении конфигурации: {str(e)}"
            logger.error(error_msg)
            return None

# Глобальный экземпляр ConfigReader для реализации синглтон-паттерна
_config_reader = None

def get_config_reader() -> ConfigReader:
    """
    Получение глобального экземпляра ConfigReader (синглтон)
    
    Создает новый экземпляр только при первом вызове, последующие вызовы
    возвращают существующий экземпляр.
    
    :return: экземпляр ConfigReader
    """
    global _config_reader
    if _config_reader is None:
        logger.info("Создание нового экземпляра ConfigReader")
        _config_reader = ConfigReader()
    else:
        logger.debug("Использование существующего экземпляра ConfigReader")
    return _config_reader

def read_config_param(file_name: str, parameter_path: str) -> Optional[Any]:
    """
    Упрощенная функция для чтения параметра конфигурации
    
    Обертка вокруг методов ConfigReader для удобного использования
    без необходимости создавать экземпляр класса вручную.
    
    :param file_name: имя конфигурационного файла
    :param parameter_path: путь к параметру
    :return: значение параметра или None при ошибке
    """
    logger.info(f"Вызов read_config_param: {file_name}/{parameter_path}")
    reader = get_config_reader()
    return reader.read_config(file_name, parameter_path)