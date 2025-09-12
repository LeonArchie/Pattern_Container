# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

import os
import json
import logging
import time
from typing import Optional, Iterator, Dict, Any
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import (
    OperationalError,
    DatabaseError,
    DataError,
    IntegrityError,
    ProgrammingError,
    InternalError,
    InterfaceError,
    TimeoutError,
    SQLAlchemyError
)
from contextlib import contextmanager

# Загрузка переменных окружения из .env файла
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(dotenv_path)

# Импорт ConfigReader
from maintenance.config_read import get_config_reader, read_config_param

# Настройка логгера
logger = logging.getLogger(__name__)

class DatabaseErrorHandler:
    """Класс для обработки ошибок базы данных с детальным логированием."""
    
    ERROR_MAPPING = {
        OperationalError: {
            'code': 'db_connection_error',
            'message': "Ошибка подключения к базе данных",
            'log_level': logging.ERROR,
            'retryable': True
        },
        DataError: {
            'code': 'db_data_error',
            'message': "Ошибка данных в запросе",
            'log_level': logging.WARNING,
            'retryable': False
        },
        IntegrityError: {
            'code': 'db_integrity_error',
            'message': "Нарушение целостности данных",
            'log_level': logging.WARNING,
            'retryable': False
        },
        ProgrammingError: {
            'code': 'db_programming_error',
            'message': "Ошибка в SQL запросе",
            'log_level': logging.ERROR,
            'retryable': False
        },
        InternalError: {
            'code': 'db_internal_error',
            'message': "Внутренняя ошибка базы данных",
            'log_level': logging.CRITICAL,
            'retryable': True
        },
        InterfaceError: {
            'code': 'db_interface_error',
            'message': "Ошибка интерфейса базы данных",
            'log_level': logging.CRITICAL,
            'retryable': True
        },
        TimeoutError: {
            'code': 'db_timeout_error',
            'message': "Таймаут операции с базой данных",
            'log_level': logging.WARNING,
            'retryable': True
        },
        DatabaseError: {
            'code': 'db_generic_error',
            'message': "Ошибка базы данных",
            'log_level': logging.ERROR,
            'retryable': False
        }
    }
    
    @classmethod
    def handle_error(cls, error: SQLAlchemyError, context: Optional[Dict[str, Any]] = None) -> None:
        """Обработка ошибки базы данных с детальным логированием контекста."""
        error_type = type(error)
        error_info = cls.ERROR_MAPPING.get(error_type, {
            'code': 'db_unknown_error',
            'message': "Неизвестная ошибка базы данных",
            'log_level': logging.CRITICAL,
            'retryable': False
        })
        
        # Формирование детального сообщения об ошибке
        error_details = [
            f"Тип: {error_type.__name__}",
            f"Код: {error_info['code']}",
            f"Сообщение: {str(error)}",
            f"Можно повторить: {'Да' if error_info['retryable'] else 'Нет'}"
        ]
        
        if context:
            error_details.append("Контекст ошибки:")
            for key, value in context.items():
                error_details.append(f"  {key}: {value}")
        
        logger.log(
            error_info['log_level'],
            "\n".join(error_details),
            exc_info=True
        )
        
        raise RuntimeError(f"{error_info['message']} (код: {error_info['code']})") from error

class DatabaseConnector:
    """Класс для управления подключениями к базе данных PostgreSQL."""
    
    def __init__(self):
        """Инициализация коннектора к базе данных."""
        self.engine = None
        self.SessionLocal = None
        self.Base = declarative_base()
        self._initialized = False
        self.config = {}
        
        logger.info("Инициализация DatabaseConnector")
    
    def _log_db_operation(self, operation: str, details: str = "", level: str = "info") -> None:
        """Унифицированное логирование операций с БД."""
        log_method = getattr(logger, level.lower(), logger.info)
        border = "=" * 60
        log_method(f"\n{border}\nOPERATION: {operation}\n{details}\n{border}")
    
    def _get_config_param_with_retry(self, file_name: str, param_path: str, max_retries: int = 10, retry_delay: float = 5.0) -> Any:
        """
        Получение параметра конфигурации с повторными попытками.
        
        :param file_name: имя файла конфигурации
        :param param_path: путь к параметру
        :param max_retries: максимальное количество попыток
        :param retry_delay: задержка между попытками в секундах
        :return: значение параметра
        :raises: RuntimeError если параметр не получен после всех попыток
        """
        for attempt in range(1, max_retries + 1):
            try:
                value = read_config_param(file_name, param_path)
                if value is not None:
                    logger.info(f"Параметр {param_path} успешно получен: {value}")
                    return value
                else:
                    logger.warning(f"Параметр {param_path} не найден (попытка {attempt}/{max_retries})")
            except Exception as e:
                logger.warning(f"Ошибка получения параметра {param_path} (попытка {attempt}/{max_retries}): {str(e)}")
            
            if attempt < max_retries:
                logger.info(f"Повторная попытка получения параметра {param_path} через {retry_delay} сек")
                time.sleep(retry_delay)
        
        error_msg = f"Не удалось получить параметр {param_path} после {max_retries} попыток"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    def _load_configuration(self) -> None:
        """Загрузка конфигурации из ConfigReader и .env файла с повторными попытками."""
        self._log_db_operation("Загрузка конфигурации БД")
        
        try:
            # Загрузка конфигурации из ConfigReader с повторными попытками
            config_params = {
                'master_host': ('db', 'master_host'),
                'master_port': ('db', 'master_port'),
                'database': ('db', 'database'),
                'pool_size': ('db', 'pool_size'),
                'max_overflow': ('db', 'max_overflow'),
                'pool_timeout': ('db', 'pool_timeout'),
                'pool_recycle': ('db', 'pool_recycle'),
                'pool_pre_ping': ('db', 'pool_pre_ping'),
                'pool_use_lifo': ('db', 'pool_use_lifo'),
                'max_retries': ('db', 'max_retries'),
                'retry_delay': ('db', 'retry_delay')
            }
            
            for key, (file_name, param_path) in config_params.items():
                value = self._get_config_param_with_retry(file_name, param_path)
                self.config[key] = value
                logger.info(f"Конфигурация {key}: {value}")
            
            # Загрузка учетных данных из .env
            self.config['user'] = os.getenv('DATABASE_USER')
            self.config['password'] = os.getenv('DB_PASSWORD')
            
            if not self.config['user']:
                raise ValueError("Не найден DATABASE_USER в .env файле")
            if not self.config['password']:
                raise ValueError("Не найден DB_PASSWORD в .env файле")
                
            logger.info("Учетные данные успешно загружены из .env")
            
            self._log_db_operation(
                "Конфигурация загружена",
                f"Параметры:\n{json.dumps({k: v for k, v in self.config.items() if k != 'password'}, indent=2, default=str)}"
            )
            
        except Exception as e:
            self._log_db_operation(
                "Ошибка загрузки конфигурации",
                f"Тип ошибки: {type(e).__name__}\nСообщение: {str(e)}",
                "error"
            )
            raise
    
    def _get_connection_string(self) -> str:
        """Генерация строки подключения к БД."""
        self._log_db_operation(
            "Генерация строки подключения",
            f"Хост: {self.config['master_host']}\n"
            f"Порт: {self.config['master_port']}\n"
            f"База данных: {self.config['database']}\n"
            f"Пользователь: {self.config['user']}"
        )
        
        return (
            f"postgresql://{self.config['user']}:{self.config['password']}@"
            f"{self.config['master_host']}:{self.config['master_port']}/"
            f"{self.config['database']}"
        )
    
    def initialize(self) -> None:
        """Инициализация подключения к базе данных."""
        if self._initialized:
            self._log_db_operation(
                "Повторная инициализация БД",
                "Попытка повторной инициализации уже работающего подключения",
                "warning"
            )
            return
        
        start_time = time.time()
        
        try:
            # Загрузка конфигурации
            self._load_configuration()
            
            # Создание строки подключения
            connection_string = self._get_connection_string()
            logger.debug(f"Полная строка подключения: {connection_string}")
            
            # Создание engine
            self._log_db_operation("Создание engine БД")
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=int(self.config['pool_size']),
                max_overflow=int(self.config['max_overflow']),
                pool_timeout=int(self.config['pool_timeout']),
                pool_recycle=int(self.config['pool_recycle']),
                pool_pre_ping=bool(self.config['pool_pre_ping']),
                pool_use_lifo=bool(self.config['pool_use_lifo']),
                echo=False,
                connect_args={
                    'connect_timeout': 5,
                    'application_name': 'EOS_App'
                }
            )
            
            # Проверка подключения
            self._log_db_operation("Проверка подключения к БД")
            test_start = time.time()
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1, version()"))
                row = result.fetchone()
                test_time = (time.time() - test_start) * 1000
                
                self._log_db_operation(
                    "Проверка подключения успешна",
                    f"Время выполнения: {test_time:.2f} мс\n"
                    f"Результат: {row[0]}\n"
                    f"Версия СУБД: {row[1]}"
                )
            
            # Инициализация сессий
            self._log_db_operation("Инициализация сессий БД")
            self.SessionLocal = scoped_session(
                sessionmaker(
                    autocommit=False,
                    autoflush=False,
                    bind=self.engine,
                    expire_on_commit=False
                )
            )
            
            self._initialized = True
            init_time = (time.time() - start_time) * 1000
            self._log_db_operation(
                "Инициализация БД завершена",
                f"Общее время: {init_time:.2f} мс\n"
                f"Размер пула: {self.config['pool_size']}\n"
                f"Макс. переполнение: {self.config['max_overflow']}"
            )
            
        except Exception as e:
            init_time = (time.time() - start_time) * 1000
            self._log_db_operation(
                "Ошибка инициализации БД",
                f"Время до ошибки: {init_time:.2f} мс\n"
                f"Тип ошибки: {type(e).__name__}",
                "critical"
            )
            if self.engine:
                self.engine.dispose()
            raise
    
    def is_healthy(self) -> bool:
        """Проверка работоспособности базы данных."""
        if not self._initialized:
            logger.warning("Попытка проверки здоровья неинициализированной БД")
            return False
        
        try:
            start_time = time.time()
            with self.engine.connect() as conn:
                # Выполняем простой запрос для проверки соединения
                result = conn.execute(text("SELECT 1"))
                health_check = result.scalar() == 1
                
                check_time = (time.time() - start_time) * 1000
                
                if health_check:
                    logger.info(f"Проверка здоровья БД успешна: {check_time:.2f} мс")
                else:
                    logger.warning(f"Проверка здоровья БД не прошла: {check_time:.2f} мс")
                
                return health_check
                
        except Exception as e:
            logger.error(f"Ошибка при проверке здоровья БД: {str(e)}")
            return False
    
    @contextmanager
    def get_session(self) -> Iterator[scoped_session]:
        """Контекстный менеджер для работы с сессией БД."""
        if not self._initialized:
            error_msg = "Попытка создать сессию неинициализированной БД"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        session = self.SessionLocal()
        session_id = id(session)
        
        try:
            logger.info(f"Открытие сессии БД (ID: {session_id})")
            
            yield session
            
            session.commit()
            logger.info(f"Сессия БД успешно завершена (ID: {session_id})")
            
        except SQLAlchemyError as e:
            session.rollback()
            DatabaseErrorHandler.handle_error(e, {
                'session_id': session_id,
                'operation': 'session_commit'
            })
            
        except Exception as e:
            session.rollback()
            logger.error(
                f"Неожиданная ошибка в сессии {session_id}: {str(e)}",
                exc_info=True
            )
            raise RuntimeError("Неожиданная ошибка при работе с БД") from e
            
        finally:
            session.close()
            if self.SessionLocal.registry.has():
                self.SessionLocal.remove()
            logger.info(f"Сессия БД закрыта (ID: {session_id})")
    
    def close(self) -> None:
        """Закрытие пула подключений к БД."""
        if not self.engine:
            self._log_db_operation(
                "Закрытие пула подключений",
                "Попытка закрыть несуществующий engine",
                "warning"
            )
            return
        
        self._log_db_operation(
            "Начало закрытия пула подключений",
            f"Текущий размер пула: {self.engine.pool.size()}\n"
            f"Активные соединения: {self.engine.pool.checkedout()}"
        )
        
        start_time = time.time()
        try:
            self.engine.dispose()
            self._initialized = False
            
            self._log_db_operation(
                "Пул подключений закрыт",
                f"Время выполнения: {(time.time() - start_time) * 1000:.2f} мс"
            )
        except Exception as e:
            self._log_db_operation(
                "Ошибка закрытия пула",
                f"Тип: {type(e).__name__}\nСообщение: {str(e)}",
                "error"
            )
            raise
    
    def is_initialized(self) -> bool:
        """Проверка инициализации подключения к БД."""
        return self._initialized

# Глобальный экземпляр для удобства использования
_db_connector = None

def get_db_connector() -> DatabaseConnector:
    """
    Получение глобального экземпляра DatabaseConnector (синглтон).
    
    :return: экземпляр DatabaseConnector
    """
    global _db_connector
    if _db_connector is None:
        logger.info("Создание нового экземпляра DatabaseConnector")
        _db_connector = DatabaseConnector()
    else:
        logger.debug("Использование существующего экземпляра DatabaseConnector")
    return _db_connector

def initialize_database() -> None:
    """Инициализация базы данных."""
    connector = get_db_connector()
    connector.initialize()

def close_database() -> None:
    """Закрытие подключения к базе данных."""
    connector = get_db_connector()
    connector.close()

def is_database_healthy() -> bool:
    """Проверка работоспособности базы данных."""
    connector = get_db_connector()
    return connector.is_healthy()

def is_database_initialized() -> bool:
    """Проверка инициализации базы данных."""
    connector = get_db_connector()
    return connector.is_initialized()

def wait_for_database_connection(max_retries: int = 10, retry_delay: float = 5.0) -> bool:
    """
    Ожидание успешного подключения к базе данных с экспоненциальной задержкой.
    
    :param max_retries: Максимальное количество попыток
    :param retry_delay: Базовая задержка между попытками в секундах
    :return: True если подключение установлено, False если все попытки исчерпаны
    """
    logger.info(
        f"Начало подключения к БД: "
        f"макс. попыток={max_retries}, "
        f"базовая задержка={retry_delay} сек"
    )
    
    connector = get_db_connector()
    start_time = time.time()
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Попытка подключения {attempt}/{max_retries}")
            
            if not connector.is_initialized():
                connector.initialize()
            
            # Проверка здоровья БД
            if connector.is_healthy():
                total_time = time.time() - start_time
                logger.info(f"Подключение к БД успешно установлено за {total_time:.2f} сек")
                return True
                
        except Exception as e:
            logger.warning(f"Ошибка подключения к БД (попытка {attempt}): {str(e)}")
            
            if attempt < max_retries:
                # Экспоненциальная задержка
                current_delay = retry_delay * (1.5 ** (attempt - 1))
                current_delay = min(current_delay, 30)  # Максимальная задержка 30 сек
                
                logger.info(f"Повторная попытка через {current_delay:.1f} сек")
                time.sleep(current_delay)
    
    total_time = time.time() - start_time
    logger.error(f"Не удалось подключиться к БД после {max_retries} попыток за {total_time:.2f} сек")
    return False