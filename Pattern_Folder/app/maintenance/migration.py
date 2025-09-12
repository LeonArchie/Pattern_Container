# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

import os
import re
import hashlib
import time
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Глобальная переменная для отслеживания статуса миграций
migration_complete = False

class MigrationError(Exception):
    """Класс для ошибок миграции с детальным логированием"""
    def __init__(self, message: str, migration_file: Optional[str] = None):
        self.message = message
        self.migration_file = migration_file
        logger.critical(
            f"ОШИБКА МИГРАЦИИ{' (' + migration_file + ')' if migration_file else ''}: {message}",
            exc_info=True
        )
        super().__init__(message)

def _log_migration_step(step: str, details: str = "", level: str = "info") -> None:
    """Унифицированное логирование шагов миграции"""
    log_method = getattr(logger, level.lower(), logger.info)
    border = "=" * 40
    log_method(f"\n{border}\nМИГРАЦИЯ: {step}\n{details}\n{border}")

def get_app_name() -> str:
    """
    Получает имя приложения из файла global.conf
    """
    try:
        current_dir = Path(__file__).parent.parent
        config_file_path = current_dir / 'global.conf'
        
        if not config_file_path.exists():
            error_msg = f"Файл конфигурации не найден: {config_file_path}"
            _log_migration_step("Ошибка", error_msg, "error")
            raise MigrationError(error_msg)

        with open(config_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('NAME_APP='):
                    app_name = line.split('=', 1)[1].strip()
                    if app_name:
                        _log_migration_step("Имя приложения получено", f"NAME_APP: {app_name}")
                        return app_name

        error_msg = "Параметр NAME_APP не найден в global.conf"
        _log_migration_step("Ошибка", error_msg, "error")
        raise MigrationError(error_msg)
        
    except Exception as e:
        error_msg = f"Ошибка чтения конфигурации: {str(e)}"
        _log_migration_step("Критическая ошибка", error_msg, "critical")
        raise MigrationError(error_msg) from e

def get_migration_files() -> List[str]:
    """
    Получаем список файлов миграций в правильном порядке
    """
    try:
        current_dir = Path(__file__).parent.parent
        migrations_dir = current_dir / 'migrations'
        
        _log_migration_step("Поиск файлов миграций", f"Директория: {migrations_dir}")
        
        if not migrations_dir.exists():
            error_msg = f"Директория с миграциями не найдена: {migrations_dir}"
            _log_migration_step("Ошибка", error_msg, "error")
            raise MigrationError(error_msg)

        files = []
        valid_files = []
        
        for f in migrations_dir.iterdir():
            if f.is_file() and f.suffix == '.sql':
                files.append(f.name)
                if re.match(r'^\d{3}-.+\.sql$', f.name):
                    valid_files.append(f.name)

        _log_migration_step(
            "Найдены файлы",
            f"Всего: {len(files)}\n"
            f"Валидных миграций: {len(valid_files)}\n"
            f"Невалидных файлов: {len(files) - len(valid_files)}"
        )

        if not valid_files:
            _log_migration_step("Нет миграций", "Валидные миграции не найдены", "warning")
            return []

        sorted_files = sorted(valid_files)
        _log_migration_step(
            "Сортировка миграций",
            f"Первая миграция: {sorted_files[0]}\n"
            f"Последняя миграция: {sorted_files[-1]}\n"
            f"Всего миграций: {len(sorted_files)}"
        )
        
        return sorted_files
        
    except Exception as e:
        error_msg = f"Ошибка чтения директории миграций: {str(e)}"
        _log_migration_step("Критическая ошибка", error_msg, "critical")
        raise MigrationError(error_msg) from e

def check_migrations_table(session) -> None:
    """
    Проверяем наличие таблицы миграций и создаем если ее нет
    """
    try:
        _log_migration_step("Проверка таблицы applied_migrations")
        
        # Проверка существования таблица
        result = session.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'applied_migrations'
            )
        """))
        exists = result.scalar()
        
        if exists:
            _log_migration_step("Таблица существует", "Продолжение без создания")
            return

        _log_migration_step("Создание таблиции applied_migrations")
        
        # Создание таблицы с полем NAME_APP
        create_table_sql = """
            CREATE TABLE applied_migrations (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                name_app VARCHAR(255) NOT NULL,
                applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                checksum VARCHAR(64) NOT NULL,
                execution_time_ms FLOAT,
                status VARCHAR(20) NOT NULL DEFAULT 'success',
                error_message TEXT,
                UNIQUE(name, name_app)
            )
        """
        session.execute(text(create_table_sql))
        session.commit()
        
        _log_migration_step("Таблица создана", "Успешно создана таблица applied_migrations")
        
    except SQLAlchemyError as e:
        session.rollback()
        error_msg = f"Ошибка создания таблицы миграций: {str(e)}"
        _log_migration_step("Ошибка SQL", error_msg, "error")
        raise MigrationError(error_msg) from e
    except Exception as e:
        session.rollback()
        error_msg = f"Неожиданная ошибка при работе с таблицей миграций: {str(e)}"
        _log_migration_step("Критическая ошибка", error_msg, "critical")
        raise MigrationError(error_msg) from e

def get_applied_migrations(session, app_name: str) -> Dict[str, Tuple[str, float, str]]:
    """
    Получаем список примененных миграций для конкретного приложения
    """
    try:
        _log_migration_step("Получение списка примененных миграций", f"Приложение: {app_name}")
        
        result = session.execute(text("""
            SELECT name, checksum, execution_time_ms, status
            FROM applied_migrations 
            WHERE name_app = :app_name
            ORDER BY applied_at
        """), {"app_name": app_name})
        
        migrations = {}
        for row in result.fetchall():
            migrations[row[0]] = (row[1], row[2], row[3])
        
        _log_migration_step(
            "Полученные миграции",
            f"Найдено примененных миграций: {len(migrations)}\n"
            f"Успешных: {len([m for m in migrations.values() if m[2] == 'success'])}\n"
            f"С ошибками: {len([m for m in migrations.values() if m[2] == 'error'])}"
        )
        
        return migrations
        
    except SQLAlchemyError as e:
        error_msg = f"Ошибка получения списка миграций: {str(e)}"
        _log_migration_step("Ошибка SQL", error_msg, "error")
        raise MigrationError(error_msg) from e
    except Exception as e:
        error_msg = f"Неожиданная ошибка при получении миграций: {str(e)}"
        _log_migration_step("Критическая ошибка", error_msg, "critical")
        raise MigrationError(error_msg) from e

def calculate_checksum(file_path: Path) -> str:
    """
    Вычисляем SHA-256 контрольную сумму файла миграции
    """
    try:
        _log_migration_step("Вычисление контрольной суммы", f"Файл: {file_path.name}")
        
        with open(file_path, 'rb') as f:
            content = f.read()
            checksum = hashlib.sha256(content).hexdigest()
            
        _log_migration_step(
            "Контрольная сумма вычислена",
            f"Файл: {file_path.name}\n"
            f"Размер: {len(content)} байт\n"
            f"SHA-256: {checksum}"
        )
        
        return checksum
        
    except Exception as e:
        error_msg = f"Ошибка вычисления контрольной суммы для {file_path.name}: {str(e)}"
        _log_migration_step("Ошибка", error_msg, "error")
        raise MigrationError(error_msg, file_path.name) from e

def split_sql_statements(sql: str) -> List[str]:
    """
    Разбивает SQL-скрипт на отдельные запросы с поддержкой dollar-quoted строк.
    """
    _log_migration_step("Разбор SQL на отдельные запросы")
    
    statements = []
    current = ""
    in_dollar_quote = False
    dollar_tag = ""
    
    i = 0
    n = len(sql)
    
    while i < n:
        char = sql[i]
        
        # Обработка комментариев
        if not in_dollar_quote and char == '-' and i + 1 < n and sql[i+1] == '-':
            # Пропускаем однострочный комментарий
            while i < n and sql[i] != '\n':
                i += 1
            continue
        
        # Обработка dollar-quoted строк
        if char == '$' and not in_dollar_quote:
            # Проверяем начало dollar-quoted строки
            j = i + 1
            tag = ""
            while j < n and (sql[j].isalpha() or sql[j] == '_'):
                tag += sql[j]
                j += 1
            
            if j < n and sql[j] == '$':
                in_dollar_quote = True
                dollar_tag = tag
                current += sql[i:j+1]
                i = j + 1
                continue
        
        elif char == '$' and in_dollar_quote:
            # Проверяем конец dollar-quoted строки
            j = i + 1
            tag = ""
            while j < n and (sql[j].isalpha() or sql[j] == '_'):
                tag += sql[j]
                j += 1
            
            if j < n and sql[j] == '$' and tag == dollar_tag:
                in_dollar_quote = False
                current += sql[i:j+1]
                i = j + 1
                continue
        
        # Если не в dollar-quoted строке, ищем точку с запятой
        if char == ';' and not in_dollar_quote:
            current += char
            if current.strip():
                statements.append(current.strip())
            current = ""
            i += 1
            continue
        
        current += char
        i += 1
    
    # Добавляем последний statement если он есть
    if current.strip():
        statements.append(current.strip())
    
    _log_migration_step(
        "Результат разбора SQL",
        f"Всего запросов: {len(statements)}\n"
        f"Пример запроса: {statements[0][:100] + '...' if statements else 'нет'}"
    )
    
    return statements

def apply_migration(session, migration_file: str, app_name: str) -> bool:
    """
    Применяет одну миграцию. Возвращает True если успешно, False если ошибка.
    В случае ошибки выполняется откат всех изменений этой миграции.
    """
    start_time = time.time()
    current_dir = Path(__file__).parent.parent
    file_path = current_dir / 'migrations' / migration_file
    
    try:
        _log_migration_step(
            "Начало применения миграции",
            f"Файл: {migration_file}\n"
            f"Приложение: {app_name}"
        )
        
        # Вычисление контрольной суммы
        checksum = calculate_checksum(file_path)
        
        # Чтение SQL из файла
        with open(file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # Разбиение на отдельные запросы
        statements = split_sql_statements(sql)
        
        # Выполнение каждого запроса
        for i, query in enumerate(statements, 1):
            query_start = time.time()
            try:
                logger.debug(f"Выполнение запроса {i}/{len(statements)}: {query[:100]}...")
                session.execute(text(query))
                query_time = (time.time() - query_start) * 1000
                logger.debug(f"Запрос {i} выполнен за {query_time:.2f} мс")
            except Exception as e:
                logger.error(f"Ошибка в запросе {i}:\n{query[:500]}...")
                logger.error(f"Полная ошибка: {str(e)}")
                # При ошибке откатываем всю транзакцию
                session.rollback()
                raise
        
        # Фиксация миграции в БД
        execution_time = (time.time() - start_time) * 1000
        session.execute(
            text("""
                INSERT INTO applied_migrations 
                (name, name_app, checksum, execution_time_ms, status) 
                VALUES (:name, :name_app, :checksum, :execution_time, 'success')
            """),
            {
                "name": migration_file, 
                "name_app": app_name,
                "checksum": checksum,
                "execution_time": execution_time
            }
        )
        session.commit()
        
        _log_migration_step(
            "Миграция успешно применена",
            f"Файл: {migration_file}\n"
            f"Приложение: {app_name}\n"
            f"Контрольная сумма: {checksum}\n"
            f"Время выполнения: {execution_time:.2f} мс\n"
            f"Выполнено запросов: {len(statements)}"
        )
        
        return True
        
    except Exception as e:
        # Уже выполнен rollback в блоке выполнения запросов
        error_msg = f"Ошибка применения миграции {migration_file}: {str(e)}"
        
        # Записываем информацию об ошибке в БД (в отдельной транзакции)
        try:
            execution_time = (time.time() - start_time) * 1000
            session.execute(
                text("""
                    INSERT INTO applied_migrations 
                    (name, name_app, checksum, execution_time_ms, status, error_message) 
                    VALUES (:name, :name_app, :checksum, :execution_time, 'error', :error_message)
                """),
                {
                    "name": migration_file, 
                    "name_app": app_name,
                    "checksum": checksum,
                    "execution_time": execution_time,
                    "error_message": str(e)[:1000]
                }
            )
            session.commit()
        except Exception as db_error:
            logger.error(f"Ошибка записи информации об ошибке миграции: {db_error}")
            session.rollback()
        
        _log_migration_step("Ошибка", error_msg, "error")
        return False

def run_migrations() -> List[str]:
    """
    Выполняет все непримененные миграции по очереди.
    Если миграция завершается ошибкой, процесс останавливается и возвращается False.
    """

    global migration_complete

    total_start = time.time()
    applied_migrations = []
    
    try:
        app_name = get_app_name()
        
        _log_migration_step(
            "Запуск процесса миграций",
            f"Приложение: {app_name}\n"
            f"Стратегия: Остановка при первой ошибке"
        )
        
        from maintenance.database_connector import get_db_connector
        
        connector = get_db_connector()
        
        with connector.get_session() as session:
            # Проверка и создание таблицы миграций
            check_migrations_table(session)
            
            # Получение списка примененных и доступных миграций
            applied = set(get_applied_migrations(session, app_name).keys())
            all_files = set(get_migration_files())
            pending = sorted(all_files - applied)
            
            _log_migration_step(
                "Статус миграций",
                f"Приложение: {app_name}\n"
                f"Всего миграций доступно: {len(all_files)}\n"
                f"Уже применено: {len(applied)}\n"
                f"Ожидает применения: {len(pending)}\n"
                f"Список ожидающих: {', '.join(pending) if pending else 'нет'}"
            )
            
            if not pending:
                _log_migration_step(
                    "Нет новых миграций",
                    "Все миграции уже применены",
                    "info"
                )
                # Устанавливаем флаг завершения миграций
                migration_complete = True
                return []
            
            # Применение миграций по порядку
            for migration_file in pending:
                success = apply_migration(session, migration_file, app_name)
                if success:
                    applied_migrations.append(migration_file)
                else:
                    error_msg = f"Миграция {migration_file} завершилась ошибкой. Процесс остановлен."
                    _log_migration_step("Критическая ошибка", error_msg, "critical")
                    raise MigrationError(error_msg, migration_file)
            
            # Если все миграции успешно применены
            total_time = (time.time() - total_start) * 1000
            _log_migration_step(
                "Все миграции успешно применены",
                f"Приложение: {app_name}\n"
                f"Применено миграций: {len(applied_migrations)}\n"
                f"Общее время: {total_time:.2f} мс\n"
                f"Список примененных: {', '.join(applied_migrations)}"
            )
            
            # Устанавливаем флаг завершения миграций
            migration_complete = True
            
            return applied_migrations
            
    except Exception as e:
        total_time = (time.time() - total_start) * 1000
        _log_migration_step(
            "Процесс миграций завершен с ошибкой",
            f"Приложение: {app_name}\n"
            f"Применено миграций: {len(applied_migrations)}\n"
            f"Общее время: {total_time:.2f} мс\n"
            f"Последняя ошибка: {str(e)}",
            "critical"
        )
        raise MigrationError(f"Процесс миграций завершен с ошибкой: {str(e)}") from e

def check_migrations_status() -> Tuple[bool, str, List[str]]:
    """
    Проверяет статус миграций без их выполнения.
    Возвращает кортеж: (все_миграции_применены, сообщение_статуса, список_непримененных_миграций)
    """
    try:
        app_name = get_app_name()
        
        from maintenance.database_connector import get_db_connector
        connector = get_db_connector()
        
        with connector.get_session() as session:
            check_migrations_table(session)
            
            applied = set(get_applied_migrations(session, app_name).keys())
            all_files = set(get_migration_files())
            pending = sorted(all_files - applied)
            
            if not pending:
                return (True, "Все миграции применены", [])
            else:
                return (False, f"Ожидают применения {len(pending)} миграций", pending)
                
    except Exception as e:
        error_msg = f"Ошибка проверки статуса миграций: {str(e)}"
        logger.error(error_msg)
        return (False, error_msg, [])

def is_migration_complete() -> bool:
    """
    Проверяет, завершены ли все миграции.
    Использует глобальную переменную для кеширования результата.
    """
    global migration_complete

    # Если уже установлен флаг завершения, возвращаем True
    if migration_complete:
        return True
    
    # Проверяем статус миграций
    complete, message, pending = check_migrations_status()
    
    if complete:
        migration_complete = True
        return True
    
    return False

def get_migration_status() -> Dict:
    """
    Возвращает детальный статус миграций в виде словаря
    """
    try:
        app_name = get_app_name()
        
        from maintenance.database_connector import get_db_connector
        connector = get_db_connector()
        
        with connector.get_session() as session:
            check_migrations_table(session)
            
            applied = get_applied_migrations(session, app_name)
            all_files = set(get_migration_files())
            pending = sorted(all_files - set(applied.keys()))
            
            # Получаем детальную информацию о примененных миграциях
            applied_details = []
            for migration in sorted(applied.keys()):
                checksum, exec_time, status = applied[migration]
                applied_details.append({
                    'name': migration,
                    'checksum': checksum,
                    'execution_time_ms': exec_time,
                    'status': status
                })
            
            return {
                'app_name': app_name,
                'total_migrations': len(all_files),
                'applied_count': len(applied),
                'pending_count': len(pending),
                'pending_migrations': pending,
                'applied_migrations': applied_details,
                'all_complete': len(pending) == 0,
                'has_errors': any(m[2] == 'error' for m in applied.values())
            }
            
    except Exception as e:
        return {
            'app_name': 'unknown',
            'total_migrations': 0,
            'applied_count': 0,
            'pending_count': 0,
            'pending_migrations': [],
            'applied_migrations': [],
            'all_complete': False,
            'has_errors': True,
            'error': str(e)
        }