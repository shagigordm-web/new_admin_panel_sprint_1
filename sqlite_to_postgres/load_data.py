import sqlite3
import psycopg2
from psycopg2.extras import DictCursor
from collections.abc import Generator
from dataclasses import dataclass
from uuid import UUID
from typing import Optional
from datetime import date, datetime

BATCH_SIZE = 4

@dataclass
class FilmWork:
    id: str
    title: str
    type: str
    description: Optional[str] = None
    creation_date: Optional[date] = None
    rating: Optional[float] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    file_path: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@dataclass
class Genre:
    id: str
    name: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@dataclass
class GenreFilmWork:
    id: str
    film_work_id: str
    genre_id: str
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.genre_id, str):
            self.genre_id = UUID(self.genre_id)

@dataclass
class Person:
    id: str
    full_name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@dataclass
class PersonFilmWork:
    id: str
    film_work_id: str
    person_id: str
    role: str
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.person_id, str):
            self.person_id = UUID(self.person_id)

SQL_INSERT_MAP = {
    FilmWork: """
        INSERT INTO content.film_work (
            id, title, type, description, creation_date, 
            file_path, rating, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            creation_date = EXCLUDED.creation_date,
            file_path = EXCLUDED.file_path,
            rating = EXCLUDED.rating,
            type = EXCLUDED.type,
            updated_at = EXCLUDED.updated_at
    """,
    
    Genre: """
        INSERT INTO content.genre (id, name, description, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            updated_at = EXCLUDED.updated_at
    """,
    
    GenreFilmWork: """
        INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            film_work_id = EXCLUDED.film_work_id,
            genre_id = EXCLUDED.genre_id
    """,
    
    Person: """
        INSERT INTO content.person (id, full_name, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            updated_at = EXCLUDED.updated_at
    """,
    
    PersonFilmWork: """
        INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            film_work_id = EXCLUDED.film_work_id,
            person_id = EXCLUDED.person_id,
            role = EXCLUDED.role
    """
}

DATA_MAP = {
    FilmWork: lambda obj: (
        str(obj.id), obj.title, obj.type, obj.description,
        obj.creation_date, obj.file_path, obj.rating,
        obj.created_at, obj.updated_at
    ),
    
    Genre: lambda obj: (
        str(obj.id), obj.name, obj.description,
        obj.created_at, obj.updated_at
    ),
    
    GenreFilmWork: lambda obj: (
        str(obj.id), str(obj.film_work_id), str(obj.genre_id),
        obj.created_at
    ),
    
    Person: lambda obj: (
        str(obj.id), obj.full_name,
        obj.created_at, obj.updated_at
    ),
    
    PersonFilmWork: lambda obj: (
        str(obj.id), str(obj.film_work_id), str(obj.person_id),
        obj.role, obj.created_at
    )
}

class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.conn.row_factory = sqlite3.Row

    def extract_data(self, table_name: str) -> Generator[list[sqlite3.Row], None, None]:
        """Извлекает данные из SQLite батчами"""
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT * FROM {table_name}')
        while results := cursor.fetchmany(BATCH_SIZE):
            yield results

    def load_table_data(self, table_name: str, data_class) -> Generator[list, None, None]:
        """Загружает данные из указанной таблицы и преобразует в объекты"""
        for batch in self.extract_data(table_name):
            # Преобразуем каждую строку в объект data_class
            objects_batch = [data_class(**dict(row)) for row in batch]
            yield objects_batch

class PostgresSaver:
    def __init__(self, connection: psycopg2.extensions.connection):
        self.conn = connection

    def save_batch(self, batch: list):
        """Сохраняет батч, автоматически определяя класс"""
        if not batch:
            return
        
        obj_class = type(batch[0])  # Определяем класс из первого объекта
        sql_query = SQL_INSERT_MAP[obj_class]
        convert_func = DATA_MAP[obj_class]
        
        data = [convert_func(obj) for obj in batch]
        with self.conn.cursor() as cursor:
            cursor.executemany(sql_query, data)
            self.conn.commit()

    def save_all_data(self, data_generator: Generator[list, None, None]):
        """Сохраняет все данные из генератора"""
        for batch_no, batch in enumerate(data_generator, start=1):
            print(f'Сохраняем батч #{batch_no}, объектов: {len(batch)}')
            self.save_batch(batch)
            print(f'Батч #{batch_no} успешно сохранен')
            print('---')

def verify_data_migration(sqlite_conn: sqlite3.Connection, pg_conn: psycopg2.extensions.connection):
    """Проверяет целостность данных после миграции из SQLite в PostgreSQL с использованием батчей"""
    
    # Определяем соответствие таблиц и их названий в PostgreSQL
    table_map = {
        'genre': 'content.genre',
        'film_work': 'content.film_work',
        'person': 'content.person',
        'genre_film_work': 'content.genre_film_work',
        'person_film_work': 'content.person_film_work'
    }
    
    # Проверка количества записей в каждой таблице
    print("Проверка количества записей...")
    for sqlite_table, pg_table in table_map.items():
        # Получаем количество записей из SQLite
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_table}")
        sqlite_count = sqlite_cursor.fetchone()[0]
        
        # Получаем количество записей из PostgreSQL
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"SELECT COUNT(*) FROM {pg_table}")
        pg_count = pg_cursor.fetchone()[0]
        
        print(f"Таблица {sqlite_table}: SQLite={sqlite_count}, PostgreSQL={pg_count}")
        assert sqlite_count == pg_count, (
            f"Несоответствие количества записей в таблице {sqlite_table}: "
            f"SQLite={sqlite_count}, PostgreSQL={pg_count}"
        )
    
    print("✓ Количество записей во всех таблицах совпадает\n")
    
    # Проверка содержимого записей для каждой таблицы с использованием батчей
    print("Проверка содержимого записей батчами...")
    
    for sqlite_table, pg_table in table_map.items():
        print(f"Проверка таблицы: {sqlite_table}")
        
        # Получаем общее количество записей для прогресса
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_table}")
        total_records = sqlite_cursor.fetchone()[0]
        
        # Получаем структуру колонок (набором, а не списком для независимости от порядка)
        sqlite_cursor.execute(f"PRAGMA table_info({sqlite_table})")
        sqlite_columns_set = {row[1] for row in sqlite_cursor.fetchall()}
        
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'content' 
            AND table_name = '{pg_table.split('.')[1]}'
        """)
        pg_columns_set = {row[0] for row in pg_cursor.fetchall()}
        
        # Проверяем, что наборы колонок совпадают
        assert sqlite_columns_set == pg_columns_set, (
            f"Несоответствие колонок в таблице {sqlite_table}: "
            f"SQLite={sorted(sqlite_columns_set)}, PostgreSQL={sorted(pg_columns_set)}"
        )
        
        # Получаем упорядоченные списки колонок для корректного сопоставления данных
        sqlite_cursor.execute(f"PRAGMA table_info({sqlite_table})")
        sqlite_columns_ordered = [row[1] for row in sqlite_cursor.fetchall()]
        
        pg_cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'content' 
            AND table_name = '{pg_table.split('.')[1]}'
            ORDER BY ordinal_position
        """)
        pg_columns_ordered = [row[0] for row in pg_cursor.fetchall()]
        
        # Проверяем данные батчами
        offset = 0
        batch_number = 1
        
        while True:
            # Получаем батч из SQLite
            sqlite_cursor.execute(f"""
                SELECT * FROM {sqlite_table} 
                ORDER BY id 
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """)
            sqlite_batch = sqlite_cursor.fetchall()
            
            if not sqlite_batch:
                break
            
            # Получаем соответствующий батч из PostgreSQL
            pg_cursor.execute(f"""
                SELECT * FROM {pg_table} 
                ORDER BY id 
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """)
            pg_batch = pg_cursor.fetchall()
            
            # Проверяем, что батчи имеют одинаковый размер
            assert len(sqlite_batch) == len(pg_batch), (
                f"Несоответствие размера батча #{batch_number} в таблице {sqlite_table}: "
                f"SQLite={len(sqlite_batch)}, PostgreSQL={len(pg_batch)}"
            )
            
            # Сравниваем каждую запись в батче
            for i, (sqlite_row, pg_row) in enumerate(zip(sqlite_batch, pg_batch)):
                record_number = offset + i + 1
                sqlite_dict = dict(zip(sqlite_columns_ordered, sqlite_row))
                pg_dict = dict(zip(pg_columns_ordered, pg_row))
                
                # Для каждой колонки проверяем соответствие значений
                for col in sqlite_columns_set:
                    sqlite_value = sqlite_dict[col]
                    pg_value = pg_dict[col]
                    
                    # Особенная обработка для временных меток
                    if col in ['created_at', 'updated_at'] and sqlite_value is not None:
                        # Преобразуем в строки и сравниваем только значимые части
                        sqlite_str = str(sqlite_value)[:19]  # Берем только дату и время без микросекунд
                        pg_str = str(pg_value)[:19]
                        assert sqlite_str == pg_str, (
                            f"Несоответствие в таблице {sqlite_table}, "
                            f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                            f"SQLite={sqlite_str}, PostgreSQL={pg_str}"
                        )
                    elif col == 'creation_date' and sqlite_value is not None:
                        # Для дат сравниваем строковое представление
                        sqlite_str = str(sqlite_value)
                        pg_str = str(pg_value)
                        assert sqlite_str == pg_str, (
                            f"Несоответствие в таблице {sqlite_table}, "
                            f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                            f"SQLite={sqlite_str}, PostgreSQL={pg_str}"
                        )
                    elif col in ['id', 'film_work_id', 'genre_id', 'person_id'] and sqlite_value is not None:
                        # Для UUID сравниваем строковые представления (приводим к нижнему регистру)
                        sqlite_str = str(sqlite_value).lower()
                        pg_str = str(pg_value).lower()
                        assert sqlite_str == pg_str, (
                            f"Несоответствие в таблице {sqlite_table}, "
                            f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                            f"SQLite={sqlite_str}, PostgreSQL={pg_str}"
                        )
                    else:
                        # Для остальных колонок сравниваем значения
                        # Особенная обработка для None значений
                        if sqlite_value is None:
                            assert pg_value is None, (
                                f"Несоответствие в таблице {sqlite_table}, "
                                f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                                f"SQLite=None, PostgreSQL={pg_value}"
                            )
                        elif pg_value is None:
                            assert sqlite_value is None, (
                                f"Несоответствие в таблице {sqlite_table}, "
                                f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                                f"SQLite={sqlite_value}, PostgreSQL=None"
                            )
                        else:
                            # Для числовых значений сравниваем с допуском
                            if isinstance(sqlite_value, (int, float)) and isinstance(pg_value, (int, float)):
                                assert abs(sqlite_value - pg_value) < 0.0001, (
                                    f"Несоответствие в таблице {sqlite_table}, "
                                    f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                                    f"SQLite={sqlite_value}, PostgreSQL={pg_value}"
                                )
                            else:
                                assert sqlite_value == pg_value, (
                                    f"Несоответствие в таблице {sqlite_table}, "
                                    f"батч #{batch_number}, запись #{record_number}, колонка {col}: "
                                    f"SQLite={sqlite_value}, PostgreSQL={pg_value}"
                                )
            
            print(f"  Батч #{batch_number} ({len(sqlite_batch)} записей) проверен успешно")
            
            offset += BATCH_SIZE
            batch_number += 1
        
        print(f"✓ Таблица {sqlite_table} прошла проверку ({total_records} записей)")
    
    print("\n✓ Все проверки пройдены успешно! Миграция данных завершена корректно.")

def load_from_sqlite(connection: sqlite3.Connection, pg_conn: psycopg2.extensions.connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    # Определяем соответствие таблиц и классов
    table_class_map = {
        'genre': Genre,
        'film_work': FilmWork,
        'person': Person,
        'genre_film_work': GenreFilmWork,
        'person_film_work': PersonFilmWork
    }

    # Загружаем данные из каждой таблицы
    for table_name, data_class in table_class_map.items():
        print(f'Загружаем данные из таблицы: {table_name}')
        
        try:
            data_generator = sqlite_loader.load_table_data(table_name, data_class)
            postgres_saver.save_all_data(data_generator)
            print(f'Таблица {table_name} успешно перенесена\n')
        except Exception as e:
            print(f'Ошибка при переносе таблицы {table_name}: {e}')
            continue

if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
        verify_data_migration(sqlite_conn, pg_conn)