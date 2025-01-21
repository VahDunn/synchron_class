from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select
from typing import List, Dict
from logger import LOG


class DatabaseSynchronizer:
    def __init__(self, source_db_url: str, target_db_url: str):
        """
        :param source_db_url: URL исходной (тестовой) базы данных
        :param target_db_url: URL целевой (боевой) базы данных
        """
        self.source_engine = create_engine(source_db_url)
        self.target_engine = create_engine(target_db_url)

        self.source_metadata = MetaData()
        self.target_metadata = MetaData()



    def get_table_differences(self, table_name: str) -> Dict:
        """
        Получение различий между таблицами в обеих БД
        :param table_name: Имя таблицы для сравнения
        :return: Словарь с различиями
        """
        source_table = Table(table_name, self.source_metadata, autoload_with=self.source_engine)
        target_table = Table(table_name, self.target_metadata, autoload_with=self.target_engine)

        differences = {
            'new_columns': [],
            'modified_columns': [],
            'removed_columns': []
        }

        source_columns = {c.name: c for c in source_table.columns}
        target_columns = {c.name: c for c in target_table.columns}

        for col_name, source_col in source_columns.items():
            if col_name not in target_columns:
                differences['new_columns'].append(col_name)
            elif str(source_col.type) != str(target_columns[col_name].type):
                differences['modified_columns'].append(col_name)

        for col_name in target_columns:
            if col_name not in source_columns:
                differences['removed_columns'].append(col_name)

        return differences

    def synchronize_table(self, table_name: str) -> None:
        """
        Синхронизация данных конкретной таблицы

        :param table_name: Имя таблицы для синхронизации
        """
        try:
            source_table = Table(table_name, self.source_metadata, autoload_with=self.source_engine)
            target_table = Table(table_name, self.target_metadata, autoload_with=self.target_engine)

            # Получаем данные из исходной таблицы
            with self.source_engine.connect() as source_conn:
                source_data = source_conn.execute(select(source_table)).fetchall()

            # Получаем первичный ключ таблицы
            primary_key_columns = [key.name for key in source_table.primary_key]

            # Обновляем данные в целевой таблице
            with self.target_engine.connect() as target_conn:
                for row in source_data:
                    # Создаем условие для поиска записи по первичному ключу
                    primary_key_condition = {
                        col: getattr(row, col) for col in primary_key_columns
                    }

                    # Проверяем существование записи
                    existing_record = target_conn.execute(
                        select(target_table).filter_by(**primary_key_condition)
                    ).first()

                    if existing_record:
                        # Обновляем существующую запись
                        update_values = {
                            col.name: getattr(row, col.name)
                            for col in source_table.columns
                            if col.name in [c.name for c in target_table.columns]
                        }
                        target_conn.execute(
                            target_table.update()
                            .filter_by(**primary_key_condition)
                            .values(**update_values)
                        )
                    else:
                        # Добавляем новую запись
                        insert_values = {
                            col.name: getattr(row, col.name)
                            for col in source_table.columns
                            if col.name in [c.name for c in target_table.columns]
                        }
                        target_conn.execute(target_table.insert().values(**insert_values))

                target_conn.commit()

            LOG.info(f"Таблица {table_name} успешно синхронизирована")

        except Exception as e:
            LOG.error(f"Ошибка при синхронизации таблицы {table_name}: {str(e)}")
            raise

    def synchronize_database(self, tables: List[str] = None) -> None:
        """
        Синхронизация всех указанных таблиц

        :param tables: Список таблиц для синхронизации. Если None, синхронизируются все таблицы
        """
        if tables is None:
            # Получаем список всех таблиц из исходной БД
            self.source_metadata.reflect(bind=self.source_engine)
            tables = list(self.source_metadata.tables.keys())

        for table_name in tables:
            LOG.info(f"Начало синхронизации таблицы {table_name}")
            differences = self.get_table_differences(table_name)

            if any(differences.values()):
                LOG.warning(
                    f"Обнаружены различия в структуре таблицы {table_name}: {differences}"
                )

            self.synchronize_table(table_name)


source_db_url = "postgresql://postgres:pampampam@localhost:5432/postgres"
target_db_url = "postgresql://postgres:pumpumpum@localhost:5434/postgres"

# Создание экземпляра синхронизатора
synchronizer = DatabaseSynchronizer(source_db_url, target_db_url)

# Синхронизация конкретных таблиц
tables_to_sync = ['users']
synchronizer.synchronize_database(tables_to_sync)

# Или синхронизация всей базы данных
# synchronizer.synchronize_database()