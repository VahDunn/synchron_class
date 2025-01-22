import collections

from sqlalchemy import create_engine, MetaData, Table, and_, inspect, Column
from sqlalchemy.sql import select
from typing import List, Dict, AnyStr, Any
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



    def get_table_differences(self, table_name: str) -> Dict[str, List[Any]]:
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
        Синхронизация данных таблицы с созданием новых записей при различиях

        :param table_name: Имя таблицы для синхронизации
        """
        try:
            source_table = Table(table_name, self.source_metadata, autoload_with=self.source_engine)
            inspector = inspect(self.target_engine)
            if not inspector.has_table(table_name):
                metadata_target = MetaData()
                target_table = Table(table_name, metadata_target)

                for column in source_table.columns:
                    new_column = Column(
                        column.name,
                        column.type,
                        primary_key=column.primary_key,
                        nullable=column.nullable,
                        unique=column.unique,
                        index=column.index,
                        default=column.default,
                        server_default=column.server_default
                    )
                    target_table.append_column(new_column)
                metadata_target.create_all(self.target_engine)

            target_table = Table(table_name, self.target_metadata, autoload_with=self.target_engine)

            with self.source_engine.connect() as source_conn:
                source_data = source_conn.execute(select(source_table)).fetchall()

            primary_key_columns = [key.name for key in source_table.primary_key]
            all_columns = [col.name for col in source_table.columns]

            with self.target_engine.begin() as target_conn:
                for source_row in source_data:
                    primary_key_condition = and_(*[
                        getattr(target_table.c, col) == getattr(source_row, col)
                        for col in primary_key_columns
                    ])

                    existing_record = target_conn.execute(
                        select(target_table).where(primary_key_condition)
                    ).first()

                    if existing_record:
                        records_match = all(
                            getattr(source_row, col) == getattr(existing_record, col)
                            for col in all_columns
                        )

                        if not records_match:
                            new_record = {
                                col: getattr(source_row, col)
                                for col in all_columns
                            }
                            target_conn.execute(target_table.insert().values(new_record))
                            LOG.info(
                                f"Найдены различия в записи с ключом {primary_key_condition}. "
                                f"Создана новая запись."
                            )
                    else:
                        new_record = {
                            col: getattr(source_row, col)
                            for col in all_columns
                        }
                        target_conn.execute(target_table.insert().values(new_record))
                        LOG.info(
                            f"Добавлена новая запись с ключом {primary_key_condition}"
                        )

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


s_db_url = "postgresql://ilyas_apunov:pampampam@localhost:5432/postgres"
t_db_url = "postgresql://ilyas_apunov:pumpumpum@localhost:5434/postgres"


synchronizer = DatabaseSynchronizer(s_db_url, t_db_url)


tables_to_sync = ['users']
synchronizer.synchronize_database(tables_to_sync)
