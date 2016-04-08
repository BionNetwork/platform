# coding: utf-8
from django.conf import settings

from core.models import ConnectionChoices

from etl.services.db import mysql, postgresql
from etl.services.source import DatasourceApi


class DatabaseService(DatasourceApi):
    """Сервис для источников данных"""


    @staticmethod
    def factory(conn_type, connection):
        """
        фабрика для инстанса бд

        Returns:
            etl.services.db.interfaces.Database
        """

        if conn_type == ConnectionChoices.POSTGRESQL:
            return postgresql.Postgresql(connection)
        elif conn_type == ConnectionChoices.MYSQL:
            return mysql.Mysql(connection)
        elif conn_type == ConnectionChoices.MS_SQL:
            import mssql
            return mssql.MsSql(connection)
        elif conn_type == ConnectionChoices.ORACLE:
            import oracle
            return oracle.Oracle(connection)
        else:
            raise ValueError("Неизвестный тип подключения!")

    def get_source_instance(self):
        """
        инстанс бд соурса

        Returns:
            etl.services.db.interfaces.Database
        """
        connection = self.get_source_data()
        conn_type = self.source.conn_type

        return self.factory(conn_type, connection)

    def get_source_data(self):
        """
        Возвращает список модели источника данных
        Returns:
            dict: словарь с информацией подключения
        """
        return {'db': self.source.db, 'host': self.source.host,
                'port': self.source.port, 'login': self.source.login,
                'password': self.source.password}

    def get_tables(self):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        return self.datasource.get_tables(self.source)

    def get_columns_info(self, tables):
        """
            Получение списка колонок
        Args:
            source(core.models.Datasource): источник
            tables(list): список таблиц

        Returns:
            list: список колонок, ограничений и индекксов таблицы
        """
        return self.datasource.get_columns_info(self.source, tables)

    def fetch_tables_columns(self, tables):
        # возвращает список колонок таблиц

        return self.datasource.get_columns(self.source, tables)

    def get_stats_info(self, tables):
        """
        Получение списка размера и кол-ва строк таблиц
        :param source: Datasource
        :param tables:
        :return:
        """
        return self.datasource.get_statistic(self.source, tables)

    def get_date_intervals(self, cols_info):
        """
        Получение данных из
        Args:

        Returns:

        """
        return self.datasource.get_intervals(self.source, cols_info)

    def get_rows_query(self, cols, structure):
        """
        Получение запроса выбранных колонок из указанных таблиц выбранного источника
        :param source: Datasource
        :return:
        """
        return self.datasource.get_rows_query(cols, structure)

    def get_rows(self, source, cols, structure):
        """
        Получение значений выбранных колонок из указанных таблиц и выбранного источника
        :type structure: dict
        :param source: Datasource
        :param cols: list
        :return:
        """
        return self.datasource.get_rows(cols, structure)

    def get_generated_joins(self, source, structure):
        """
        связи таблиц
        :param source: Datasource
        :param structure: dict
        :return: str
        """
        return self.datasource.generate_join(structure)

    @classmethod
    def get_connection(cls, source):
        """
        Получение соединения источника
        :type source: Datasource
        """
        conn_info = source.get_connection_dict()
        return cls.get_connection_by_dict(conn_info)

    @classmethod
    def get_connection_by_dict(cls, con_type, conn_info):
        """
        Получение соединения источника
        :type conn_info: dict
        """
        instance = cls.factory(con_type, conn_info)

        conn_info['port'] = int(conn_info['port'])

        conn = instance.get_connection(conn_info)
        if conn is None:
            raise ValueError("Сбой при подключении!")
        return conn

    def processing_records(self, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов соурса
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        return self.datasource.processing_records(col_records, index_records, const_records)

    @classmethod
    def get_local_connection_dict(cls):
        """
        возвращает словарь параметров подключения
        к локальному хранилищу данных(Postgresql)
        :rtype : dict
        :return:
        """
        db_info = settings.DATABASES['default']
        return {
            'host': db_info['HOST'], 'db': db_info['NAME'],
            'login': db_info['USER'], 'password': db_info['PASSWORD'],
            'port': str(db_info['PORT']),
            # жестко постгрес
            'conn_type': ConnectionChoices.POSTGRESQL,
        }

    @classmethod
    def get_local_instance(cls):
        """
        возвращает инстанс локального хранилища данных(Postgresql)
        :rtype : object Postgresql()
        :return:
        """
        local_data = cls.get_local_connection_dict()
        instance = cls.factory(**local_data)
        return instance

    # fixme: не использутеся
    def get_separator(self):
        return self.datasource.get_separator()

    def get_structure_rows_number(self, structure, cols):
        """
        возвращает примерное кол-во строк в запросе селекта для планирования
        :param source:
        :param structure:
        :param cols:
        :return:
        """
        return self.datasource.get_structure_rows_number(structure, cols)

    def get_remote_table_create_query(self):
        """
        возвращает запрос на создание таблицы в БД клиента
        """
        return self.datasource.remote_table_create_query()

    def get_remote_triggers_create_query(self):
        """
        возвращает запрос на создание григгеров в БД клиента
        """
        return self.datasource.remote_triggers_create_query()

    def get_fetchall_result(self, connection, query, *args, **kwargs):
        """
        возвращает результат fetchall преобразованного запроса с аргументами
        """
        return self.datasource.get_fetchall_result(connection, query, *args, **kwargs)

    def get_processed_for_triggers(self, source, columns):
        """
        Получает инфу о колонках, возвращает преобразованную инфу
        для создания триггеров
        """
        return self.datasource.get_processed_for_triggers(columns)

    def get_processed_indexes(self, source, indexes):
        """
        Получает инфу об индексах, возвращает преобразованную инфу
        для создания триггеров
        """
        return self.datasource.get_processed_indexes(indexes)

    def get_required_indexes(self, source):
        return self.datasource.get_required_indexes()


class LocalDatabaseService(object):

    def __init__(self):
        self.datasource = self.get_local_connection()

    @staticmethod
    def get_local_connection():
        db_info = settings.DATABASES['default']
        params = {
            'host': db_info['HOST'], 'db': db_info['NAME'],
            'login': db_info['USER'], 'password': db_info['PASSWORD'],
            'port': str(db_info['PORT']),
        }

        return postgresql.Postgresql(params)

    def reload_datasource_trigger_query(self, params):
        """
        запрос на создание триггеров в БД локально для размерностей и мер
        """
        return self.datasource.reload_datasource_trigger_query(params)

    def get_date_table_names(self, col_type):
        """
        Получене запроса на создание таблицы даты
        """
        return self.datasource.get_date_table_names(col_type)

    def get_table_create_col_names(self, fields, ref_key):
        return self.datasource.get_table_create_col_names(fields, ref_key)

    def cdc_key_delete_query(self, table_name):
        return self.datasource.cdc_key_delete_query(table_name)

    def get_table_create_query(self, table_name, cols_str):
        """
        Получение запроса на создание новой таблицы
        для локального хранилища данных
        :param table_name: str
        :param cols_str: str
        :return: str
        """
        return self.datasource.local_table_create_query(table_name, cols_str)

    def check_table_exists_query(self, local_instance, table, db):
        """
        Проверка на существование таблицы
        """
        return self.datasource.check_table_exists_query(table, db)

    def get_page_select_query(self, table_name, cols):
        """
        Формирование строки запроса на получение данных (с дальнейшей пагинацией)

        Args:
            table_name(unicode): Название таблицы
            cols(list): Список получаемых колонок
        """
        return self.datasource.get_page_select_query(table_name, cols)

    def get_select_dates_query(self, date_table):
        """
        Получение всех дат из таблицы дат
        """
        return self.datasource.get_select_dates_query(date_table)

    def get_table_insert_query(self, table_name, cols_num):
        """
        Запрос на добавление в новую таблицу локал хранилища

        Args:
            table_name(str): Название таблиц
            cols_num(int): Число столбцов
        Returns:
            str: Строка на выполнение
        """
        return self.datasource.local_table_insert_query(table_name, cols_num)
