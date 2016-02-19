# coding: utf-8
from django.conf import settings

from core.models import ConnectionChoices
from etl.services.db import mysql, postgresql


class DatabaseService(object):
    """Сервис для источников данных"""

    @staticmethod
    def factory(**connection):
        """
        фабрика для инстанса бд

        Args:
            **connection(dict): словарь с информацией о подключении

        Returns:
            etl.services.db.interfaces.Database
        """
        conn_type = int(connection.get('conn_type', ''))
        del connection['conn_type']

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

    @classmethod
    def get_source_instance(cls, source):
        """
        инстанс бд соурса

        Args:
            source(core.models.Datasource): источник

        Returns:
            etl.services.db.interfaces.Database

        """
        data = cls.get_source_data(source)
        instance = cls.factory(**data)
        return instance

    @classmethod
    def get_tables(cls, source):
        """
        Возвращает таблицы источника
        Args:
            source(core.models.Datasource): источник

        Returns:
            list: список таблиц
        """
        instance = cls.get_source_instance(source)
        return instance.get_tables(source)

    @classmethod
    def get_source_data(cls, source):
        """
        Возвращает список модели источника данных
        Args:
            source(core.models.Datasource): источник

        Returns:
            dict: словарь с информацией подключения
        """
        return dict({'db': source.db, 'host': source.host, 'port': source.port, 'login': source.login,
                     'password': source.password, 'conn_type': source.conn_type})

    @classmethod
    def get_columns_info(cls, source, tables):
        """
            Получение списка колонок
        Args:
            source(core.models.Datasource): источник
            tables(list): список таблиц

        Returns:
            list: список колонок таблицы
        """
        instance = cls.get_source_instance(source)
        return instance.get_columns(source, tables)

    @classmethod
    def get_stats_info(cls, source, tables):
        """
        Получение списка размера и кол-ва строк таблиц
        :param source: Datasource
        :param tables:
        :return:
        """
        instance = cls.get_source_instance(source)
        return instance.get_statistic(source, tables)

    @classmethod
    def get_date_intervals(cls, source, cols_info):
        """
        Получение данных из
        Args:


        Returns:

        """
        instance = cls.get_source_instance(source)
        return instance.get_intervals(source, cols_info)


    @classmethod
    def get_rows_query(cls, source, cols, structure):
        """
        Получение запроса выбранных колонок из указанных таблиц выбранного источника
        :param source: Datasource
        :return:
        """
        instance = cls.get_source_instance(source)
        return instance.get_rows_query(cols, structure)

    @classmethod
    def get_rows(cls, source, cols, structure):
        """
        Получение значений выбранных колонок из указанных таблиц и выбранного источника
        :type structure: dict
        :param source: Datasource
        :param cols: list
        :return:
        """
        instance = cls.get_source_instance(source)
        return instance.get_rows(cols, structure)

    @classmethod
    def get_table_create_query(cls, table_name, cols_str):
        """
        Получение запроса на создание новой таблицы
        для локального хранилища данных
        :param table_name: str
        :param cols_str: str
        :return: str
        """
        local_instance = cls.get_local_instance()
        return local_instance.local_table_create_query(table_name, cols_str)

    @classmethod
    def check_table_exists_query(cls, local_instance, table, db):
        """
        Проверка на существование таблицы
        """
        return local_instance.check_table_exists_query(table, db)

    @classmethod
    def get_page_select_query(cls, table_name, cols):
        """
        Формирование строки запроса на получение данных (с дальнейшей пагинацией)

        Args:
            table_name(unicode): Название таблицы
            cols(list): Список получаемых колонок
        """
        local_instance = cls.get_local_instance()
        return local_instance.get_page_select_query(table_name, cols)

    @classmethod
    def get_table_insert_query(cls, table_name, cols_num):
        """
        Запрос на добавление в новую таблицу локал хранилища

        Args:
            table_name(str): Название таблиц
            cols_num(int): Число столбцов
        Returns:
            str: Строка на выполнение
        """
        local_instance = cls.get_local_instance()
        return local_instance.local_table_insert_query(table_name, cols_num)

    @classmethod
    def get_generated_joins(cls, source, structure):
        """
        связи таблиц
        :param source: Datasource
        :param structure: dict
        :return: str
        """
        instance = cls.get_source_instance(source)
        return instance.generate_join(structure)

    @classmethod
    def get_connection(cls, source):
        """
        Получение соединения источника
        :type source: Datasource
        """
        conn_info = source.get_connection_dict()
        return cls.get_connection_by_dict(conn_info)

    @classmethod
    def get_connection_by_dict(cls, conn_info):
        """
        Получение соединения источника
        :type conn_info: dict
        """
        instance = cls.factory(**conn_info)

        del conn_info['conn_type']
        conn_info['port'] = int(conn_info['port'])

        conn = instance.get_connection(conn_info)
        if conn is None:
            raise ValueError("Сбой при подключении!")
        return conn

    @classmethod
    def processing_records(cls, source, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов соурса
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        instance = cls.get_source_instance(source)
        return instance.processing_records(col_records, index_records, const_records)

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
    @classmethod
    def get_separator(cls, source):
        instance = cls.get_source_instance(source)
        return instance.get_separator()

    @classmethod
    def get_structure_rows_number(cls, source, structure, cols):
        """
        возвращает примерное кол-во строк в запросе селекта для планирования
        :param source:
        :param structure:
        :param cols:
        :return:
        """
        instance = cls.get_source_instance(source)
        return instance.get_structure_rows_number(structure, cols)

    @classmethod
    def get_remote_table_create_query(cls, source):
        """
        возвращает запрос на создание таблицы в БД клиента
        """
        instance = cls.get_source_instance(source)
        return instance.remote_table_create_query()

    @classmethod
    def get_remote_triggers_create_query(cls, source):
        """
        возвращает запрос на создание григгеров в БД клиента
        """
        instance = cls.get_source_instance(source)
        return instance.remote_triggers_create_query()

    @classmethod
    def reload_datasource_trigger_query(cls, params):
        """
        запрос на создание триггеров в БД локально для размерностей и мер
        """

        cls.get_local_instance().reload_datasource_trigger_query(params)

    @classmethod
    def get_date_table_names(cls, col_type):
        """
        Получене запроса на создание таблицы даты
        """
        return cls.get_local_instance().get_date_table_names(col_type)

    @classmethod
    def get_dim_table_names(cls, fields, ref_key):
        return cls.get_local_instance().get_dim_table_names(fields, ref_key)

    @classmethod
    def cdc_key_delete_query(cls, table_name):
        return cls.get_local_instance().cdc_key_delete_query(table_name)
