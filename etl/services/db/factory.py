# coding: utf-8
import calendar
from contextlib import closing

import math

from datetime import timedelta, datetime
from django.conf import settings

from core.models import (ConnectionChoices, DatasourcesJournal)
from etl.constants import DATE_TABLE_COLS_LEN
from etl.services.db import mysql, postgresql
from etl.services.queue.base import DTCN
from etl.services.source import DatasourceApi


class DatabaseService(DatasourceApi):
    """Сервис для источников данных"""

    def __init__(self, source):
        super(DatabaseService, self).__init__(source=source)
        self.connection = self.get_connection(source)

    def execute(self, query, args=None, many=False):
        with self.connection:
            with closing(self.connection.cursor()) as cursor:
                if not many:
                    cursor.execute(query, args)
                else:
                    cursor.executemany(query, args)

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
        Фильтрация таблиц по факту созданных раннее триггеров
        Returns:
            list: список таблиц
        """
        source = self.source
        tables = self.datasource.get_tables(source)

        trigger_tables = DatasourcesJournal.objects.filter(
            trigger__datasource=source).values_list('name', flat=True)

        # фильтруем, не показываем таблицы триггеров
        tables = filter(lambda x: x['name'] not in trigger_tables, tables)
        return tables

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

    # FIXME: Удалить
    def get_rows_query(self, cols, structure):
        """
        Получение запроса выбранных колонок из указанных таблиц выбранного источника
        :param source: Datasource
        :return:
        """
        return self.datasource.get_rows_query(cols, structure)

    def get_rows(self, cols, structure):
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
    def get_connection_by_dict(cls, conn_info):
        """
        Получение соединения источника
        :type conn_info: dict
        """
        conn_type = conn_info['conn_type']
        del conn_info['conn_type']
        instance = cls.factory(conn_type, conn_info)

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

    def get_source_rows(self, structure, cols, limit=None, offset=None):
        """
        Получение постраничных данных из базы пользователя
        """

        query = self.datasource.get_rows_query(cols, structure)
        return self.get_fetchall_result(self.connection, query, limit=limit, offset=offset)


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

    def execute(self, query, args=None, many=False):
        with self.datasource.connection:
            with closing(self.datasource.connection.cursor()) as cursor:
                if not many:
                    cursor.execute(query, args)
                else:
                    cursor.executemany(query, args)

    def fetchall(self, query, **kwargs):
        with self.datasource.connection:
            with closing(self.datasource.connection.cursor()) as cursor:
                cursor.execute(query, kwargs)
                return cursor.fetchall()

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

    # FIXME удалить
    def get_table_create_col_names(self, fields, time_table_name):
        return self.datasource.get_table_create_col_names(fields, time_table_name)

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

    # FIXME Удалить
    def get_select_dates_query(self, date_table):
        """
        Получение всех дат из таблицы дат
        """
        return self.datasource.get_select_dates_query(date_table)

    # FIXME Удалить
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

    def create_time_table(self, time_table_name):
        cols = ', '.join(self.datasource.get_date_table_names(DTCN.types))
        query = self.datasource.local_table_create_query(time_table_name, cols)
        self.execute(query, many=True)

    def create_sttm_table(self, sttm_table_name, time_table_name, processed_cols):
        col_names = self.datasource.get_table_create_col_names(
            processed_cols, time_table_name)
        self.execute(self.datasource.local_table_create_query(
            sttm_table_name, ', '.join(col_names)))

    def local_insert(self, table_name, cols_num, data):
        query = self.datasource.local_table_insert_query(
            table_name, cols_num)
        self.execute(query, data, many=True)

    def date_select(self, table_name):
        query = self.datasource.get_select_dates_query(table_name)
        return self.fetchall(query)

    def create_date_tables(self, time_table_name, meta_info, is_update):
        """
        Создание таблиц дат
        """

        date_tables = {}

        date_intervals_info = [(t_name, t_info['date_intervals'])
                               for (t_name, t_info) in meta_info.iteritems()]

        # список всех интервалов
        intervals = reduce(
            lambda x, y: x[1]+y[1], date_intervals_info, ['', []])

        if not intervals:
            return date_tables

        # как минимум создадим таблицу с 1 записью c id = 0
        # для пустых значений, если новая закачка
        if not is_update:
            self.create_time_table(time_table_name)

            none_row = {'0': 0}
            none_row.update({str(x): None for x in range(1, DATE_TABLE_COLS_LEN)})
            self.local_insert(time_table_name, DATE_TABLE_COLS_LEN, [none_row])

        # если в таблице в колонке все значения дат пустые, то
        # startDate и endDate = None, отфильтруем
        not_none_intervals = [
            i for i in intervals if i['startDate'] is not None and
            i['endDate'] is not None
        ]

        if not not_none_intervals:
            return date_tables

        # если имеются численные интервалы
        # новые границы дат, пришедшие с пользователя
        start_date = min(
            [datetime.strptime(interval['startDate'], "%d.%m.%Y").date()
             for interval in not_none_intervals])
        end_date = max(
            [datetime.strptime(interval['endDate'], "%d.%m.%Y").date()
             for interval in not_none_intervals])

        delta = end_date - start_date

        # первое заполнение таблицы дат
        if not is_update:
            # +1 потому start_date тоже суем
            records, date_ids = self.prepare_dates_records(
                start_date, delta.days + 1, 0)
            date_tables.update(date_ids)
            self.local_insert(time_table_name, DATE_TABLE_COLS_LEN, records)
            return date_tables

        # update таблицы дат
        db_exists_dates = self.date_select(time_table_name)

        exists_dates = {x[0].strftime("%d.%m.%Y"): x[1] for x in db_exists_dates}

        date_tables = exists_dates

        # если в таблице дат имелся только 1 запись с id = 0
        if not exists_dates:
            # +1 потому start_date тоже суем
            records, date_ids = self.prepare_dates_records(
                start_date, delta.days + 1, 0)
            date_tables.update(date_ids)
            self.local_insert(time_table_name, DATE_TABLE_COLS_LEN, records)
            return date_tables

        # если в таблице дат имелись записи кроме id = 0
        max_id = max(exists_dates.itervalues())
        dates_list = [x[0] for x in db_exists_dates]
        exist_min_date = min(dates_list)
        exist_max_date = max(dates_list)

        min_delta = (exist_min_date - start_date).days

        if min_delta > 0:
            records, date_ids = self.prepare_dates_records(
                start_date, min_delta, max_id)
            date_tables.update(date_ids)
            self.local_insert(
                time_table_name, DATE_TABLE_COLS_LEN, records)

            max_id += min_delta

        max_delta = (end_date - exist_max_date).days

        if max_delta > 0:
            records, date_ids = self.prepare_dates_records(
                exist_max_date+timedelta(days=1), max_delta, max_id)
            date_tables.update(date_ids)
            self.local_insert(
                time_table_name, DATE_TABLE_COLS_LEN, records)

            max_id += max_delta

        # если в таблице дат имелся только 1 запись с id = 0
        else:
            # +1 потому start_date тоже суем
            records, date_ids = self.prepare_dates_records(
                start_date, delta.days + 1, 0)
            date_tables.update(date_ids)
            self.local_insert(time_table_name, DATE_TABLE_COLS_LEN, records)

    def prepare_dates_records(self, start_date, days_count, max_id):
        # список рекордов для таблицы дат
        date_ids = {}
        records = []
        for day_delta in range(days_count):
            current_day = start_date + timedelta(days=day_delta)
            current_day_str = current_day.isoformat()
            month = current_day.month
            records.append({
                    '0': max_id + day_delta + 1,
                    '1': current_day_str,
                    '2': calendar.day_name[current_day.weekday()],
                    '3': current_day.year,
                    '4': month,
                    '5': current_day.strftime('%B'),
                    '6': current_day.day,
                    '7': current_day.isocalendar()[1],
                    '8': int(math.ceil(float(month) / 3)),

                 })
            date_ids.update(
                {current_day.strftime("%d.%m.%Y"): max_id + day_delta + 1})

        return records, date_ids
