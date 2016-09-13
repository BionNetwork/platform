# coding: utf-8


import calendar
import math
from contextlib import closing
from datetime import timedelta, datetime

from django.conf import settings

from core.models import (ConnectionChoices, DatasourcesJournal, Datasource)
from etl.constants import DATE_TABLE_COLS_LEN
from etl.services.datasource.db import mysql, postgresql
from etl.services.datasource.source import DatasourceApi


class DatabaseService(DatasourceApi):

    """Сервис для источников данных"""

    def execute(self, query, args=None, many=False):
        with self.datasource.connection:
            with closing(self.datasource.connection.cursor()) as cursor:
                if not many:
                    cursor.execute(query, args)
                else:
                    cursor.executemany(query, args)

    def get_source_instance(self):
        """
        фабрика для инстанса бд

        Returns:
            etl.services.db.interfaces.Database
        """
        source = self.source
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.POSTGRESQL:
            return postgresql.Postgresql(source)
        elif conn_type == ConnectionChoices.MYSQL:
            return mysql.Mysql(source)
        elif conn_type == ConnectionChoices.MS_SQL:
            from . import mssql
            return mssql.MsSql(source)
        elif conn_type == ConnectionChoices.ORACLE:
            from . import oracle
            return oracle.Oracle(source)
        else:
            raise ValueError("Неизвестный тип подключения!")

    def get_tables(self):
        """
        Возвращает таблицы источника
        Фильтрация таблиц по факту созданных раннее триггеров
        Returns:
            list: список таблиц
        """
        source = self.source
        tables = self.datasource.get_tables()

        trigger_tables = DatasourcesJournal.objects.filter(
            trigger__datasource=source).values_list('name', flat=True)

        # фильтруем, не показываем таблицы триггеров
        tables = [x for x in tables if x['name'] not in trigger_tables]
        return tables

    def get_columns_info(self, tables, indents):
        """
            Получение полной информации о колонках таблиц
        Args:
            tables(list): список таблиц

        Returns:
            list: список колонок, индексов, FK-ограничений,
            статистики, интервалов дат таблиц
        """
        instance = self.datasource

        col_records, index_records, const_records = (
                instance.get_columns_info(tables))
        statistics = instance.get_statistic(tables)
        date_intervals = instance.get_intervals(col_records)

        columns, indexes, foreigns = instance.processing_records(
                col_records, index_records, const_records)

        return columns, indexes, foreigns, statistics, date_intervals

    def fetch_tables_columns(self, tables, indent):
        # возвращает список колонок таблиц

        return self.datasource.get_columns(self.source, tables)

    def get_date_intervals(self, cols_info):
        """
        Получение данных из
        Args:

        Returns:

        """
        return self.datasource.get_intervals(self.source, cols_info)

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

    def get_structure_rows_number(self, structure, cols):
        """
        возвращает примерное кол-во строк в запросе селекта для планирования
        :param source:
        :param structure:
        :param cols:
        :return:
        """
        return self.datasource.get_structure_rows_number(structure, cols)

    def get_fetchall_result(self, connection, query, *args, **kwargs):
        """
        возвращает результат fetchall преобразованного запроса с аргументами
        """
        return self.datasource.get_fetchall_result(connection, query, *args, **kwargs)

    def get_source_table_rows(self, table_name, **kwargs):
        """
        Данные по одной таблице источника
        """
        limit = kwargs.get('limit')
        offset = kwargs.get('offset')

        query = self.datasource.get_table_data(table_name, limit, offset)
        return self.get_fetchall_result(
            self.datasource.connection, query)

    def get_source_rows(self, structure, cols, limit=None, offset=None):
        """
        Получение постраничных данных из базы пользователя
        """

        query = self.datasource.get_rows_query(cols, structure)
        return self.get_fetchall_result(
            self.datasource.connection, query, limit=limit, offset=offset)


class LocalDatabaseService(object):
    """
    Сервис для работы с локальной базой данных
    """

    def __init__(self):
        self.datasource = self.get_local_connection()

    @staticmethod
    def get_local_connection():
        """
        Получение экземпляра класса `postgresql.Postgresql`
        """
        db_info = settings.DATABASES['default']
        params = {
            'host': db_info['HOST'], 'db': db_info['NAME'],
            'login': db_info['USER'], 'password': db_info['PASSWORD'],
            'port': str(db_info['PORT']),
        }
        source = Datasource()
        source.set_from_dict(**params)
        return postgresql.Postgresql(source)

    def execute(self, query, args=None, many=False):
        """
        Выполнение запросов

        Args:
            query(str): Строка запроса
            args(list): Список аргументов запроса
            many(bool): Флаг множественного запроса
        """
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

    def create_fdw_server(self, name, source_params):
        """
        Создание mongodb-расширения
        с соответсвущим сервером и картой пользователя
        """
        query = self.datasource.fdw_server_create_query(name, source_params)
        self.execute(query)

    def create_foreign_table(self, name, server_name, options, cols):
        """
        Создание удаленную таблицу к Mongodb
        """
        query = self.datasource.foreign_table_create_query(
            name, server_name, options, cols)
        self.execute(query)

    def create_foreign_view(self, sub_tree):
        """
        Создание представления для удаленной таблицы (Foreign Table)
        Args:
            sub_tree(dict): Поддерево

        Returns: FIXME: Описать
        """
        query = self.datasource.create_foreign_view_query(sub_tree)
        self.execute(query)

    def create_materialized_view(self, dimensions_mv, measures_mv, relations):
        """"
        Создание материализованного представления для мер и разменостей
        """
        dim_mv_query, meas_mv_query = self.datasource.create_materialized_view_query(
            dimensions_mv, measures_mv, relations)

        self.execute(dim_mv_query)
        self.execute(meas_mv_query)

    def create_sttm_select_query(self, file_path, file_name, relations):
        """
        Создание запроса на получение данных для конечной таблицы
        Args:
            file_path(str): Путь к директории
            file_name(str): Название файла
            relations:

        Returns:
            str: Строка запроса

        """
        select_query = self.datasource.create_sttm_select_query(relations)

        copy_query = """COPY ({select_query}) TO '{file_path}{file_name}.csv' With CSV;""".format(
            select_query=select_query, file_path=file_path, file_name=file_name)
        self.execute(copy_query)

    def create_posgres_warehouse(self, warehouse, relations):

        select_query = self.datasource.create_sttm_select_query(relations)

        create_query = """CREATE MATERIALIZED VIEW {warehouse} AS {select_query}""".format(
            warehouse=warehouse, select_query=select_query)
        self.execute(create_query)

    def check_table_exists_query(self, local_instance, table, db):
        """
        Проверка на существование таблицы
        """
        return self.datasource.check_table_exists_query(table, db)

    def create_time_table(self, time_table_name):
        from etl.services.queue.base import DTCN
        cols = ', '.join(self.datasource.get_date_table_names(DTCN.types))
        query = self.datasource.local_table_create_query(time_table_name, cols)
        self.execute(query)

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

    def page_select(self, table_name, col_names, limit, offset):
        query = self.datasource.get_page_select_query(table_name, col_names)
        return self.fetchall(query, limit=limit, offset=offset)

    def delete(self, table_name, records):
        query = self.datasource.cdc_key_delete_query(table_name)
        self.execute(query, records)

    def create_date_tables(self, time_table_name, sub_trees, is_update):
        """
        Создание таблиц дат
        """
        intervals = []
        for sub_tree in sub_trees:
            intervals.extend(sub_tree['date_intervals'])

        date_tables = {}

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
        max_id = max(exists_dates.values())
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
            # +1 потому start_date тоже кладем
            records, date_ids = self.prepare_dates_records(
                start_date, delta.days + 1, 0)
            date_tables.update(date_ids)
            self.local_insert(time_table_name, DATE_TABLE_COLS_LEN, records)

        return date_ids

    @staticmethod
    def prepare_dates_records(start_date, days_count, max_id):
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

    def create_schema(self, schema_name):
        """
        Создание схемы в локальной базе по id карточки
        Args:
            schema_name(str): Название схемы
        """

        query = self.datasource.create_schema_query(schema_name)
        self.execute(query)
