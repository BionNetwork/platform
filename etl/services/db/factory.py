# coding: utf-8


import calendar
from contextlib import closing
import math
from datetime import timedelta, datetime

from django.conf import settings
from django.db import transaction

from core.models import (ConnectionChoices, DatasourcesJournal, Datasource,
                         DatasourcesTrigger)
from etl.constants import DATE_TABLE_COLS_LEN
from etl.services.db import mysql, postgresql
from etl.services.source import DatasourceApi


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

    def create_triggers(self, tables_info):
        sep = self.datasource.get_separator()

        remote_table_create_query = self.datasource.remote_table_create_query()
        remote_triggers_create_query = self.datasource.remote_triggers_create_query()

        connection = self.datasource.connection
        cursor = connection.cursor()

        for table, columns in tables_info.items():

            table_name = '_etl_{0}'.format(table)
            tables_str = "('{0}')".format(table_name)

            cdc_cols_query = self.datasource.db_map.cdc_cols_query.format(
                tables_str, self.source.db, 'public')

            cursor.execute(cdc_cols_query)
            fetched_cols = cursor.fetchall()

            existing_cols = {k: v for (k, v) in fetched_cols}

            REQUIRED_INDEXES = self.datasource.get_required_indexes()

            required_indexes = {k.format(table_name): v
                                for k, v in REQUIRED_INDEXES.items()}

            for_triggers = self.datasource.get_processed_for_triggers(columns)
            cols_str = for_triggers['cols_str']

            # если таблица существует
            if existing_cols:
                # удаление primary key, если он есть
                primary_query = self.datasource.get_primary_key(
                    table_name, self.source.db)
                cursor.execute(primary_query)
                primary = cursor.fetchall()

                if primary:
                    primary_name = primary[0][0]
                    del_pr_query = self.datasource.delete_primary_query(
                        table_name, primary_name)
                    cursor.execute(del_pr_query)

                # добавление недостающих колонок, не учитывая cdc-колонки
                new_came_cols = [
                    {
                        'col_name': x['name'],
                        'col_type': x["type"],
                        'max_length': '({0})'.format(x['max_length'])
                            if x['max_length'] is not None else '',
                        'nullable': 'not null' if x['is_nullable'] == 'NO' else ''
                    }
                    for x in columns]

                diff_cols = [x for x in new_came_cols
                             if x['col_name'] not in existing_cols]

                add_col_q = self.datasource.db_map.add_column_query
                del_col_q = self.datasource.db_map.del_column_query

                for d_col in diff_cols:
                    cursor.execute(add_col_q.format(
                        table_name,
                        d_col['col_name'],
                        d_col['col_type'],
                        d_col['max_length'],
                        d_col['nullable']
                    ))
                    connection.commit()

                # проверка cdc-колонок на существование и типы
                cdc_required_types = self.datasource.db_map.cdc_required_types

                for cdc_k, v in cdc_required_types.items():
                    if cdc_k not in existing_cols:
                        cursor.execute(add_col_q.format(
                            table_name, cdc_k, v["type"], "", v["nullable"]))
                    else:
                        # если типы не совпадают
                        if not existing_cols[cdc_k].startswith(v["type"]):
                            cursor.execute(del_col_q.format(table_name, cdc_k))
                            cursor.execute(add_col_q.format(
                                table_name, cdc_k, v["type"], "", v["nullable"]))

                connection.commit()

                # проверяем индексы на колонки и существование,
                # лишние индексы удаляем

                indexes_query = self.datasource.db_map.indexes_query.format(
                    tables_str, self.source.db)
                cursor.execute(indexes_query)
                exist_indexes = cursor.fetchall()

                exist_indexes = self.datasource.get_processed_indexes(
                    exist_indexes)

                index_cols_i, index_name_i = 0, 1

                create_index_q = self.datasource.db_map.create_index_query
                drop_index_q = self.datasource.db_map.drop_index_query

                allright_index_names = []

                for index in exist_indexes:
                    index_name = index[index_name_i]

                    if index_name not in required_indexes:
                        cursor.execute(drop_index_q.format(index_name, table_name))
                    else:
                        index_cols = sorted(index[index_cols_i].split(','))
                        if index_cols != required_indexes[index_name]:

                            cursor.execute(drop_index_q.format(index_name, table_name))
                            cursor.execute(create_index_q.format(
                                index_name, table_name,
                                ','.join(
                                    ['{0}{1}{0}'.format(sep, x)
                                     for x in required_indexes[index_name]]),
                                self.source.db))

                        allright_index_names.append(index_name)

                diff_indexes_names = [
                    x for x in required_indexes if x not in allright_index_names]

                for d_index in diff_indexes_names:
                    cursor.execute(
                        create_index_q.format(
                            d_index, table_name,
                            ','.join(['{0}{1}{0}'.format(sep, x)
                                      for x in required_indexes[d_index]])))

                connection.commit()

            # если таблица не существует
            else:
                # создание таблицы у юзера
                cursor.execute(remote_table_create_query.format(
                    table_name, cols_str))

                # создание индексов
                create_index_q = self.datasource.db_map.create_index_query

                for index_name, index_cols in required_indexes.items():

                    index_cols = [
                        '{0}{1}{0}'.format(sep, x) for x in index_cols]

                    cursor.execute(create_index_q.format(
                        index_name, table_name,
                        ','.join(index_cols), self.source.db))

                connection.commit()

            trigger_names = self.datasource.get_remote_trigger_names(table)
            drop_trigger_query = self.datasource.db_map.drop_remote_trigger

            trigger_commands = remote_triggers_create_query.format(
                orig_table=table, new_table=table_name,
                new=for_triggers['new'],
                old=for_triggers['old'],
                cols=for_triggers['cols'])

            # multi queries of mysql, delimiter $$
            for i, create_trigger_query in enumerate(trigger_commands.split('$$')):

                trigger_name = trigger_names.get("trigger_name_{0}".format(i))

                trigger = DatasourcesTrigger.objects.filter(
                    name=trigger_name, collection_name=table,
                    datasource=self.source)

                if not trigger.exists():

                    # удаляем старый триггер
                    cursor.execute(drop_trigger_query.format(
                        trigger_name=trigger_name, orig_table=table,
                    ))

                    # создаем новый триггер
                    cursor.execute(create_trigger_query)

                    with transaction.atomic():
                        # создаем запись о триггере
                        source_trigger = DatasourcesTrigger(
                            name=trigger_name, collection_name=table,
                            datasource=self.source,
                        )
                        source_trigger.src = create_trigger_query
                        source_trigger.save()

                        # создаем запись о триггере для remote источника
                        DatasourcesJournal.objects.create(
                            name=table_name, collection_name=table,
                            trigger=source_trigger,
                        )

                    connection.commit()
        cursor.close()
        connection.close()


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

    def reload_trigger(self, trigger_name, orig_table, new_table, column_names):
        sep = self.datasource.get_separator()
        insert_cols = ['NEW.{1}{0}{1}'.format(col, sep) for col in column_names]
        select_cols = ['{1}{0}{1}'.format(col, sep) for col in column_names]
        query_params = dict(
            trigger_name=trigger_name,
            new_table=new_table,
            orig_table=orig_table,
            del_condition="{0}cdc_key{0}=OLD.{0}cdc_key{0}".format(sep),
            insert_cols=','.join(insert_cols),
            cols="({0})".format(','.join(select_cols)),
        )
        query = self.datasource.reload_datasource_trigger_query(query_params)
        self.execute(query)
        return query

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


class ClickhouseQuery(object):
    """
    Работа с запросами к Clickhouse
    """

    def __init__(self, ):
        pass
