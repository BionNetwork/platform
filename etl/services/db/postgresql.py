# coding: utf-8
from __future__ import unicode_literals
from .interfaces import Database, Operations
from etl.services.db.maps import postgresql as pgsql_map
from collections import defaultdict
from itertools import groupby
import psycopg2


class Postgresql(Database):
    """Управление источником данных Postgres"""

    db_map = pgsql_map

    @staticmethod
    def get_connection(conn_info):
        """
        Получение соединения к базе данных
        """
        try:
            conn_str = (u"host='{host}' dbname='{db}' user='{login}' "
                        u"password='{password}' port={port}").format(**conn_info)
            conn = psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            return None
        return conn

    @staticmethod
    def get_separator():
        """
            Возвращает кавычки(") для запроса
        """
        return '\"'

    def get_structure_rows_number(self, structure, cols):
        """
        возвращает примерное кол-во строк в запросе для планирования
        :param structure:
        :param cols:
        :return:
        """
        separator = self.get_separator()
        query_join = self.generate_join(structure)

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)
        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        select_query = self.get_select_query().format(
            cols_str, query_join)

        explain_query = 'explain analyze ' + select_query
        records = self.get_query_result(explain_query)
        data = records[0][0].split()
        count = None
        for d in data:
            if d.startswith('rows='):
                count = d
        return int(count[5:])

    @classmethod
    def get_columns_query(cls, tables_str, source):
        # public - default scheme for postgres
        return cls.db_map.cols_query.format(tables_str, source.db, 'public')

    @staticmethod
    def local_table_create_query(key_str, cols_str):
        """
        запрос на создание новой таблицы в локал хранилище
        :param key_str:
        :param cols_str:
        :return:
        """
        create_query = "CREATE TABLE {0} ({1})".format(key_str, cols_str)

        return create_query

    @classmethod
    def check_table_exists_query(cls, table, db):
        """
        Проверка на существование таблицы
        """
        table_exists_query = cls.db_map.check_table_exists.format(table, db)

        return table_exists_query

    @classmethod
    def get_page_select_query(cls, table_name, cols):
        fields_str = '"'+'", "'.join(cols)+'"'
        return cls.db_map.row_query.format(
            fields_str, table_name, '%(limit)s', '%(offset)s')

    @classmethod
    def get_select_dates_query(cls, date_table):
        return cls.db_map.select_dates_query.format(date_table)

    @staticmethod
    def local_table_insert_query(table_name, cols_num):
        """
        Запрос на добавление в новую таблицу локал хранилища

        Args:
            table_name(str): Название таблиц
            cols_num(int): Число столбцов
        Returns:
            str: Строка на выполнение
        """
        cols = '(%s)' % ','.join(['%({0})s'.format(i) for i in xrange(
                cols_num)])
        return "INSERT INTO {0} VALUES {1}".format(table_name, cols)

    @staticmethod
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        return pgsql_map.remote_table_query

    @staticmethod
    def reload_datasource_trigger_query(params):
        """
        запрос на создание триггеров в БД локально для размерностей и мер
        """
        return pgsql_map.dimension_measure_triggers_query.format(**params)

    @staticmethod
    def get_remote_trigger_names(table_name):
        return {
            "trigger_name_0": "cdc_{0}_audit".format(table_name),
        }

    def create_mongo_server(self):
        """
        Создание mongodb-расширения
        с соответсвущим сервером и картой пользователя
        """
        return self.db_map.create_mongo_server

    def create_postgres_server(self):
        return self.db_map.create_postgres_server

    def create_foreign_table_query(self, table_name, cols_types):

        col_names = []
        for field_name, field in cols_types.iteritems():
            col_names.append(u'"{0}" {1}'.format(
                field_name, field['type']))
        query = self.db_map.create_foreign_table_query

        return query.format(table_name=table_name, cols=','.join(col_names))

    def create_materialized_view_query(self, name, relations):
        """
        Запрос на создание материализованного представления

        Args:
            name(str): название представления
            relations(list): Информация об объединяемых таблицах
        """
        table_names = [x['table_hash'] for x in relations]
        query = "CREATE MATERIALIZED VIEW {view_name} AS SELECT {columns} FROM {first_table_name}".format(
            view_name=name,
            columns=','.join(['"sttm__%s".*' % x for x in table_names]),
            first_table_name='"sttm__%s"' % table_names[0])

        for node in relations[1:]:
            query += u' INNER JOIN "{table_name}" ON {condition}'.format(
                table_name=u'sttm__%s' % node['table_hash'],
                condition=u'{l}{operation}{r}'.format(
                    l=node['conditions'][0]['l'], operation=Operations.values[node['conditions'][0]['operation']], r=node['conditions'][0]['r'])
            )
        return query




