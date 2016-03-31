# coding: utf-8
from .interfaces import Database
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

    @staticmethod
    def _get_columns_query(source, tables):
        """
        Получение запросов на получение данных о колонках, индексах и
        ограничениях
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        # public - default scheme for postgres
        cols_query = pgsql_map.cols_query.format(tables_str, source.db, 'public')
        constraints_query = pgsql_map.constraints_query.format(tables_str)
        indexes_query = pgsql_map.indexes_query.format(tables_str)
        return cols_query, constraints_query, indexes_query

    @classmethod
    def get_statistic_query(cls, source, tables):
        """
        запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'
        return cls.db_map.stat_query.format(tables_str)

    @classmethod
    def get_interval_query(cls, source, cols_info):
        """

        Args:


        Returns:

        """
        intervals_query = []
        for table, col_name, col_type, _, _ in cols_info:
            if col_type in cls.db_map.dates:
                query = "SELECT MIN({0}), MAX({0}) FROM {1};".format(
                        col_name, table)
                intervals_query.append([table, col_name, query])
        return intervals_query

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
            fields_str, table_name, '%s', '%s')

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
        insert_query = "INSERT INTO {0} VALUES {1}".format(table_name, cols)
        return insert_query

    @staticmethod
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        return pgsql_map.remote_table_query

    @staticmethod
    def remote_triggers_create_query():
        """
        запрос на создание триггеров в БД клиента
        """
        return pgsql_map.remote_triggers_query

    @staticmethod
    def get_primary_key(table, db):
        """
        запрос на получение Primary Key
        """
        return pgsql_map.pr_key_query.format("('{0}')".format(table), db)

    @staticmethod
    def delete_primary_query(table, primary):
        return pgsql_map.delete_primary_key.format(table, primary)

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
