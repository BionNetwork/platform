# coding: utf-8
from __future__ import unicode_literals

import binascii
import psycopg2
import MySQLdb
import json
import operator
import datetime
import decimal
from itertools import groupby
from collections import defaultdict

from django.conf import settings
from django.utils import timezone

from core.models import ConnectionChoices
from . import r_server
from redis_collections import Dict as RedisDict
from .maps import postgresql as psql_map
from .maps import mysql as mysql_map
from core.models import Datasource, DatasourceMeta


class BaseEnum(object):
    """
        Базовый класс для перечислений
    """
    values = {}

    @classmethod
    def get_value(cls, key):
        if key not in cls.values:
            raise ValueError("Unknown key provided " + key)
        return cls.values[key]


class JoinTypes(BaseEnum):

    INNER, LEFT, RIGHT = ('inner', 'left', 'right')

    values = {
        INNER: "INNER JOIN",
        LEFT: "LEFT JOIN",
        RIGHT: "RIGHT JOIN",
    }


class Operations(object):

    EQ, LT, GT, LTE, GTE, NEQ = ('eq', 'lt', 'gt', 'lte', 'gte', 'neq')

    values = {
        EQ: '=',
        LT: '<',
        GT: '>',
        LTE: '<=',
        GTE: '>=',
        NEQ: '<>',
    }

    @staticmethod
    def get_value(operation_type):
        if operation_type not in Operations.values:
            raise ValueError("Unknown operation type provided " + operation_type)
        return Operations.values[operation_type]


def get_utf8_string(value):
    """
    Кодирование в utf-8 строки
    :param value: string
    :return: string
    """
    return unicode(value)


class Database(object):
    """
    Базовыми возможности для работы с базами данных
    Получение информации о таблице, список колонок, проверка соединения и т.д.
    """
    def __init__(self, connection):
        self.connection = self.get_connection(connection)

    @staticmethod
    def get_connection(conn_info):
        """
        достает коннекшн бд
        :param conn_info: dict
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def lose_brackets(str_):
        """
        типы колонок приходят типа bigint(20), убираем скобки
        :param str_:
        :return: str
        """
        return str_.split('(')[0].lower()

    def get_query_result(self, query):
        """
        достает результат запроса
        :param query: str
        :return: list
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def _get_columns_query(source, tables):
        raise ValueError("Columns query is not realized")

    def generate_join(self, structure, main_table=None):
        """
        Генерация соединения таблиц для реляционных источников

        Args:
            structure: dict структура для генерации
            main_table: string основная таблица, участвующая в связях
        """

        separator = self.get_separator()

        # определяем начальную таблицу
        if main_table is None:
            main_table = structure['val']
            query_join = '{sep}{table}{sep}'.format(
                    table=main_table, sep=separator)
        else:
            query_join = ''
        for child in structure['childs']:
            # определяем тип соединения
            query_join += " " + JoinTypes.get_value(child['join_type'])

            # присоединяем таблицу + ' ON '
            query_join += " " + '{sep}{table}{sep}'.format(
                table=child['val'], sep=separator) + " ON ("

            # список джойнов, чтобы перечислить через 'AND'
            joins_info = []

            # определяем джойны
            for joinElement in child['joins']:

                joins_info.append(("{sep}%s{sep}.{sep}%s{sep} %s {sep}%s{sep}.{sep}%s{sep}" % (
                    joinElement['left']['table'], joinElement['left']['column'],
                    Operations.get_value(joinElement['join']['value']),
                    joinElement['right']['table'], joinElement['right']['column']
                )).format(sep=separator))

            query_join += " AND ".join(joins_info) + ")"

            # рекурсивно обходим остальные элементы
            query_join += self.generate_join(child, child['val'])

        return query_join

    @staticmethod
    def get_rows_query():
        """
        возвращает селект запрос
        :raise: NotImplementedError
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_rows(self, cols, structure):
        """
        достает строки из соурса для превью
        :param cols: list
        :param structure: dict
        :return: list
        """
        query_join = self.generate_join(structure,)

        separator = self.get_separator()

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)

        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        query = self.get_rows_query().format(
            cols_str, query_join,
            settings.ETL_COLLECTION_PREVIEW_LIMIT, 0)

        records = self.get_query_result(query)
        return records

    @staticmethod
    def get_separator():
        """
            Возвращает ковычки( ' or " ) для запроса
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def get_statistic_query(source, tables):
        """
        строка для статистики таблицы
        :param source: Datasource
        :param tables: list
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_statistic(self, source, tables):
        """
        возвращает статистику таблиц
        :param source:
        :param tables:
        :return: list
        """
        stats_query = self.get_statistic_query(source, tables)
        stat_records = self.get_query_result(stats_query)
        stat_records = self.processing_statistic(stat_records)
        return stat_records

    @staticmethod
    def processing_statistic(records):
        """
        обработка статистики
        :param records: list
        :return: dict
        """
        return {x[0]: ({'count': int(x[1]), 'size': x[2]}
                       if (x[1] and x[2]) else None) for x in records}

    @staticmethod
    def local_table_create_query(key_str, cols_str):
        """
        запрос создания таблицы в локал хранилище
        :param key_str:
        :param cols_str:
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def local_table_insert_query(key_str):
        """
        запрос инсерта в таблицу локал хранилища
        :param key_str: str
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_explain_result(self, explain_row):
        """
        запрос получения количества строк в селект запросе
        :param explain_row: str (explain + запрос)
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def get_select_query():
        """
        возвращает селект запрос
        :return: str
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)


class Postgresql(Database):
    """Управление источником данных Postgres"""

    @staticmethod
    def get_connection(conn_info):
        """
        возвращает коннект бд
        :param conn_info: dict
        :return: connection
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
            Возвращает ковычки(") для запроса
        """
        return '\"'

    def get_tables(self, source):
        """
        возвращает таблицы соурса
        :param source: Datasource
        :return: list
        """
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='public' order by table_name;
        """
        records = self.get_query_result(query)
        records = map(lambda x: {'name': x[0], },
                      sorted(records, key=lambda y: y[0]))

        return records

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
        запросы для колонок, констраинтов, индексов соурса
        :param source: Datasource
        :param tables:
        :return: tuple
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        # public - default scheme for postgres
        cols_query = psql_map.cols_query.format(tables_str, source.db, 'public')

        constraints_query = psql_map.constraints_query.format(tables_str)

        indexes_query = psql_map.indexes_query.format(tables_str)

        return cols_query, constraints_query, indexes_query

    def get_columns(self, source, tables):
        """
        Получение списка колонок в таблицах
        :param source:
        :param tables:
        :return:
        """
        columns_query, consts_query, indexes_query = self._get_columns_query(
            source, tables)

        col_records = self.get_query_result(columns_query)
        index_records = self.get_query_result(indexes_query)
        const_records = self.get_query_result(consts_query)

        return col_records, index_records, const_records

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов соурса
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        indexes = defaultdict(list)
        itable_name, icol_names, index_name, primary, unique = xrange(5)

        for ikey, igroup in groupby(index_records, lambda x: x[itable_name]):
            for ig in igroup:
                indexes[ikey].append({
                    "name": ig[index_name],
                    "columns": ig[icol_names].split(','),
                    "is_primary": ig[primary] == 't',
                    "is_unique": ig[unique] == 't',
                })

        constraints = defaultdict(list)
        (c_table_name, c_col_name, c_name, c_type,
         c_foreign_table, c_foreign_col, c_update, c_delete) = xrange(8)

        for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
            for ig in igroup:
                constraints[ikey].append({
                    "c_col_name": ig[c_col_name],
                    "c_name": ig[c_name],
                    "c_type": ig[c_type],
                    "c_f_table": ig[c_foreign_table],
                    "c_f_col": ig[c_foreign_col],
                    "c_upd": ig[c_update],
                    "c_del": ig[c_delete],
                })

        columns = defaultdict(list)
        foreigns = defaultdict(list)

        table_name, col_name, col_type = xrange(3)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

                for i in t_indexes:
                    if col in i['columns']:
                        is_index = True
                        index_name = i['name']
                        for c in t_consts:
                            const_type = c['c_type']
                            if index_name == c['c_name']:
                                if const_type == 'UNIQUE':
                                    is_unique = True
                                elif const_type == 'PRIMARY KEY':
                                    is_unique = True
                                    is_primary = True

                columns[key].append({"name": col,
                                     "type": (psql_map.PSQL_TYPES[cls.lose_brackets(x[col_type])]
                                              or x[col_type]),
                                     "is_index": is_index,
                                     "is_unique": is_unique, "is_primary": is_primary})

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'FOREIGN KEY':
                    foreigns[key].append({
                        "name": c['c_name'],
                        "source": {"table": key, "column": c["c_col_name"]},
                        "destination":
                            {"table": c["c_f_table"], "column": c["c_f_col"]},
                        "on_delete": c["c_del"],
                        "on_update": c["c_upd"],
                    })
        return columns, indexes, foreigns

    @staticmethod
    def get_rows_query():
        """
        возвращает селект запрос c лимитом, оффсетом
        :return: str
        """
        query = "SELECT {0} FROM {1} LIMIT {2} OFFSET {3};"
        return query

    @staticmethod
    def get_select_query():
        """
        возвращает селект запрос
        :return: str
        """
        query = "SELECT {0} FROM {1};"
        return query

    @staticmethod
    def get_statistic_query(source, tables):
        """
        запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        tables_str = '(' + ', '.join(["'{0}'::regclass::oid".format(y) for y in tables]) + ')'
        stats_query = psql_map.stat_query.format(tables_str)
        return stats_query

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

    @staticmethod
    def local_table_insert_query(key_str):
        """
        запрос на инсерт в новую таблицу локал хранилища
        :param key_str:
        :return:
        """
        insert_query = "INSERT INTO {0} VALUES {1}".format(key_str, '{0}')
        return insert_query


class Mysql(Database):
    """Управление источником данных MySQL"""

    @staticmethod
    def get_connection(conn_info):
        """
        connection бд соурса
        :param conn_info:
        :return: connection
        """
        try:
            connection = {'db': str(conn_info['db']),
                          'host': str(conn_info['host']),
                          'port': int(conn_info['port']),
                          'user': str(conn_info['login']),
                          'passwd': str(conn_info['password']),
                          'use_unicode': True,
                          }
            conn = MySQLdb.connect(**connection)
        except MySQLdb.OperationalError:
            return None
        return conn

    @staticmethod
    def get_separator():
        """
            Возвращает ковычки(') для запроса
        """
        return '`'

    def get_tables(self, source):
        """
        Получение списка таблиц
        :param source:
        :return:
        """
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='{0}' order by table_name;
        """.format(source.db)

        records = self.get_query_result(query)
        records = map(lambda x: {'name': x[0], }, records)

        return records

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

        select_query = self.get_select_query()

        explain_query = 'explain ' + select_query.format(
            cols_str, query_join)

        records = self.get_query_result(explain_query)
        total = 1
        for rec in records:
            total *= int(rec[8])
        # если total меньше 100000, то делаем count запрос
        # иначе возвращаем перемноженное кол-во в каждой строке,
        # возвращенной EXPLAIN-ом
        if total < 100000:
            rows_count_query = select_query.format(
                'count(1) ', query_join)
            records = self.get_query_result(rows_count_query)
            # records = ((100L,),)
            return int(records[0][0])
        return total

    @staticmethod
    def _get_columns_query(source, tables):
        """
        запросы для колонок, констраинтов, индексов соурса
        :param source: Datasource
        :param tables:
        :return: tuple
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        cols_query = mysql_map.cols_query.format(tables_str, source.db)

        constraints_query = mysql_map.constraints_query.format(tables_str, source.db)

        indexes_query = mysql_map.indexes_query.format(tables_str, source.db)

        return cols_query, constraints_query, indexes_query

    def get_columns(self, source, tables):
        """
        Получение списка колонок в таблицах
        :param source:
        :param tables:
        :return:
        """
        columns_query, consts_query, indexes_query = self._get_columns_query(
            source, tables)

        col_records = self.get_query_result(columns_query)
        index_records = self.get_query_result(indexes_query)
        const_records = self.get_query_result(consts_query)

        return col_records, index_records, const_records

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов соурса
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        indexes = defaultdict(list)
        itable_name, icol_names, index_name, primary, unique = xrange(5)

        for ikey, igroup in groupby(index_records, lambda x: x[itable_name]):
            for ig in igroup:
                indexes[ikey].append({
                    "name": ig[index_name],
                    "columns": ig[icol_names].split(','),
                    "is_primary": ig[primary] == 't',
                    "is_unique": ig[unique] == 't',
                })

        constraints = defaultdict(list)
        (c_table_name, c_col_name, c_name, c_type,
         c_foreign_table, c_foreign_col, c_update, c_delete) = xrange(8)

        for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
            for ig in igroup:
                constraints[ikey].append({
                    "c_col_name": ig[c_col_name],
                    "c_name": ig[c_name],
                    "c_type": ig[c_type],
                    "c_f_table": ig[c_foreign_table],
                    "c_f_col": ig[c_foreign_col],
                    "c_upd": ig[c_update],
                    "c_del": ig[c_delete],
                })

        columns = defaultdict(list)
        foreigns = defaultdict(list)

        table_name, col_name, col_type = xrange(3)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

                for i in t_indexes:
                    if col in i['columns']:
                        is_index = True
                        for c in t_consts:
                            const_type = c['c_type']
                            if col == c['c_col_name']:
                                if const_type == 'UNIQUE':
                                    is_unique = True
                                elif const_type == 'PRIMARY KEY':
                                    is_unique = True
                                    is_primary = True

                columns[key].append({"name": col,
                                     "type": (mysql_map.MYSQL_TYPES[cls.lose_brackets(x[col_type])]
                                              or x[col_type]),
                                     "is_index": is_index,
                                     "is_unique": is_unique, "is_primary": is_primary})

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'FOREIGN KEY':
                    foreigns[key].append({
                        "name": c['c_name'],
                        "source": {"table": key, "column": c["c_col_name"]},
                        "destination":
                            {"table": c["c_f_table"], "column": c["c_f_col"]},
                        "on_delete": c["c_del"],
                        "on_update": c["c_upd"],
                    })
        return columns, indexes, foreigns

    @staticmethod
    def get_rows_query():
        """
        возвращает селект запрос c лимитом, оффсетом
        :return: str
        """
        query = "SELECT {0} FROM {1} LIMIT {2} OFFSET {3};"
        return query

    @staticmethod
    def get_select_query():
        """
        возвращает селект запрос
        :return: str
        """
        query = "SELECT {0} FROM {1};"
        return query

    @staticmethod
    def get_statistic_query(source, tables):
        """
        запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'
        stats_query = mysql_map.stat_query.format(tables_str, source.db)
        return stats_query


class DatabaseService(object):
    """Сервис для источников данных"""

    @staticmethod
    def factory(**connection):
        """
        фабрика для инстанса бд
        :param connection: **dict
        :return: instance :raise ValueError:
        """
        conn_type = int(connection.get('conn_type', ''))
        del connection['conn_type']

        if conn_type == ConnectionChoices.POSTGRESQL:
            return Postgresql(connection)
        elif conn_type == ConnectionChoices.MYSQL:
            return Mysql(connection)
        else:
            raise ValueError("Неизвестный тип подключения!")

    @classmethod
    def get_source_instance(cls, source):
        """
        инстанс бд соурса
        :param source: Datasource
        :return: instance
        """
        data = cls.get_source_data(source)
        instance = cls.factory(**data)
        return instance

    @classmethod
    def get_tables(cls, source):
        """
        возвращает таблицы соурса
        :type source: Datasource
        """
        instance = cls.get_source_instance(source)
        return instance.get_tables(source)

    @classmethod
    def get_source_data(cls, source):
        """
        Возвращает список модели источника данных
        :type source: Datasource
        :return list
        """
        return dict({'db': source.db, 'host': source.host, 'port': source.port, 'login': source.login,
                     'password': source.password, 'conn_type': source.conn_type})

    @classmethod
    def get_columns_info(cls, source, tables):
        """
        Получение списка колонок
        :param source: Datasource
        :param tables:
        :return:
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
    def get_rows_query(cls, source):
        """
        Получение запроса выбранных колонок из указанных таблиц выбранного источника
        :param source: Datasource
        :return:
        """
        instance = cls.get_source_instance(source)
        return instance.get_rows_query()

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
    def get_table_create_query(cls, local_instance, key_str, cols_str):
        """
        Получение запроса на создание новой таблицы
        для локального хранилища данных
        :param local_instance: Database
        :param key_str: str
        :param cols_str: str
        :return: str
        """
        create_query = local_instance.local_table_create_query(key_str, cols_str)
        return create_query

    @classmethod
    def get_table_insert_query(cls, local_instance, key_str):
        """
        Получение запроса на заполнение таблицы
        для локального хранилища данных
        :param local_instance: Database
        :param key_str: str
        :return: str
        """
        insert_query = local_instance.local_table_insert_query(key_str)
        return insert_query

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


class Node(object):
    """
        Узел дерева таблиц
    """
    def __init__(self, t_name, parent=None, joins=[], join_type='inner'):
        self.val = t_name
        self.parent = parent
        self.childs = []
        self.joins = joins
        self.join_type = join_type

    def get_node_joins_info(self):
        """
        связи узла
        :return: defaultdict
        """
        node_joins = defaultdict(list)

        n_val = self.val
        for join in self.joins:
            left = join['left']
            right = join['right']
            operation = join['join']
            if n_val == right['table']:
                node_joins[left['table']].append({
                    "left": left, "right": right,
                    "join": operation
                })
            else:
                node_joins[right['table']].append({
                    "left": right, "right": left,
                    "join": operation,
                })
        return node_joins


class TablesTree(object):
    """
        Дерево Таблиц
    """

    def __init__(self, t_name):
        self.root = Node(t_name)

    # def display(self):
    #     if self.root:
    #         print self.root.val, self.root.joins
    #         r_chs = [x for x in self.root.childs]
    #         print [(x.val, x.joins) for x in r_chs]
    #         for c in r_chs:
    #             print [x.val for x in c.childs]
    #         print 80*'*'
    #     else:
    #         print 'Empty Tree!!!'

    @classmethod
    def get_tree_ordered_nodes(cls, nodes):
        """
        узлы дерева по порядку от корня вниз слева направо
        :param nodes: list
        :return: list
        """
        all_nodes = []
        all_nodes += nodes
        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            all_nodes += cls.get_tree_ordered_nodes(child_nodes)
        return all_nodes

    @classmethod
    def get_nodes_count_by_level(cls, nodes):
        """
        список количества нодов на каждом уровне дерева
        :param nodes: list
        :return: list
        """
        counts = [len(nodes)]

        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            counts += cls.get_nodes_count_by_level(child_nodes)
        return counts

    @classmethod
    def get_tree_structure(cls, root):
        """
        структура дерева
        :param root: Node
        :return: dict
        """
        root_info = {'val': root.val, 'childs': [], 'joins': list(root.joins), }

        root_info['join_type'] = (
            None if not root_info['joins'] else root.join_type)

        for ch in root.childs:
            root_info['childs'].append(cls.get_tree_structure(ch))
        return root_info

    @classmethod
    def build_tree(cls, childs, tables, tables_info):
        """
        строит дерево таблиц, возвращает таблицы без свяезй
        :param childs:
        :param tables:
        :param tables_info:
        :return: list
        """

        def inner_build_tree(childs, tables):
            child_vals = [x.val for x in childs]
            tables = [x for x in tables if x not in child_vals]

            new_childs = []

            for child in childs:
                new_childs += child.childs
                r_val = child.val
                l_info = tables_info[r_val]

                for t_name in tables[:]:
                    r_info = tables_info[t_name]
                    joins = cls.get_joins(r_val, t_name, l_info, r_info)

                    if joins:
                        tables.remove(t_name)
                        new_node = Node(t_name, child, joins)
                        child.childs.append(new_node)
                        new_childs.append(new_node)

            if new_childs and tables:
                tables = inner_build_tree(new_childs, tables)

            # таблицы без связей
            return tables

        tables = inner_build_tree(childs, tables)

        return tables

    @classmethod
    def select_tree(cls, trees):
        """
        возвращает из списка деревьев лучшее дерево по насыщенности
        детей сверху вниз
        :param trees: list
        :return: list
        """
        counts = {}
        for tr_name, tree in trees.iteritems():
            counts[tr_name] = cls.get_nodes_count_by_level([tree.root])
        root_table = max(counts.iteritems(), key=operator.itemgetter(1))[0]
        return trees[root_table]

    @classmethod
    def build_tree_by_structure(cls, structure):
        """
        строит дерево по структуре дерева
        :param structure: dict
        :return: TablesTree
        """
        tree = TablesTree(structure['val'])

        def inner_build(root, childs):
            for ch in childs:
                new_node = Node(ch['val'], root, ch['joins'],
                                ch['join_type'])
                root.childs.append(new_node)
                inner_build(new_node, ch['childs'])

        inner_build(tree.root, structure['childs'])

        return tree

    @classmethod
    def update_node_joins(cls, sel_tree, left_table,
                          right_table, join_type, joins):
        """
        добавляет/меняет связи между таблицами
        :param sel_tree: TablesTree
        :param left_table: str
        :param right_table: str
        :param join_type: str
        :param joins: list
        """
        nodes = cls.get_tree_ordered_nodes([sel_tree.root, ])
        parent = [x for x in nodes if x.val == left_table][0]
        childs = [x for x in parent.childs if x.val == right_table]

        # случай, когда две таблицы не имели связей
        if not childs:
            node = Node(right_table, parent, [], join_type)
            parent.childs.append(node)
        else:
            # меняем существующие связи
            node = childs[0]
            node.joins = []
            node.join_type = join_type

        for came_join in joins:
            parent_col, oper, child_col = came_join
            node.joins.append({
                'left': {'table': left_table, 'column': parent_col},
                'right': {'table': right_table, 'column': child_col},
                'join': {"type": join_type, "value": oper},
            })

    @classmethod
    def get_joins(cls, l_t, r_t, l_info, r_info):
        """
        Функция выявляет связи между таблицами
        :param l_t:
        :param r_t:
        :param l_info:
        :param r_info:
        :return: list
        """
        l_cols = l_info['columns']
        r_cols = r_info['columns']

        joins = set()
        # избавляет от дублей
        unique_set = set()

        for l_c in l_cols:
            l_str = '{0}_{1}'.format(l_t, l_c['name'])
            for r_c in r_cols:
                r_str = '{0}_{1}'.format(r_t, r_c['name'])
                if l_c['name'] == r_str and l_c['type'] == r_c['type']:
                    j_tuple = (l_t, l_c["name"], r_t, r_c["name"])
                    sort_j_tuple = tuple(sorted(j_tuple))
                    if sort_j_tuple not in unique_set:
                        joins.add(j_tuple)
                        unique_set.add(sort_j_tuple)
                        break
                if l_str == r_c["name"] and l_c['type'] == r_c['type']:
                    j_tuple = (l_t, l_c["name"], r_t, r_c["name"])
                    sort_j_tuple = tuple(sorted(j_tuple))
                    if sort_j_tuple not in unique_set:
                        joins.add(j_tuple)
                        unique_set.add(sort_j_tuple)
                        break

        l_foreign = l_info['foreigns']
        r_foreign = r_info['foreigns']

        for f in l_foreign:
            if f['destination']['table'] == r_t:
                j_tuple = (
                    f['source']['table'],
                    f['source']['column'],
                    f['destination']['table'],
                    f['destination']['column'],
                )
                sort_j_tuple = tuple(sorted(j_tuple))
                if sort_j_tuple not in unique_set:
                    joins.add(j_tuple)
                    unique_set.add(sort_j_tuple)
                    break

        for f in r_foreign:
            if f['destination']['table'] == l_t:
                j_tuple = (
                    f['source']['table'],
                    f['source']['column'],
                    f['destination']['table'],
                    f['destination']['column'],
                )
                sort_j_tuple = tuple(sorted(j_tuple))
                if sort_j_tuple not in unique_set:
                    joins.add(j_tuple)
                    unique_set.add(sort_j_tuple)
                    break

        dict_joins = []

        for join in joins:
            dict_joins.append({
                'left': {'table': join[0], 'column': join[1]},
                'right': {'table': join[2], 'column': join[3]},
                'join': {"type": JoinTypes.INNER, "value": Operations.EQ},
            })

        return dict_joins


class TableTreeRepository(object):
    """
        Обработчик деревьев TablesTree
    """

    @classmethod
    def build_trees(cls, tables, source):
        """
        строит всевозможные деревья
        :param tables: list
        :param source: Datasource
        :return:
        """
        trees = {}
        without_bind = {}

        tables_info = RedisSourceService.info_for_tree_building(
                (), tables, source)

        for t_name in tables:
            tree = TablesTree(t_name)

            without_bind[t_name] = TablesTree.build_tree(
                [tree.root, ], tables, tables_info)
            trees[t_name] = tree

        return trees, without_bind

    @classmethod
    def delete_nodes_from_tree(cls, tree, source, tables):
        """
        удаляет узлы дерева
        :param tree: TablesTree
        :param source: Datasource
        :param tables: list
        """

        def inner_delete(node):
            for child in node.childs[:]:
                if child.val in tables:
                    child.parent = None
                    node.childs.remove(child)
                else:
                    inner_delete(child)

        r_val = tree.root.val
        if r_val in tables:
            RedisSourceService.tree_full_clean(source)
            tree.root = None
        else:
            inner_delete(tree.root)


class RedisCacheKeys(object):
    """Ключи для редиса"""
    @staticmethod
    def get_user_databases(user_id):
        """
        бд юзера
        :param user_id:
        :return:
        """
        return 'user_datasources:{0}'.format(user_id)

    @staticmethod
    def get_user_datasource(user_id, datasource_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:{1}'.format(
            RedisCacheKeys.get_user_databases(user_id), datasource_id)

    @staticmethod
    def get_user_collection_counter(user_id, datasource_id):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param user_id: int
        :param datasource_id: int
        :return: str
        """
        return '{0}:{1}'.format(RedisCacheKeys.get_user_datasource(
            user_id, datasource_id), 'counter')

    @staticmethod
    def get_active_table(user_id, datasource_id, number):
        """
        фулл инфа таблицы, которая в дереве
        :param user_id:
        :param datasource_id:
        :param number:
        :return:
        """
        return '{0}:collection:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), number)

    @staticmethod
    def get_active_tables(user_id, datasource_id):
        """
        список таблиц из дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:active_collections'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_active_table_by_name(user_id, datasource_id, table):
        """
        фулл инфа таблицы, которая НЕ в дереве
        :param user_id:
        :param datasource_id:
        :param table:
        :return:
        """
        return '{0}:collection:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), table)

    @staticmethod
    def get_source_joins(user_id, datasource_id):
        """
        инфа о джоинах дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:joins'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))


    @staticmethod
    def get_source_remain(user_id, datasource_id):
        """
        таблица без связей
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:remain'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_active_tree(user_id, datasource_id):
        """
        структура дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:active:tree'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_task_counter():
        """
        счетчик задач
        :return:
        """
        return 'tasks_counter'

    @staticmethod
    def get_user_task_list(user_id):
        """
        список задач юзера
        :param user_id:
        :return:
        """
        return 'user_tasks:{0}'.format(user_id)


class RedisSourceService(object):
    """
        Сервис по работе с редисом
    """

    @classmethod
    def delete_datasource(cls, source):
        """
        удаляет информацию о датасосре из редиса
        :param cls:
        :param source: Datasource
        """
        user_db_key = RedisCacheKeys.get_user_databases(source.user_id)
        user_datasource_key = RedisCacheKeys.get_user_datasource(
            source.user_id, source.id)

        r_server.lrem(user_db_key, 1, source.id)
        r_server.delete(user_datasource_key)

    @classmethod
    def get_tables(cls, source, tables):
        """
        достает информацию о таблицах из редиса
        :param source: Datasource
        :param tables: list
        :return: list
        """
        user_db_key = RedisCacheKeys.get_user_databases(source.user_id)
        user_datasource_key = RedisCacheKeys.get_user_datasource(source.user_id, source.id)

        def inner_save_tables():
            new_db = {
                "db": source.db,
                "host": source.host,
                "tables": tables
            }
            if str(source.id) not in r_server.lrange(user_db_key, 0, -1):
                r_server.rpush(user_db_key, source.id)
            r_server.set(user_datasource_key, json.dumps(new_db))
            r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)
            return new_db

        if not r_server.exists(user_datasource_key):
            return inner_save_tables()

        return json.loads(r_server.get(user_datasource_key))

    @classmethod
    def delete_tables(cls, source, tables):
        """
        удаляет инфу о таблицах
        :param source: Datasource
        :param tables: list
        """
        rck = RedisCacheKeys

        str_table = rck.get_active_table(source.user_id, source.id, '{0}')
        str_table_by_name = rck.get_active_table(source.user_id, source.id, '{0}')
        str_active_tables = rck.get_active_tables(source.user_id, source.id)
        str_joins = rck.get_source_joins(source.user_id, source.id)

        actives = json.loads(r_server.get(str_active_tables))
        joins = json.loads(r_server.get(str_joins))

        # если есть, то удаляем таблицу без связей
        for t_name in tables:
            r_server.delete(str_table_by_name.format(t_name))

        # удаляем все джоины пришедших таблиц
        cls.initial_delete_joins(tables, joins)
        child_tables = cls.delete_joins(tables, joins)

        # добавляем к основным таблицам, их дочерние для дальнейшего удаления
        tables += child_tables

        r_server.set(str_joins, json.dumps(joins))

        # удаляем полную инфу пришедших таблиц
        cls.delete_tables_info(tables, actives, str_table)
        r_server.set(str_active_tables, json.dumps(actives))

    @classmethod
    def initial_delete_joins(cls, tables, joins):
        """
            удаляем связи таблиц, из таблиц, стоящих левее выбранных
        """
        for v in joins.values():
            for j in v[:]:
                if j['right']['table'] in tables:
                    v.remove(j)

    @classmethod
    def delete_joins(cls, tables, joins):
        """
            удаляем связи таблиц, плюс связи таблиц, стоящих правее выбранных!
            возвращает имена дочерних таблиц на удаление
        """
        destinations = []
        for table in tables:
            if table in joins:
                destinations += [x['right']['table'] for x in joins[table]]
                del joins[table]
                if destinations:
                    destinations += cls.delete_joins(destinations, joins)
        return destinations

    @classmethod
    def delete_tables_info(cls, tables, actives, str_table):
        """
        удаляет инфу о таблицах
        :param tables: list
        :param actives: list
        :param str_table: str
        """
        names = [x['name'] for x in actives]
        for table in tables:
            if table in names:
                found = [x for x in actives if x['name'] == table][0]
                r_server.delete(str_table.format(found['order']))
                actives.remove(found)

    @classmethod
    def get_table_full_info(cls, source, table):
        """
        Получение полной информации по источнику из хранилища
        :param source: Datasource
        :param table: string
        :return:
        """
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)

        active_tables = json.loads(r_server.get(str_active_tables))

        if r_server.exists(str_table_by_name.format(table)):
            return r_server.get(str_table_by_name.format(table))
        else:
            order = [x for x in active_tables if x['name'] == table][0]['order']
            return r_server.get(str_table.format(order))

    @classmethod
    def save_active_tree(cls, tree_structure, source):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        :param source: Datasource
        """
        str_active_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)

        r_server.set(str_active_tree, json.dumps(tree_structure))

    # достаем структуру дерева из редиса
    @classmethod
    def get_active_tree_structure(cls, source):
        """
        Получение текущей структуры дерева источника
        :param source: Datasource
        :return:
        """
        str_active_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)

        return json.loads(r_server.get(str_active_tree))

    @classmethod
    def insert_tree(cls, structure, ordered_nodes, source, update_joins=True):
        """
        сохраняем полную инфу о дереве
        :param structure:
        :param ordered_nodes:
        :param source:
        """

        user_id, source_id = source.user_id, source.id

        str_table = RedisCacheKeys.get_active_table(
            user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            user_id, source_id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            user_id, source_id)
        str_joins = RedisCacheKeys.get_source_joins(
            user_id, source_id)

        # новый список коллекций
        new_actives = []
        # старый список коллекций
        old_actives = cls.get_active_list(user_id, source_id)

        joins_in_redis = defaultdict(list)

        pipe = r_server.pipeline()

        for node in ordered_nodes:
            n_val = node.val
            order = cls.get_order_from_actives(n_val, old_actives)
            # если инфы о коллекции нет
            if order is None:

                # cчетчик коллекций пользователя
                coll_counter = cls.get_next_user_collection_counter(
                    user_id, source_id)
                # достаем инфу либо по имени, либо по порядковому номеру
                pipe.set(str_table.format(coll_counter),
                         RedisSourceService.get_table_full_info(source, n_val))
                # удаляем таблицы с именованными ключами
                pipe.delete(str_table_by_name.format(n_val))
                # добавляем новую таблциу в карту активных таблиц
                new_actives.append({'name': n_val, 'order': coll_counter})
            else:
                # старая таблица
                new_actives.append({'name': n_val, 'order': order})

            # добавляем инфу новых джойнов
            if update_joins:
                joins = node.get_node_joins_info()
                for k, v in joins.iteritems():
                    joins_in_redis[k] += v

        pipe.set(str_active_tables, json.dumps(new_actives))
        if update_joins:
            pipe.set(str_joins, json.dumps(joins_in_redis))

        pipe.execute()

        # сохраняем само дерево
        RedisSourceService.save_active_tree(structure, source)

    @classmethod
    def tree_full_clean(cls, source):
        """ удаляет информацию о таблицах, джоинах, дереве
            из редиса
        """
        user_id = source.user_id
        source_id = source.id

        active_tables_key = RedisCacheKeys.get_active_tables(
            user_id, source_id)
        tables_joins_key = RedisCacheKeys.get_source_joins(
            user_id, source_id)
        tables_remain_key = RedisCacheKeys.get_source_remain(
            user_id, source_id)
        active_tree_key = RedisCacheKeys.get_active_tree(
            user_id, source_id)
        table_by_name_key = RedisCacheKeys.get_active_table_by_name(
            user_id, source_id, '{0}')

        # delete keys in redis
        pipe = r_server.pipeline()
        pipe.delete(table_by_name_key.format(r_server.get(tables_remain_key)))
        pipe.delete(tables_remain_key)

        actives = cls.get_active_list(source.user_id, source.id)
        for t in actives:
            table_str = RedisCacheKeys.get_active_table(
                user_id, source_id, t['order'])
            pipe.delete(table_str)

        pipe.delete(active_tables_key)
        pipe.delete(tables_joins_key)
        pipe.delete(active_tree_key)
        pipe.execute()

    @classmethod
    def insert_remains(cls, source, remains):
        """
        сохраняет таблицу без связей
        :param source: Datasource
        :param remains: list
        :return:
        """
        str_remain = RedisCacheKeys.get_source_remain(source.user_id, source.id)
        if remains:
            # первая таблица без связей
            last = remains[0]
            # таблица без связей
            r_server.set(str_remain, last)

            # удаляем таблицы без связей, кроме первой
            cls.delete_unneeded_remains(source, remains[1:])
        else:
            last = None
            # r_server.set(str_remain, '')
        # либо таблица без связи, либо None
        return last

    @classmethod
    def delete_unneeded_remains(cls, source, remains):
        """
        удаляет таблицы без связей,(все кроме первой)
        :param source: Datasource
        :param remains: list
        """
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        for t_name in remains:
            r_server.delete(str_table_by_name.format(t_name))

    @classmethod
    def delete_last_remain(cls, source):
        """
        удаляет единственную таблицу без связей
        :param source: Datasource
        """
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_remain = RedisCacheKeys.get_source_remain(
            source.user_id, source.id)
        if r_server.exists(str_remain):
            last = r_server.get(str_remain)
            r_server.delete(str_table_by_name.format(last))
            r_server.delete(str_remain)

    @classmethod
    def get_columns_for_tables_without_bind(
            cls, source, parent_table, without_bind_table):
        """
        колонки таблиц, которым хотим добавить джойны
        :param source:
        :param parent_table:
        :param without_bind_table:
        :return: :raise Exception:
        """
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        str_remain = RedisCacheKeys.get_source_remain(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        err_msg = 'Истекло время хранения ключей в редисе!'

        if (not r_server.exists(str_active_tables) or
                not r_server.exists(str_remain)):
            raise Exception(err_msg)

        wo_bind_columns = json.loads(r_server.get(str_table_by_name.format(
            without_bind_table)))['columns']

        actives = json.loads(r_server.get(str_active_tables))

        parent_columns = json.loads(r_server.get(str_table.format(
            cls.get_order_from_actives(parent_table, actives)
        )))['columns']

        return {
            without_bind_table: [x['name'] for x in wo_bind_columns],
            parent_table: [x['name'] for x in parent_columns],
        }

    @classmethod
    def get_columns_for_tables_with_bind(
            cls, source, parent_table, child_table):
        """
        колонки таблиц, у которых есть связи
        :param source:
        :param parent_table:
        :param child_table:
        :return: :raise Exception:
        """
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_joins = RedisCacheKeys.get_source_joins(
            source.user_id, source.id)

        err_msg = 'Истекло время хранения ключей!'

        if (not r_server.exists(str_active_tables) or
                not r_server.exists(str_joins)):
            raise Exception(err_msg)

        actives = json.loads(r_server.get(str_active_tables))

        parent_columns = json.loads(r_server.get(str_table.format(
            cls.get_order_from_actives(parent_table, actives)
        )))['columns']

        child_columns = json.loads(r_server.get(str_table.format(
            cls.get_order_from_actives(child_table, actives)
        )))['columns']

        # exist_joins = json.loads(r_server.get(str_joins))
        # parent_joins = exist_joins[parent_table]
        # child_joins = [x for x in parent_joins if x['right']['table'] == child_table]

        return {
            child_table: [x['name'] for x in child_columns],
            parent_table: [x['name'] for x in parent_columns],
            # 'without_bind': False,
            # 'joins': child_joins,
        }

    @classmethod
    def get_final_info(cls, ordered_nodes, source, last=None):
        """
        инфа дерева для отрисовки на фронте
        :param ordered_nodes:
        :param source:
        :param last:
        :return:
        """
        result = []
        str_table = RedisCacheKeys.get_active_table(source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(source.user_id, source.id)
        actives = json.loads(r_server.get(str_active_tables))
        db = source.db
        host = source.host

        for ind, node in enumerate(ordered_nodes):
            n_val = node.val
            n_info = {'tname': n_val, 'db': db, 'host': host,
                      'dest': getattr(node.parent, 'val', None),
                      'is_root': not ind, 'without_bind': False,
                      }
            order = cls.get_order_from_actives(n_val, actives)
            table_info = json.loads(r_server.get(str_table.format(order)))
            n_info['cols'] = [x['name'] for x in table_info['columns']]
            result.append(n_info)

        if last:
            table_info = json.loads(r_server.get(str_table_by_name.format(last)))
            l_info = {'tname': last, 'db': db, 'host': host,
                      'dest': n_val, 'without_bind': True,
                      'cols': [x['name'] for x in table_info['columns']]
                      }
            result.append(l_info)
        return result

    @classmethod
    def insert_columns_info(cls, source, tables, columns,
                            indexes, foreigns, stats):
        """
        инфа о колонках, констраинтах, индексах в редис
        :param source:
        :param tables:
        :param columns:
        :param indexes:
        :param foreigns:
        :param stats:
        :return:
        """
        user_id = source.user_id
        source_id = source.id

        str_table = RedisCacheKeys.get_active_table(user_id, source_id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table(
            user_id, source_id, '{0}')

        pipe = r_server.pipeline()

        for t_name in tables:
            pipe.set(str_table_by_name.format(t_name), json.dumps(
                {
                    "columns": columns[t_name],
                    "indexes": indexes[t_name],
                    "foreigns": foreigns[t_name],
                    "stats": stats[t_name],
                }
            ))
            pipe.expire(str_table.format(t_name), settings.REDIS_EXPIRE)
        pipe.execute()

    @classmethod
    def info_for_tree_building(cls, ordered_nodes, tables, source):
        """
        инфа для построения дерева
        :param ordered_nodes:
        :param tables:
        :param source:
        :return:
        """
        user_id = source.user_id
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            user_id, source.id, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            user_id, source.id)
        actives = json.loads(r_server.get(str_active_tables))

        final_info = {}

        # инфа таблиц из существующего дерева
        for child in ordered_nodes:
            ch_val = child.val
            order = [x for x in actives if x['name'] == ch_val][0]['order']
            final_info[ch_val] = json.loads(r_server.get(str_table.format(order)))
        # инфа таблиц не из дерева
        for t_name in tables:
            if r_server.exists(str_table_by_name.format(t_name)):
                final_info[t_name] = json.loads(
                    r_server.get(str_table_by_name.format(t_name)))

        return final_info

    @classmethod
    def get_order_from_actives(cls, t_name, actives):
        """
        возвращает порядковый номер таблицы по имени
        :param t_name:
        :param actives:
        :return: list
        """
        processed = [x for x in actives if x['name'] == t_name]
        return processed[0]['order'] if processed else None

    @classmethod
    def tables_info_for_metasource(cls, source, tables):
        """
        Достает инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta
        :param source: Datasource
        :param columns: list список вида [{'table': 'name', 'col': 'name'}]
        """

        tables_info_for_meta = {}

        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        actives_list = json.loads(r_server.get(str_active_tables))

        for table in tables:
            tables_info_for_meta[table] = json.loads(
                r_server.get(str_table.format(
                    RedisSourceService.get_order_from_actives(
                        table, actives_list)
                )))
        return tables_info_for_meta

    @classmethod
    def get_next_task_counter(cls):
        """
        порядковый номер задачи
        :return:
        """
        counter = RedisCacheKeys.get_task_counter()
        if not r_server.exists(counter):
            r_server.set(counter, 1)
            return 1
        return r_server.incr(counter)

    @staticmethod
    def add_user_task(user_id, task_id, data):
        key = RedisCacheKeys.get_user_task_list(user_id)
        storage = RedisStorage(r_server)
        storage.set_dict(key, task_id, data)

    @staticmethod
    def get_user_tasks(user_id):
        """
        список задач юзера
        :param user_id:
        :return:
        """
        key = RedisCacheKeys.get_user_task_list(user_id)
        storage = RedisStorage(r_server)
        tasks = storage.get_dict(key)
        return tasks

    @classmethod
    def get_user_task_ids(cls, user_id):
        """
        список id задач юзера
        :param user_id:
        :return:
        """
        tasks = cls.get_user_tasks(user_id)
        return list(tasks)

    @classmethod
    def get_user_database_task_ids(cls, user_id, status_id):
        """
        список id тасков для постгреса, которые в данный момент в обработке
        :param user_id:
        :return:
        """
        tasks_ids = []
        tasks = cls.get_user_tasks(user_id)

        for (task_id, task_info) in tasks.iteritems():
            if (task_info['name'] == 'etl:load_data:database' and
                    task_info['status_id'] == status_id):
                tasks_ids.append(task_id)

        return tasks_ids

    @staticmethod
    def get_user_task_by_id(user_id, task_id):
        """Получение пользовательской задачи"""
        key = RedisCacheKeys.get_user_task_list(user_id)
        storage = RedisStorage(r_server)
        tasks = storage.get_dict(key)
        if task_id not in tasks:
            return None
        return tasks[task_id]

    @classmethod
    def update_task_status(cls, user_id, task_id, status_id,
                           error_code=None, error_msg=None):
        """
            Меняем статусы тасков
        """
        tasks = cls.get_user_tasks(user_id)
        task_dict = tasks[task_id]
        task_dict['status_id'] = status_id

        if status_id == TaskStatusEnum.ERROR:
            task_dict['error'] = {
                'code': error_code,
                'message': error_msg,
            }

        tasks[task_id] = task_dict

    @classmethod
    def get_next_user_collection_counter(cls, user_id, source_id):
        """
        порядковый номер коллекции юзера
        :param user_id: int
        :param source_id: int
        :return: int
        """
        counter = RedisCacheKeys.get_user_collection_counter(user_id, source_id)
        if not r_server.exists(counter):
            r_server.set(counter, 1)
            return 1
        return r_server.incr(counter)

    @classmethod
    def get_active_list(cls, user_id, source_id):
        """
        Возвращает список коллекций юзера
        :param user_id: int
        :param source_id: int
        :return:
        """
        str_active_tables = RedisCacheKeys.get_active_tables(
            user_id, source_id)
        if not r_server.exists(str_active_tables):
            r_server.set(str_active_tables, '[]')
            r_server.expire(str_active_tables, settings.REDIS_EXPIRE)
            return []
        else:
            return json.loads(r_server.get(str_active_tables))

    @classmethod
    def save_good_error_joins(cls, source, left_table, right_table,
                              good_joins, error_joins, join_type):
        """
        Сохраняет временные ошибочные и нормальные джойны таблиц
        :param source: Datasource
        :param joins: list
        :param error_joins: list
        """
        str_joins = RedisCacheKeys.get_source_joins(source.user_id, source.id)
        r_joins = json.loads(r_server.get(str_joins))

        if left_table in r_joins:
            # старые связи таблицы папы
            old_left_joins = r_joins[left_table]
            # меняем связи с right_table, а остальное оставляем
            r_joins[left_table] = [j for j in old_left_joins
                                   if j['right']['table'] != right_table]
        else:
            r_joins[left_table] = []

        for j in good_joins:
            l_c, j_val, r_c = j
            r_joins[left_table].append(
                {
                    'left': {'table': left_table, 'column': l_c},
                    'right': {'table': right_table, 'column': r_c},
                    'join': {'type': join_type, 'value': j_val},
                }
            )

        if error_joins:
            for j in error_joins:
                l_c, j_val, r_c = j
                r_joins[left_table].append(
                    {
                        'left': {'table': left_table, 'column': l_c},
                        'right': {'table': right_table, 'column': r_c},
                        'join': {'type': join_type, 'value': j_val},
                        'error': 'types mismatch'
                    }
                )
        r_server.set(str_joins, json.dumps(r_joins))

        return {'has_error_joins': bool(error_joins), }

    @classmethod
    def get_good_error_joins(cls, source, parent_table, child_table):

        r_joins = cls.get_source_joins(source.user_id, source.id)

        good_joins = []
        error_joins = []

        # если 2 таблицы выбраны без связей, то r_joins пустой,
        # если биндим последнюю таблицу без связи,то parent_table not in r_joins
        if r_joins and parent_table in r_joins:
            par_joins = r_joins[parent_table]
            good_joins = [
                j for j in par_joins if j['right']['table'] == child_table
                and 'error' not in j]

            error_joins = [
                j for j in par_joins if j['right']['table'] == child_table
                and 'error' in j and j['error'] == 'types mismatch']

        return good_joins, error_joins

    @classmethod
    def get_source_joins(cls, user_id, source_id):
        str_joins = RedisCacheKeys.get_source_joins(user_id, source_id)
        return json.loads(r_server.get(str_joins))

    @classmethod
    def get_last_remain(cls, user_id, source_id):
        tables_remain_key = RedisCacheKeys.get_source_remain(
            user_id, source_id)
        return (r_server.get(tables_remain_key)
                if r_server.exists(tables_remain_key) else None)


class DataSourceService(object):
    """
        Сервис управляет сервисами БД и Редиса
    """
    @classmethod
    def delete_datasource(cls, source):
        """ удаляет информацию о датасосре
        """
        RedisSourceService.delete_datasource(source)

    @classmethod
    def tree_full_clean(cls, source):
        """ удаляет информацию о таблицах, джоинах, дереве
        """
        RedisSourceService.tree_full_clean(source)

    @staticmethod
    def get_database_info(source):
        """ Возвращает таблицы истоника данных
        :type source: Datasource
        """
        tables = DatabaseService.get_tables(source)

        if settings.USE_REDIS_CACHE:
            return RedisSourceService.get_tables(source, tables)
        else:
            return {
                "db": source.db,
                "host": source.host,
                "tables": tables
            }

    @staticmethod
    def check_connection(post):
        """ Проверяет подключение
        """
        conn_info = {
            'host': get_utf8_string(post.get('host')),
            'login': get_utf8_string(post.get('login')),
            'password': get_utf8_string(post.get('password')),
            'db': get_utf8_string(post.get('db')),
            'port': get_utf8_string(post.get('port')),
            'conn_type': get_utf8_string(post.get('conn_type')),
        }

        return DatabaseService.get_connection_by_dict(conn_info)

    @classmethod
    def get_columns_info(cls, source, tables):

        """
        Получение информации по колонкам
        :param source: Datasource
        :param tables:
        :return:
        """
        col_records, index_records, const_records = (
            DatabaseService.get_columns_info(source, tables))

        stat_records = DatabaseService.get_stats_info(source, tables)

        cols, indexes, foreigns = DatabaseService.processing_records(
            source, col_records, index_records, const_records)

        if settings.USE_REDIS_CACHE:
            RedisSourceService.insert_columns_info(
                source, tables, cols, indexes, foreigns, stat_records)

            # выбранные ранее таблицы в редисе
            active_tables = RedisSourceService.get_active_list(
                source.user_id, source.id)

            # работа с деревьями
            if not active_tables:
                trees, without_bind = TableTreeRepository.build_trees(tuple(tables), source)
                sel_tree = TablesTree.select_tree(trees)

                remains = without_bind[sel_tree.root.val]
            else:
                # достаем структуру дерева из редиса
                structure = RedisSourceService.get_active_tree_structure(source)
                # строим дерево
                sel_tree = TablesTree.build_tree_by_structure(structure)

                ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])

                tables_info = RedisSourceService.info_for_tree_building(
                    ordered_nodes, tables, source)

                # перестраиваем дерево
                remains = TablesTree.build_tree(
                    [sel_tree.root, ], tuple(tables), tables_info)

            # таблица без связи
            last = RedisSourceService.insert_remains(source, remains)

            # сохраняем дерево
            structure = TablesTree.get_tree_structure(sel_tree.root)
            ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])
            RedisSourceService.insert_tree(structure, ordered_nodes, source)

            # возвращаем результат
            return RedisSourceService.get_final_info(ordered_nodes, source, last)

        return []

    @classmethod
    def get_rows_info(cls, source, cols):
        """
        Получение списка значений указанных колонок и таблиц в выбранном источнике данных

        :param source: Datasource
        :param cols: list
        :return: list
        """
        structure = RedisSourceService.get_active_tree_structure(source)
        return DatabaseService.get_rows(source, cols, structure)

    @classmethod
    def remove_tables_from_tree(cls, source, tables):
        """
        удаление таблиц из дерева
        :param source:
        :param tables:
        """
        # достаем структуру дерева из редиса
        structure = RedisSourceService.get_active_tree_structure(source)
        # строим дерево
        sel_tree = TablesTree.build_tree_by_structure(structure)
        TableTreeRepository.delete_nodes_from_tree(sel_tree, source, tables)

        if sel_tree.root:
            RedisSourceService.delete_tables(source, tables)

            ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])
            structure = TablesTree.get_tree_structure(sel_tree.root)
            RedisSourceService.insert_tree(structure, ordered_nodes, source, update_joins=False)

    @classmethod
    def check_is_binding_remain(cls, source, child_table):
        remain = RedisSourceService.get_last_remain(
            source.user_id, source.id)
        return remain == child_table

    @classmethod
    def get_columns_and_joins_for_join_window(
            cls, source, parent_table, child_table, has_warning):
        """
        список колонок и джойнов таблиц для окнв связей таблиц
        :param source:
        :param parent_table:
        :param child_table:
        :param has_warning:
        :return:
        """

        is_binding_remain = cls.check_is_binding_remain(
                source, child_table)

        # если связываем проблемных и один из них последний(remain)
        if has_warning and is_binding_remain:
                columns = RedisSourceService.get_columns_for_tables_without_bind(
                    source, parent_table, child_table)
                good_joins, error_joins = RedisSourceService.get_good_error_joins(
                    source, parent_table, child_table)

        # если связываем непроблемных или таблицы в дереве,
        # имеющие неправильные связи
        else:
            columns = RedisSourceService.get_columns_for_tables_with_bind(
                source, parent_table, child_table)
            good_joins, error_joins = RedisSourceService.get_good_error_joins(
                    source, parent_table, child_table)

        result = {'columns': columns,
                  'good_joins': good_joins,
                  'error_joins': error_joins,
                  }
        return result

    @classmethod
    def check_new_joins(cls, source, left_table, right_table, joins):
        # избавление от дублей
        """
        проверяет пришедшие джойны на совпадение типов
        :param source:
        :param left_table:
        :param right_table:
        :param joins:
        :return:
        """
        joins_set = set()
        for j in joins:
            joins_set.add(tuple(j))

        cols_types = cls.get_columns_types(source, [left_table, right_table])

        # список джойнов с неверными типами
        error_joins = list()
        good_joins = list()

        for j in joins_set:
            l_c, j_val, r_c = j
            if (cols_types['{0}.{1}'.format(left_table, l_c)] !=
                    cols_types['{0}.{1}'.format(right_table, r_c)]):
                error_joins.append(j)
            else:
                good_joins.append(j)

        return good_joins, error_joins, joins_set

    @classmethod
    def save_new_joins(cls, source, left_table, right_table, join_type, joins):
        """
        сохранение новых джойнов
        :param source:
        :param left_table:
        :param right_table:
        :param join_type:
        :param joins:
        :return:
        """
        # joins_set избавляет от дублей
        good_joins, error_joins, joins_set = cls.check_new_joins(
            source, left_table, right_table, joins)

        data = RedisSourceService.save_good_error_joins(
            source, left_table, right_table,
            good_joins, error_joins, join_type)

        if not error_joins:
            # достаем структуру дерева из редиса
            structure = RedisSourceService.get_active_tree_structure(source)
            # строим дерево
            sel_tree = TablesTree.build_tree_by_structure(structure)

            TablesTree.update_node_joins(
                sel_tree, left_table, right_table, join_type, joins_set)

            # сохраняем дерево
            ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])
            structure = TablesTree.get_tree_structure(sel_tree.root)
            RedisSourceService.insert_tree(
                structure, ordered_nodes, source, update_joins=False)

            # если совсем нет ошибок ни у кого, то на клиенте перерисуем дерево,
            # на всякий пожарный
            data['draw_table'] = RedisSourceService.get_final_info(
                ordered_nodes, source)

            # работа с последней таблицей
            remain = RedisSourceService.get_last_remain(
                source.user_id, source.id)
            if remain == right_table:
                # удаляем инфу о таблице без связи, если она есть
                RedisSourceService.delete_last_remain(source)

        return data

    @classmethod
    def get_columns_types(cls, source, tables):
        """
        типы колонок таблиц
        :param source:
        :param tables:
        :return:
        """
        types_dict = {}

        for table in tables:
            t_cols = json.loads(
                RedisSourceService.get_table_full_info(source, table))['columns']
            for col in t_cols:
                types_dict['{0}.{1}'.format(table, col['name'])] = col['type']

        return types_dict

    @classmethod
    def get_separator(cls, source):
        return DatabaseService.get_separator(source)

    @classmethod
    def get_rows_query_for_loading_task(cls, source, structure, cols):
        """
        Получение предзапроса данных указанных
        колонок и таблиц для селери задачи
        :param source:
        :param structure:
        :param cols:
        :return:
        """

        separator = cls.get_separator(source)
        query_join = DatabaseService.get_generated_joins(source, structure)

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)
        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        rows_query = DatabaseService.get_rows_query(source).format(
            cols_str, query_join, '{0}', '{1}')
        return rows_query

    @classmethod
    def check_existing_table(cls, table_name):
        """
        проверка существования таблицы с именем при создании
        :param table_name:
        :return:
        """
        from django.db import connection
        return table_name in connection.introspection.table_names()

    @classmethod
    def get_source_connection(cls, source):
        """
        Получить объект соединения источника данных
        :type source: Datasource
        """
        return DatabaseService.get_connection(source)

    @classmethod
    def get_local_instance(cls):
        """
        возвращает инстанс локального хранилища данных(Postgresql)
        :rtype : object Postgresql()
        :return:
        """
        return DatabaseService.get_local_instance()

    @classmethod
    def tables_info_for_metasource(cls, source, tables):
        """
        Достает инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta
        :param source: Datasource
        :param columns: list список вида [{'table': 'name', 'col': 'name'}]
        """
        tables_info_for_meta = RedisSourceService.tables_info_for_metasource(
            source, tables)
        return tables_info_for_meta

    @staticmethod
    def update_datasource_meta(table_name, source, cols,
                               tables_info_for_meta, last_row):
        """
        Создание DatasourceMeta для Datasource

        Args:
            table_name(str): Название страницы
            source(Datasource): Источник данных
            cols(list): Список колонок
            last_row(str or None): Последняя запись
            tables_info_for_meta: Данные о таблицах

        Returns:
            DatasourceMeta: Объект мета-данных

        """
        try:
            source_meta = DatasourceMeta.objects.get(
                datasource_id=source.id,
                collection_name=table_name,
            )
        except DatasourceMeta.DoesNotExist:
            source_meta = DatasourceMeta(
                datasource_id=source.id,
                collection_name=table_name,
            )

        stats = {'tables_stat': {}, 'row_key': {}, 'row_key_value': defaultdict(list), }
        fields = {'columns': defaultdict(list), }

        # избавляет от дублей
        row_keys = defaultdict(set)

        for table, col_group in groupby(cols, lambda x: x['table']):
            table_info = tables_info_for_meta[table]

            stats['tables_stat'][table] = table_info['stats']
            t_cols = table_info['columns']

            for sel_col in col_group:
                for col in t_cols:
                    # cols info
                    if sel_col['col'] == col['name']:
                        fields['columns'][table].append(col)

                    # primary keys
                    if col['is_primary']:
                        row_keys[table].add(col['name'])

        for k, v in row_keys.iteritems():
            stats['row_key'][k] = list(v)

        if last_row:
            # корневая таблица
            root_table = cols[0]['table']
            mapped = filter(
                lambda x: x[0]['table'] == root_table, zip(cols, last_row))

            if stats['row_key']:
                primaries = stats['row_key'][root_table]

                for pri in primaries:
                    for (k, v) in mapped:
                        if pri == k['col']:
                            stats['row_key_value'][root_table].append(
                                {pri: v})

        # source_meta.update_date = datetime.datetime.now()
        source_meta.fields = json.dumps(fields)
        source_meta.stats = json.dumps(stats)
        source_meta.save()
        return source_meta

    @classmethod
    def get_structure_rows_number(cls, source, structure,  cols):
        """
        возвращает примерное кол-во строк в запросе селекта для планирования
        :param source:
        :param structure:
        :param cols:
        :return:
        """
        return DatabaseService.get_structure_rows_number(
            source, structure,  cols)


class TaskErrorCodeEnum(BaseEnum):
    """
        Коды ошибок тасков пользователя
    """
    DEFAULT_CODE = '1050'

    values = {
        DEFAULT_CODE: '1050',
    }


class TaskStatusEnum(BaseEnum):
    """
        Статусы тасков пользователя
    """
    IDLE, PROCESSING, ERROR, DONE, DELETED = ('idle', 'processing', 'error',
                                              'done', 'deleted', )
    values = {
        IDLE: " В ожидании",
        PROCESSING: "В обработке",
        ERROR: "Ошибка",
        DONE: "Выполнено",
        DELETED: "Удалено",
    }


class TaskService:
    """
    Добавление новых задач в очередь
    Управление пользовательскими задачами
    """
    def __init__(self, name):
        self.name = name

    def add_task(self, user_id, data, tree, source_dict):
        """
        Добавляем задачу юзеру в список задач и возвращаем идентификатор заадчи
        :type tree: dict дерево источника
        :param user_id: integer
        :param data: dict
        :param source_dict: dict
        :return: integer
        """
        task_id = RedisSourceService.get_next_task_counter()
        task = {'name': self.name,
                'data': {
                    'cols': data['cols'], 'tables': data['tables'],
                    'tree': tree, 'col_types': data['col_types'],
                    'meta_info': data['meta_info'],
                },
                'source': source_dict,
                'status_id': TaskStatusEnum.IDLE,
                }

        RedisSourceService.add_user_task(user_id, task_id, task)
        return task_id

    @classmethod
    def table_create_query_for_loading_task(
            cls, local_instance, table_key, cols_str):
        """
            Получение запроса на создание новой таблицы
            для локального хранилища данных
        """
        create_query = DatabaseService.get_table_create_query(
            local_instance, table_key, cols_str)
        return create_query

    @classmethod
    def table_insert_query_for_loading_task(cls, local_instance, table_key):
        """
            Получение запроса на заполнение таблицы
            для локального хранилища данных
        """
        insert_query = DatabaseService.get_table_insert_query(
            local_instance, table_key)
        return insert_query


class RedisStorage:
    """
    Обертка над методами сохранения информации в redis
    Позволяет работать с объектами в python стиле, при этом информация сохраняется в redis
    Пока поддерживаются словари
    """
    def __init__(self, client):
        self.client = client

    def set_dict(self, redis_key, key, value):
        tasks = RedisDict(key=redis_key, redis=self.client, pickler=json)
        tasks[key] = value

    def get_dict(self, key):
        tasks = RedisDict(key=key, redis=self.client, pickler=json)
        return tasks


class EtlEncoder:
    @staticmethod
    def encode(obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%d.%m.%Y')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%d.%m.%Y')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return obj


def generate_table_name_key(source, cols_str):
    """Генерация ключа для названия промежуточной таблицы

    Args:
        source(Datasource): источник
        cols_str(str): Строка с названием столбцов

    Returns:
        str: Ключ для названия промежуточной таблицы

    """
    return binascii.crc32(
        reduce(operator.add,
               [source.host, str(source.port),
                str(source.user_id), cols_str], ''))
