# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb
import json
import decimal
from itertools import groupby

from django.conf import settings

from core.models import ConnectionChoices
from . import r_server


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
    @staticmethod
    def get_db_info(user_id, source):

        if settings.USE_REDIS_CACHE:
            user_db_key = RedisCacheKeys.get_user_databases(user_id)
            user_datasource_key = RedisCacheKeys.get_user_datasource(user_id, source.id)

            if not r_server.exists(user_datasource_key):
                conn_info = source.get_connection_dict()
                conn = DataSourceService.get_connection(conn_info)
                tables = DataSourceService.get_tables(source, conn)

                new_db = {
                    "db": source.db,
                    "host": source.host,
                    "tables": tables
                }
                if str(source.id) not in r_server.lrange(user_db_key, 0, -1):
                    r_server.rpush(user_db_key, source.id)
                r_server.set(user_datasource_key, json.dumps(new_db))
                r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)

            return json.loads(r_server.get(user_datasource_key))

        else:
            conn_info = source.get_connection_dict()
            conn = DataSourceService.get_connection(conn_info)
            tables = DataSourceService.get_tables(source, conn)

            return {
                "db": source.db,
                "host": source.host,
                "tables": tables
            }

    @staticmethod
    def check_connection(post):
        """
        Проверка соединения источников данных
        :param data: dict
        :return: connection obj or None
        """
        conn_info = {
            'host': get_utf8_string(post.get('host')),
            'user': get_utf8_string(post.get('login')),
            'passwd': get_utf8_string(post.get('password')),
            'db': get_utf8_string(post.get('db')),
            'port': get_utf8_string(post.get('port')),
            'conn_type': get_utf8_string(post.get('conn_type')),
        }

        return DataSourceService.get_connection(conn_info)

    @staticmethod
    def get_columns_info(source, tables):
        conn_info = source.get_connection_dict()
        conn = DataSourceService.get_connection(conn_info)

        return DataSourceService.get_columns(source, tables, conn)

    @staticmethod
    def get_rows_info(source, tables, cols):
        conn_info = source.get_connection_dict()
        conn = DataSourceService.get_connection(conn_info)

        return DataSourceService.get_rows(source, conn, tables, cols)

    @staticmethod
    def get_query_result(query, conn):
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def _get_columns_query(source, tables):
        raise ValueError("Columns query is not realized")

    @classmethod
    def get_columns(cls, source, tables, conn):

        query = cls._get_columns_query(source, tables)

        records = Database.get_query_result(query, conn)

        result = []
        for key, group in groupby(records, lambda x: x[0]):
            result.append({
                "tname": key, 'display': key[:21], 'db': source.db, 'host': source.host,
                "cols": [x[1] for x in group]
            })
        return result

    @staticmethod
    def get_rows(conn, tables, cols):
        query = """
            SELECT {0} FROM {1} LIMIT {2};
        """.format(', '.join(cols), ', '.join(tables), settings.ETL_COLLECTION_PREVIEW_LIMIT)
        records = Database.get_query_result(query, conn)
        return records


class Postgresql(Database):
    """Управление источником данных Postgres"""
    @staticmethod
    def get_connection(conn_info):
        try:
            conn_str = (u"host='{host}' dbname='{db}' user='{user}' "
                        u"password='{passwd}' port={port}").format(**conn_info)
            conn = psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            return None
        return conn

    @staticmethod
    def get_tables(source, conn):
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='public' order by table_name;
        """
        records = Postgresql.get_query_result(query, conn)
        records = map(lambda x: {'name': x[0], },
                      sorted(records, key=lambda y: y[0]))

        return records

    @staticmethod
    def _get_columns_query(source, tables):
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        # public - default scheme for postgres
        query = """
            SELECT table_name, column_name FROM information_schema.columns
            where table_name in {0} and table_catalog = '{1}' and table_schema = '{2}';
        """.format(tables_str, source.db, 'public')
        return query


class Mysql(Database):
    """Управление источником данных MySQL"""
    @staticmethod
    def get_connection(conn_info):
        try:
            conn = MySQLdb.connect(**conn_info)
        except MySQLdb.OperationalError:
            return None
        return conn

    @staticmethod
    def get_tables(source, conn):
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='{0}' order by table_name;
        """.format(source.db)

        records = Mysql.get_query_result(query, conn)
        records = map(lambda x: {'name': x[0], }, records)

        return records

    @staticmethod
    def _get_columns_query(source, tables):
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        query = """
            SELECT table_name, column_name FROM information_schema.columns
            where table_name in {0} and table_schema = '{1}';
        """.format(tables_str, source.db)
        return query


class DataSourceConnectionFactory(object):
    """Фабрика для подключения к источникам данных"""
    @staticmethod
    def factory(conn_type):
        if conn_type == ConnectionChoices.POSTGRESQL:
            return Postgresql()
        elif conn_type == ConnectionChoices.MYSQL:
            return Mysql()
        else:
            raise ValueError("Неизвестный тип подключения!")


class DataSourceService(object):
    """Сервис для источников данных"""
    @staticmethod
    def get_tables(source, conn):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_tables(source, conn)

    @staticmethod
    def get_columns(source, tables, conn):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_columns(source, tables, conn)

    @staticmethod
    def get_rows(source, conn, tables, cols):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_rows(conn, tables, cols)

    @staticmethod
    def get_connection(conn_info):

        instance = DataSourceConnectionFactory.factory(
            int(conn_info.get('conn_type', '')))

        del conn_info['conn_type']
        conn_info['port'] = int(conn_info['port'])

        conn = instance.get_connection(conn_info)
        if conn is None:
            raise ValueError("Сбой при подключении!")
        return conn


class RedisCacheKeys(object):
    """Ключи для редиса"""
    @staticmethod
    def get_user_databases(user_id):
        return '{0}_user_dbs'.format(user_id)

    @staticmethod
    def get_user_datasource(user_id, datasource_id):
        return 'source_{0}_{1}'.format(user_id, datasource_id)


# class DecimalEncoder(object):
#     @staticmethod
#     def encode(data):
#         """Преобразуем decimal в строку, чтобы передать ответ клиенту"""
#         res = []
#         for row in data:
#             row = list(row)
#             for k, obj in enumerate(row):
#                 if isinstance(obj, decimal.Decimal):
#                     row[k] = float(obj)
#             res.append(row)
#         return res
