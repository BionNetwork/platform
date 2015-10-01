# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb
import json
from itertools import groupby
from collections import defaultdict

from django.conf import settings

from core.models import ConnectionChoices
from . import r_server
from .maps import postgresql as psql_map


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
    def get_columns_info(source, user, tables):
        conn_info = source.get_connection_dict()
        conn = DataSourceService.get_connection(conn_info)

        return DataSourceService.get_columns(source, user, tables, conn)

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
    def get_columns(cls, source, user, tables, conn):

        query = cls._get_columns_query(source, tables)

        records = Database.get_query_result(query, conn)

        result = []
        for key, group in groupby(records, lambda x: x[0]):
            result.append({
                "tname": key, 'db': source.db, 'host': source.host,
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
        cols_query = psql_map.cols_query.format(tables_str, source.db, 'public')

        constraints_query = psql_map.constraints_query.format(tables_str)

        indexes_query = psql_map.indexes_query.format(tables_str)

        return cols_query, constraints_query, indexes_query

    @classmethod
    def get_columns(cls, source, user, tables, conn):

        max_ = 0
        active_tables = []
        exist_result = []
        new_result = []

        str_table = RedisCacheKeys.get_user_source_table(source.id, user.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(source.id, user.id)
        r_max = RedisCacheKeys.get_last_active_table(source.id, user.id)

        if settings.USE_REDIS_CACHE:
            # выбранные ранее таблицы в редисе
            if not r_server.exists(str_active_tables):
                r_server.set(str_active_tables, '[]')
                r_server.expire(str_active_tables, settings.REDIS_EXPIRE)
            else:
                active_tables = json.loads(r_server.get(str_active_tables))

            ex_tables = [x for x in tables if r_server.exists(str_table.format(x))]
            tables = [x for x in tables if x not in ex_tables]

            # наибольшее активное число таблицы
            if not r_server.exists(r_max):
                r_server.set(r_max, '{"max": 0}')
                r_server.expire(r_max, settings.REDIS_EXPIRE)
            else:
                max_ = json.loads(r_server.get(r_max))["max"]

            for ex_t in ex_tables:
                ext_info = json.loads(r_server.get(str_table.format(ex_t)))
                table = {"tname": ex_t, 'db': source.db, 'host': source.host,
                         'cols': [x["name"] for x in ext_info['columns']], }
                exist_result.append(table)

        if tables:
            columns_query, consts_query, indexes_query = cls._get_columns_query(source, tables)

            col_records = Database.get_query_result(columns_query, conn)

            for key, group in groupby(col_records, lambda x: x[0]):
                new_result.append({"tname": key, 'db': source.db, 'host': source.host,
                                   'cols': [x[1] for x in group]})

            if settings.USE_REDIS_CACHE:
                index_records = Database.get_query_result(indexes_query, conn)
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

                const_records = Database.get_query_result(consts_query, conn)
                constraints = {}
                (c_table_name, c_col_name, c_name, c_type,
                 c_foreign_table, c_foreign_col, c_update, c_delete) = xrange(8)

                for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
                    constraints[ikey] = []
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

                for key, group in groupby(col_records, lambda x: x[0]):

                    t_indexes = indexes[key]
                    t_consts = constraints[key]

                    for x in group:
                        is_index = is_unique = is_primary = False
                        col = x[1]

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

                        columns[key].append({"name": col, "type": psql_map.PSQL_TYPES[x[2]] or x[2],
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

                for ind, t_name in enumerate(tables, start=1):
                    active_tables.append({"name": t_name, "order": max_ + ind})
                    r_server.set(str_table.format(t_name), json.dumps(
                        {
                            "columns": columns[key],
                            "indexes": indexes[key],
                            "foreigns": foreigns[key],
                        }
                    ))
                    r_server.expire(str_table.format(t_name), settings.REDIS_EXPIRE)

                # сохраняем активные таблицы
                r_server.set(str_active_tables, json.dumps(active_tables))
                r_server.expire(str_active_tables, settings.REDIS_EXPIRE)

                # сохраняем последнюю таблицу
                r_server.set(r_max, json.dumps({"name": t_name, "max": ind}))
                r_server.expire(r_max, settings.REDIS_EXPIRE)

        return exist_result + new_result


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

    @classmethod
    def get_columns(cls, source, user, tables, conn):
        pass


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
    def get_columns(source, user, tables, conn):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_columns(source, user, tables, conn)

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

    @staticmethod
    def get_last_active_table(source_id, user_id):
        return 'source_{0}_user_{1}_collectionactive'.format(source_id, user_id)

    @staticmethod
    def get_active_tables(source_id, user_id):
        return 'source_{0}_user_{1}_active_collections'.format(source_id, user_id)

    @staticmethod
    def get_user_source_table(source_id, user_id, table):
        return 'source_{0}_user_{1}_table_{2}'.format(source_id, user_id, table)
