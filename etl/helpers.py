# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb
import json

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


def get_db_info(user_id, source):

    result = []

    if settings.USE_REDIS_CACHE:
        user_db_key = RedisCacheKeys.get_user_databases(user_id)
        user_datasource_key = RedisCacheKeys.get_user_datasource(user_id, source.id)

        if not r_server.exists(user_datasource_key):
            conn_info = {
                'host': get_utf8_string(source.host),
                'user': get_utf8_string(source.login or ''),
                'passwd': get_utf8_string(source.password or ''),
                'db': get_utf8_string(source.db),
                'port': source.port,
                'conn_type': source.conn_type
            }

            conn = DataSourceService.get_connection(conn_info)

            if not conn:
                raise ValueError("Сбой при подключении!")
            else:
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

        for el in r_server.lrange(user_db_key, 0, -1):
            el_key = RedisCacheKeys.get_user_datasource(user_id, el)
            if not r_server.exists(el_key):
                r_server.lrem(user_db_key, 1, el)
            else:
                result.append(json.loads(r_server.get(el_key)))

    return result


class Postgresql(object):
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
            SELECT table_name FROM information_schema.tables where table_schema='public';
        """
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()

        records = map(lambda x: {'name': x[0], 'columns': []}, records)

        return records


class Mysql(object):
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
            SELECT table_name FROM information_schema.tables where table_schema='{0}';
        """.format(source.db)
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()

        records = map(lambda x: {'name': x[0], 'columns': []}, records)

        return records


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
    def get_connection(conn_info):

        instance = DataSourceConnectionFactory.factory(
            int(conn_info.get('conn_type', '')))

        del conn_info['conn_type']
        conn_info['port'] = int(conn_info['port'])

        conn = instance.get_connection(conn_info)
        return conn


class RedisCacheKeys(object):
    """Ключи для редиса"""
    @staticmethod
    def get_user_databases(user_id):
        return '{0}_user_dbs'.format(user_id)

    @staticmethod
    def get_user_datasource(user_id, datasource_id):
        return 'source_{0}_{1}'.format(user_id, datasource_id)
