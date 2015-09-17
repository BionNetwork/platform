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
        user_dbs = '{0}_user_dbs'.format(user_id)
        source_str = 'source_{0}_{1}'.format(user_id, '{0}')

        if not r_server.exists(source_str.format(source.id)):
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
                    "db_name": '{0}{1}'.format(source.host, source.db),
                    "db_name2": '{0}: {1}'.format(source.host, source.db),
                    "tables": tables
                }
                if str(source.id) not in r_server.lrange(user_dbs, 0, -1):
                    r_server.rpush(user_dbs, source.id)
                r_server.set(source_str.format(source.id), json.dumps(new_db))
                r_server.expire(source_str.format(source.id), settings.REDIS_EXPIRE)

        for el in r_server.lrange(user_dbs, 0, -1):
            if not r_server.exists(source_str.format(el)):
                r_server.lrem(user_dbs, 1, el)
            else:
                result.append(json.loads(r_server.get(source_str.format(el))))

    return result


class Postgresql(object):

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

    @staticmethod
    def factory(conn_type):
        if conn_type == ConnectionChoices.POSTGRESQL:
            return Postgresql()
        elif conn_type == ConnectionChoices.MYSQL:
            return Mysql()
        else:
            raise ValueError("Неизвестный тип подключения!")


class DataSourceService(object):

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
