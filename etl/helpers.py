# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb

from core.models import ConnectionChoices


def get_utf8_string(value):
    """
    Кодирование в utf-8 строки
    :param value: string
    :return: string
    """
    return unicode(value)


def check_connection(data):
    """
    Проверка соединения источников данных
    :param data: dict
    :return: bool
    """
    conn_info = {
        'host': get_utf8_string(data.get('host', '')),
        'user': get_utf8_string(data.get('login', '')),
        'passwd': get_utf8_string(data.get('password', '')),
        'db': get_utf8_string(data.get('name', '')),
        'port': int(data.get('port', ''))
    }

    conn_type = int(data.get('conn_type', ''))

    if conn_type == ConnectionChoices.POSTGRESQL:
        try:
            conn_str = (u"host='{host}' dbname='{db}' user='{user}' "
                        u"password='{passwd}' port={port}").format(**conn_info)
            psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            return False
    elif conn_type == ConnectionChoices.MYSQL:
        try:
            MySQLdb.connect(**conn_info)
        except MySQLdb.OperationalError:
            return False
    else:
        raise ValueError("Неизвестный тип подключения")

    return True
