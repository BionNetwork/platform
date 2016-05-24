# coding: utf-8
from __future__ import unicode_literals

import decimal
import datetime
import operator

from core.helpers import HashEncoder


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


def generate_columns_string(columns):
    """
        Генерирует строку из имен таблиц и колонок для binascii.crc32
    """
    cols_str = ''
    for obj in columns:
        t = obj['table']
        c = obj['col']
        cols_str += '{0}-{1};'.format(t, c)

    return cols_str


def generate_columns_string_NEW(sources):
    """
        Генерирует строку из имен таблиц и колонок
    """
    result = []
    for sid, tables in sources.iteritems():
        for table, cols in tables.iteritems():
            cols.sort()
            result.append('{0}-{1};'.format(table, ','.join(cols)))

    result.sort()
    cols_str = ''.join(result)

    return cols_str


def extract_tables_info(columns):

    tables_dict = {}

    for sid, tables in columns.iteritems():
        tables_dict[sid] = tables.keys()
    return tables_dict


def generate_table_name_key(source, cols_str):
    """Генерация ключа для названия промежуточной таблицы

    Args:
        source(Datasource): источник
        cols_str(str): Строка с названием столбцов

    Returns:
        str: Ключ для названия промежуточной таблицы

    """
    key = HashEncoder.encode(
        reduce(operator.add,
               [source.host, str(source.port),
                str(source.user_id), cols_str], ''))
    return str(key) if key > 0 else '_{0}'.format(abs(key))


def generate_cube_key(cols_str, cube_id):
    """
    Генерация ключа для куба
    cols_str(str): Строка с названием столбцов
    """
    key = HashEncoder.encode(
        reduce(operator.add, [str(cube_id), cols_str], ''))
    return str(key) if key > 0 else '_{0}'.format(abs(key))


def get_table_name(prefix, key):
    """
    название новой таблицы

    Args:
        prefix(unicode): префикс перед ключем
        key(unicode): ключ
    Returns:
        str: Название новой наблицы
    """
    return u'{0}_{1}'.format(
        prefix, key)


def datetime_now_str():
    """
    Нынешнее время в строковой форме
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
