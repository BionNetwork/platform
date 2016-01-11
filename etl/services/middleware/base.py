# coding: utf-8

import decimal
import binascii
import datetime
import operator


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


def generate_table_name_key(source, cols_str):
    """Генерация ключа для названия промежуточной таблицы

    Args:
        source(Datasource): источник
        cols_str(str): Строка с названием столбцов

    Returns:
        str: Ключ для названия промежуточной таблицы

    """
    key = binascii.crc32(
        reduce(operator.add,
               [source.host, str(source.port),
                str(source.user_id), cols_str], ''))
    return str(key) if key > 0 else '_{0}'.format(abs(key))


def get_table_name(prefix, key):
    """
    название новой таблицы

    Args:
        prefix(str): префикс перед ключем
        key(str): ключ
    Returns:
        str: Название новой наблицы
    """
    return '{0}_{1}'.format(
        prefix, key)


def datetime_now_str():
    """
    Нынешнее время в строковой форме
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
