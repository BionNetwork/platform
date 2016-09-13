# coding: utf-8

from collections import defaultdict
import decimal
import datetime


def group_by_source(columns_info):
    """
    Группировка по соурсам, на всякий пожарный перед загрузкой
    """
    sid_grouped = defaultdict(dict)

    for sid, tables in columns_info.items():
        sid_grouped[str(sid)].update(tables)

    return dict(sid_grouped)


def extract_tables_info(columns):

    tables_dict = {}

    for sid, tables in columns.items():
        tables_dict[sid] = list(tables.keys())
    return tables_dict


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


class HashEncoder(object):
    """
    Базовый класс для хэширования данных
    """

    @staticmethod
    def encode(data):
        """
        Кодирование данных
        Args:
            data(object): list, dict, str данные для кодирования
        Returns:
            object(int): integer представление
        """
        return hash(data)


def datetime_now_str():
    """
    Нынешнее время в строковой форме
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
