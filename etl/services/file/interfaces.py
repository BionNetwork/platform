# coding: utf-8
from __future__ import unicode_literals


class File(object):
    """
    Базовый класс для источников файлового типа
    """

    def __init__(self, source):
        """
        Присваиваем источник
        """
        self.source = source

    def get_tables(self):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        raise NotImplementedError
