# coding: utf-8
from __future__ import unicode_literals


class File(object):
    """
    Базовый класс для источников файлового типа
    """

    def get_tables(self, source):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        raise NotImplementedError
