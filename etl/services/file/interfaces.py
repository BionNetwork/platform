# coding: utf-8
from __future__ import unicode_literals

import ntpath

# соответствие типов pandas-a и postgres

TIMESTAMP = "timestamp"

TYPES_MAP = {
    "int": "integer",
    "float": "double precision",
    "datetime": TIMESTAMP,
    "object": "text",
    "boolean": "boolean"
}


def process_type(type_):
    """
    FIXME Возможно надо для Mongo, а не для Postgresql
    Отображение типов Файла в типы Postgresql
    """
    for k, v in TYPES_MAP.items():
        if type_.startswith(k):
            return v
    raise ValueError("Необработанный тип для Excel!")


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

    @property
    def file_name(self):
        """
        Ссылка на истодный файл
        """
        return ntpath.basename(self.source.file.name)

    @property
    def file_path(self):
        """
        Ссылка на истодный файл
        """
        return self.source.get_file_path()
