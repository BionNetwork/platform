# coding: utf-8

import ntpath
from etl.services.datasource.source import SourceConvertTypes as SCT


class File(object):
    """
    Базовый класс для источников файлового типа
    """

    # соответствие типов pandas-a и postgres
    TYPES_MAP = {
        "int": SCT.INT,
        "float": SCT.DOUBLE,
        "datetime": SCT.DATE,
        "object": SCT.TEXT,
        "boolean": SCT.BOOL,
    }

    def __init__(self, source):
        """
        Присваиваем источник
        """
        self.source = source

    def process_type(self, type_):
        """
        FIXME Возможно надо для Mongo, а не для Postgresql
        Отображение типов Файла в типы Postgresql
        """
        for k, v in list(self.TYPES_MAP.items()):
            if type_.startswith(k):
                return v
        raise ValueError("Необработанный тип для Файла!")

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
