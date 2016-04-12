# coding: utf-8
from __future__ import unicode_literals

import xlrd

from django.conf import settings

from etl.services.file.interfaces import File


MEDIA_ROOT = settings.MEDIA_ROOT


class Excel(File):
    """
    Класс для работы с Excel файлами
    """

    def get_tables(self):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        file_path = self.source.file.path
        excel = xlrd.open_workbook(file_path)

        sheet_names = excel.sheet_names()
        return map(lambda x: {'name': x, }, sheet_names)

    def get_columns_info(self, source, tables):
        """
        Получение списка колонок в таблицах

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            Кортеж из списков, в первом списке возвращаются колонки таблиц
            вида [(table_name, col_name, type, length), ]
        """
