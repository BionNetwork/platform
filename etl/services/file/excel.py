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

    def get_tables(self, source):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        file_path = source.file.path
        excel = xlrd.open_workbook(file_path)

        sheet_names = excel.sheet_names()
        return map(lambda x: {'name': x, }, sheet_names)
