# coding: utf-8
from __future__ import unicode_literals

from core.models import ConnectionChoices

from etl.services.source import DatasourceApi
from etl.services.file.excel import Excel


class FileService(DatasourceApi):
    """Сервис для источников данных на основе файлов"""

    def __init__(self, source):
        # проверка файла источника
        if not source.file:
            raise ValueError("Отсутствует файл источника!")

        super(FileService, self).__init__(source)

    @staticmethod
    def factory(conn_type):
        """
        Фабрика для инстанса файлов

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            etl.services.files.interfaces.File
        """

        if conn_type == ConnectionChoices.EXCEL:
            return Excel()
        elif conn_type == ConnectionChoices.CSV:
            return
        elif conn_type == ConnectionChoices.TXT:
            return
        else:
            raise ValueError("Нефайловый тип подключения!")

    def get_source_instance(self):
        """
        Инстанс файлового соурса

        Returns:
            etl.services.db.interfaces.File
        """
        conn_type = self.source.conn_type

        return self.factory(conn_type)
