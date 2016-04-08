# coding: utf-8
from etl.services.source import DatasourceApi
from core.models import ConnectionChoices


class FileService(DatasourceApi):
    """Сервис для источников данных на основе файлов"""


    @staticmethod
    def factory(source):
        """
        Фабрика для инстанса файлов

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            etl.services.files.interfaces.File
        """
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.EXCEL:
            return
        elif conn_type == ConnectionChoices.CSV:
            return
        elif conn_type == ConnectionChoices.TXT:
            return
        else:
            raise ValueError("Нефайловый тип подключения!")

