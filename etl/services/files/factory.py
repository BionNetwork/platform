# coding: utf-8
__author__ = 'damir(GDR)'

from django.conf import settings

from core.models import ConnectionChoices
from etl.services.base_service import DataService


class FileService(DataService):
    """Сервис для источников данных на основе файлов"""

    @staticmethod
    def factory(source):
        """
        Фабрика для инстанса файлов

        Args:
            source: DataSource

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

    @classmethod
    def get_source_instance(cls, source):
        """
        инстанс соурса-файла

        Args:
            source(core.models.Datasource): источник

        Returns:
            etl.services.db.interfaces.File
        """
        instance = cls.factory(source)

        return instance