# coding: utf-8
from __future__ import unicode_literals

from collections import defaultdict

from core.models import ConnectionChoices

from etl.services.source import DatasourceApi
from etl.services.file.excel import Excel
from etl.services.file.csv_file import CSV


class FileService(DatasourceApi):
    """Сервис для источников данных на основе файлов"""

    def __init__(self, source):
        # проверка файла источника
        if not source.file:
            raise ValueError("Отсутствует файл источника!")

        super(FileService, self).__init__(source)

    def get_source_instance(self):
        """
        Фабрика для инстанса файлов

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            etl.services.files.interfaces.File
        """

        source = self.source
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.EXCEL:
            return Excel(source)
        elif conn_type == ConnectionChoices.CSV:
            return CSV(source)
        elif conn_type == ConnectionChoices.TXT:
            return
        else:
            raise ValueError("Нефайловый тип подключения!")

    def get_tables(self):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        return self.datasource.get_tables()

    def get_columns_info(self, sheets):
        """
            Получение полной информации о колонках таблиц
        Args:
            tables(list): список таблиц

        Returns:
            list: список колонок, индексов нет, FK-ограничений нет,
            статистики, интервалов дат таблиц
        """
        instance = self.datasource

        columns = instance.get_columns_info(sheets)
        statistics = instance.get_statistic(sheets)
        date_intervals = instance.get_intervals(sheets)

        # заглушки
        indexes = defaultdict(list)
        foreigns = defaultdict(list)

        return columns, indexes, foreigns, statistics, date_intervals

    def fetch_tables_columns(self, sheets, indents):
        # возвращает список колонок страниц
        instance = self.datasource
        columns = instance.get_columns_info(sheets, indents)
        return columns

    def get_structure_rows_number(self, structure,  cols):
        return 0

    def get_source_rows(self, structure, cols, limit=None, offset=None):
        return self.datasource.get_rows(cols, structure)

    def get_source_table_rows(self, table_name, **kwargs):
        """
        Данные по одной таблице источника
        """
        indents = kwargs.get('indents')
        instance = self.datasource
        columns = instance.get_data(table_name, indents)
        return columns
