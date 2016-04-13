# coding: utf-8
from __future__ import unicode_literals

import pandas
from collections import defaultdict

from django.conf import settings

from etl.services.file.interfaces import File


MEDIA_ROOT = settings.MEDIA_ROOT


TYPES_MAP = {
    "int": "integer",
    "float": "double precision",
    "datetime": "datetime",
    "object": "text",
}


def process_type(type_):
    """
    # FIXME надо для Mongo, а не для Postgresql
    Отображение типов Excel в типы Postgresql
    """
    for k, v in TYPES_MAP.items():
        if type_.startswith(k):
            return v
    raise ValueError("Необработанный тип для Excel!")


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
        file_path = self.source.file
        excel = pandas.ExcelFile(file_path)
        sheet_names = excel.sheet_names

        return map(lambda x: {'name': x, }, sheet_names)

    def get_columns_info(self, sheets):
        """
        Получение списка колонок в таблицах

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            Кортеж из списков, в первом списке возвращаются колонки таблиц
            вида [(table_name, col_name, type), ]
        """
        columns = []

        excel_path = self.source.get_file_path()

        for sheet_name in sheets:
            sheet_df = pandas.read_excel(excel_path, sheetname=sheet_name)
            col_names = sheet_df.columns
            for col_name in col_names:
                col_type = process_type(sheet_df[col_name].dtype.name)
                columns.append((sheet_name, col_name, col_type))

        return columns

    def get_statistic(self, sheets):
        """
        возвращает статистику страниц файла

        Args:
            tables(list): Список таблиц

        Returns:
            dict: Статистические данные
            {'sheet_name': ({'count': кол-во строк, 'size': объем памяти строк)}
        """

        statistic = {}

        excel_path = self.source.get_file_path()

        for sheet_name in sheets:
            sheet_df = pandas.read_excel(excel_path, sheetname=sheet_name)
            height, width = sheet_df.shape
            size = sheet_df.memory_usage(deep=True).sum()
            statistic[sheet_name] = {"count": height, "size": size}

        return statistic

    def get_intervals(self, sheets):
        """
        Возращается список интервалов для полей типа Дата

        Args:
            source('Datasource'): Источник
            cols_info(list): Информация о колонках

        Returns:
            dict: Информация о крайних значениях дат
        """

        intervals = defaultdict(list)

        excel_path = self.source.get_file_path()

        for sheet_name in sheets:
            sheet_df = pandas.read_excel(excel_path, sheetname=sheet_name)



        for table, col, query in interval_queries:
            start_date, end_date = self.get_query_result(query)[0]
            if res.get(table, None):
                res[table].append({
                    'last_updated': now,
                    'name': col,
                    'startDate': start_date,
                    'endDate': end_date,
                })
            else:
                res[table] = [{
                    'last_updated': now,
                    'name': col,
                    'startDate': start_date,
                    'endDate': end_date,
                }]
        print res
        return res






















