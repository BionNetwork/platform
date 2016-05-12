# coding: utf-8
from __future__ import unicode_literals

import time
import csv
import datetime
import pandas
from collections import defaultdict
from itertools import groupby

from etl.services.file.interfaces import File, process_type


class CSV(File):
    """
    Класс для работы с Excel файлами
    """

    def get_tables(self):
        """
        Возвращает 1 страницу Лист1
        Returns:
            list: список таблиц
        """
        return [{'name': u"Лист 1", }, ]

    def get_columns_info(self, sheets):
        """
        Получение списка колонок в таблицах
        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            dict вида {'sheet_name': [{
            "name": col_name,
            "type": col_type,
            "origin_type": origin_type,}, ]
        """
        columns = defaultdict(list)
        csv_path = self.source.get_file_path()

        sheet_df = pandas.read_csv(csv_path)
        col_names = sheet_df.columns
        for col_name in col_names:
            origin_type = sheet_df[col_name].dtype.name
            col_type = process_type(origin_type)
            columns[u"Лист 1"].append({
                "name": col_name,
                "type": col_type,
                "origin_type": origin_type,
                "extra": None,
                "max_length": None,
                "is_unique": None,
                "is_primary": None,
            })

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
        csv_path = self.source.get_file_path()

        sheet_df = pandas.read_csv(csv_path)
        height, width = sheet_df.shape
        size = sheet_df.memory_usage(deep=True).sum()
        statistic[u"Лист 1"] = {"count": height, "size": size}

        return statistic

    def get_intervals(self, sheets):
        """
        Возращается список интервалов для полей типа Дата

        Args:
            source('Datasource'): Источник
            cols_info(list): Информация о колонках

        Returns:
            dict: Информация о крайних значениях дат
            {'sheet_name': [{'last_updated': now,
                   'name': col_name,
                   'startDate': start_date,
                   'endDate': end_date,}]}
        """

        intervals = defaultdict(list)
        now = time.mktime(datetime.datetime.now().timetuple())
        csv_path = self.source.get_file_path()

        sheet_df = pandas.read_csv(csv_path)
        col_names = sheet_df.columns
        for col_name in col_names:
            col_df = sheet_df[col_name]
            col_type = process_type(col_df.dtype.name)
            if col_type in ["datetime"]:

                start_date = col_df.min().strftime("%d.%m.%Y")
                end_date = col_df.max().strftime("%d.%m.%Y")

                intervals[u"Лист 1"].append({
                    'last_updated': now,
                    'name': col_name,
                    'startDate': start_date,
                    'endDate': end_date,
                })

        return intervals

    def generate_join(self, childs):
        """
        Принимает структуру дерева и
        """
        result = []
        new_childs = []
        for child in childs:
            new_childs += child['childs']
            result.append({
                'name': child['val'],
                'join_type': child['join_type'],
                'joins': child['joins']
            })
        if new_childs:
            result += self.generate_join(new_childs)
        return result

    def get_rows(self, columns, structure):

        csv_path = self.source.get_file_path()
        sheet_df = pandas.read_csv(csv_path)
        return sheet_df.fillna('').values.tolist()
