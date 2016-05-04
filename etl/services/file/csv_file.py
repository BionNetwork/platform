# coding: utf-8
from __future__ import unicode_literals

import time
import csv
import datetime
import pandas
from collections import defaultdict
from itertools import groupby

from etl.services.file.interfaces import File


class CSV(File):
    """
    Класс для работы с Excel файлами
    """

    # def get_tables(self):
    #     """
    #     Возвращает таблицы источника
    #
    #     Returns:
    #         list: список таблиц
    #     """
    #     file_path = self.source.file
    #     csv_file = (file_path)
    #     sheet_names = excel.sheet_names
    #
    #     return map(lambda x: {'name': x, }, sheet_names)
    #
    # def get_columns_info(self, sheets):
    #     """
    #     Получение списка колонок в таблицах
    #
    #     Args:
    #         source(`Datasource`): источник
    #         tables(list): список названий таблиц
    #
    #     Returns:
    #         dict вида {'sheet_name': [{
    #         "name": col_name,
    #         "type": col_type,
    #         "origin_type": origin_type,}, ]
    #     """
    #     columns = defaultdict(list)
    #
    #     excel_path = self.source.get_file_path()
    #
    #     for sheet_name in sheets:
    #         sheet_df = pandas.read_excel(excel_path, sheetname=sheet_name)
    #         col_names = sheet_df.columns
    #         for col_name in col_names:
    #             origin_type = sheet_df[col_name].dtype.name
    #             col_type = process_type(origin_type)
    #             columns[sheet_name].append({
    #                 "name": col_name,
    #                 "type": col_type,
    #                 "origin_type": origin_type,
    #                 "extra": None,
    #                 "max_length": None,
    #             })
    #
    #     return columns
