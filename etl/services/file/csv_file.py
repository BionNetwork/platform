# coding: utf-8


import time
import datetime
from dateutil.parser import parse
import pandas
from collections import defaultdict
from more_itertools import first

from etl.services.file.interfaces import File, process_type, TIMESTAMP
from etl.services.exceptions import SheetExcept


# FIXME get delimiter from storage
class CSV(File):
    """
    Класс для работы с Excel файлами
    """
    # если что-то пошло не так, ставим default название страницы
    DEFAULT_SHEET = 'Sheet1'

    @classmethod
    def check_sheet(cls, func):
        def inner(*args, **kwargs):
            inst, sheets = args[:2]

            sheet = inst.get_sheet_name()
            if sheet not in sheets:
                raise SheetExcept()

            return func(*args, **kwargs)
        return inner

    def get_sheet_name(self):
        """
        Возвращает название страницы,
        для csv это будет название файла без формата
        """
        sheet_name = first(self.file_name.split('.'), self.DEFAULT_SHEET)
        return sheet_name

    def get_tables(self):
        """
        Returns:
            list: список таблиц, для csv это будет 1 страница(название файла)
        """
        return [{'name': self.get_sheet_name(), }, ]

    def get_data(self, sheet_name, indents):

        csv_path = self.file_path
        indent = indents[sheet_name]

        df = pandas.read_csv(csv_path, skiprows=indent, delimiter=',')

        return df.to_dict(orient='records')

    # обернут в check_sheet
    def get_columns_info(self, sheets, indents):
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

        sheet = self.get_sheet_name()

        columns = defaultdict(list)
        csv_path = self.file_path
        indent = indents[sheet]

        sheet_df = pandas.read_csv(csv_path, skiprows=indent, delimiter=',')
        col_names = sheet_df.columns
        for col_name in col_names:
            col_df = sheet_df[col_name]
            origin_type = col_df.dtype.name
            col_type = process_type(origin_type)

            # определяем типы дат вручную
            if col_type != TIMESTAMP:
                try:
                    parse(col_df.loc[0])
                except Exception:
                    pass
                else:
                    col_type = TIMESTAMP

            columns[sheet].append({
                "name": col_name,
                "type": col_type,
                "origin_type": origin_type,
                "max_length": None,
            })

        return columns

    # обернут в check_sheet
    def get_statistic(self, sheets, indents):
        """
        возвращает статистику страниц файла

        Args:
            tables(list): Список таблиц

        Returns:
            dict: Статистические данные
            {'sheet_name': ({'count': кол-во строк, 'size': объем памяти строк)}
        """

        sheet = self.get_sheet_name()

        statistic = {}
        csv_path = self.file_path
        indent = indents[sheet]

        sheet_df = pandas.read_csv(csv_path, skiprows=indent, delimiter=',')
        height, width = sheet_df.shape
        size = sheet_df.memory_usage(deep=True).sum()
        statistic[sheet] = {"count": height, "size": size}

        return statistic

    # обернут в check_sheet
    def get_intervals(self, sheets, indents):
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

        sheet = self.get_sheet_name()

        intervals = defaultdict(list)
        now = time.mktime(datetime.datetime.now().timetuple())
        csv_path = self.source.get_file_path()
        indent = indents[sheet]

        sheet_df = pandas.read_csv(csv_path, skiprows=indent, delimiter=',')
        col_names = sheet_df.columns
        for col_name in col_names:
            col_df = sheet_df[col_name]
            col_type = process_type(col_df.dtype.name)
            if col_type in ["datetime"]:

                start_date = col_df.min().strftime("%d.%m.%Y")
                end_date = col_df.max().strftime("%d.%m.%Y")

                intervals[sheet].append({
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


CSV.get_columns_info = CSV.check_sheet(CSV.get_columns_info)
CSV.get_statistic = CSV.check_sheet(CSV.get_statistic)
CSV.get_intervals = CSV.check_sheet(CSV.get_intervals)
