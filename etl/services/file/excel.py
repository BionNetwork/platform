# coding: utf-8
from __future__ import unicode_literals

import time
import datetime
from dateutil.parser import parse
import pandas
from collections import defaultdict
from itertools import groupby
from xlrd import XLRDError

from etl.services.file.interfaces import File, process_type, TIMESTAMP
from etl.services.excepts import SheetExcept


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

    def get_data(self, sheet_name, indents):

        file_path = self.source.file
        excel = pandas.ExcelFile(file_path)
        indent = indents[sheet_name]

        df = pandas.read_excel(excel, sheet_name, skiprows=indent)
        return df[:50].fillna('').to_dict(orient='records')

    def get_columns_info(self, sheets, indents):
        """
        Получение списка колонок в таблицах

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц
            indents: defaultdict
        Returns:
            dict вида {'sheet_name': [{
            "name": col_name,
            "type": col_type,
            "origin_type": origin_type,}, ]
        """

        columns = defaultdict(list)

        excel_path = self.source.get_file_path()

        for sheet_name in sheets:

            indent = indents[sheet_name]
            try:
                sheet_df = pandas.read_excel(
                    excel_path, sheetname=sheet_name, skiprows=indent)
            except XLRDError as e:
                raise SheetExcept(message=e.message)

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

                columns[sheet_name].append({
                    "name": col_name,
                    "type": col_type,
                    "origin_type": origin_type,
                    "max_length": None,
                })

        return columns

    def get_statistic(self, sheets, indents):
        """
        возвращает статистику страниц файла

        Args:
            tables(list): Список таблиц

        Returns:
            dict: Статистические данные
            {'sheet_name': ({'count': кол-во строк, 'size': объем памяти строк)}
        """

        statistic = {}

        excel_path = self.file_path

        for sheet_name in sheets:
            indent = indents[sheet_name]
            try:
                sheet_df = pandas.read_excel(
                    excel_path, sheetname=sheet_name, skiprows=indent)
            except XLRDError as e:
                raise SheetExcept(message=e.message)

            height, width = sheet_df.shape
            size = sheet_df.memory_usage(deep=True).sum()
            statistic[sheet_name] = {"count": height, "size": size}

        return statistic

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

        intervals = defaultdict(list)
        now = time.mktime(datetime.datetime.now().timetuple())
        excel_path = self.source.get_file_path()

        for sheet_name in sheets:
            indent = indents[sheet_name]

            try:
                sheet_df = pandas.read_excel(
                    excel_path, sheetname=sheet_name, skiprows=indent)
            except XLRDError as e:
                raise SheetExcept(message=e.message)

            col_names = sheet_df.columns
            for col_name in col_names:
                col_df = sheet_df[col_name]
                col_type = process_type(col_df.dtype.name)
                if col_type in ["datetime"]:

                    start_date = col_df.min().strftime("%d.%m.%Y")
                    end_date = col_df.max().strftime("%d.%m.%Y")

                    intervals[sheet_name].append({
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

        # FIXME недоработанная штука, preview работает чуток тока

        left_sheet = structure['val']
        join_rules = self.generate_join(structure['childs'])

        dfs = {}
        excel_path = self.source.get_file_path()

        if columns:
            for sheet_name, col_group in groupby(columns, lambda x: x['table']):
                sheet_df = pandas.read_excel(excel_path, sheetname=sheet_name)
                col_names = [x['col'] for x in col_group]
                dfs[sheet_name] = sheet_df[col_names]

            left_df = dfs[left_sheet]
        else:
            # качаем в бд постранично
            left_df = pandas.read_excel(excel_path, sheetname=left_sheet)

        new_name = "new_name_{0}"
        merge_columns = []

        for rule in join_rules:

            right_sheet = rule["name"]
            join_type = rule["join_type"]
            joins = rule["joins"]
            new_columns1 = {}
            new_columns2 = {}

            for i, j in enumerate(joins, start=0):
                new_name_i = new_name.format(i)
                new_columns1[j['left']['column']] = new_name_i
                new_columns2[j['right']['column']] = new_name_i
                merge_columns.append(new_name_i)

            left_df = left_df.rename(columns=new_columns1)
            right_df = dfs[right_sheet]

            # right_columns = right_df.columns.tolist()
            # left_len = len(left_df.columns)

            right_df = right_df.rename(columns=new_columns2)

            # for r_col in new_columns2:
            #     new_columns2[r_col] = right_columns.index(r_col)

            left_df = pandas.merge(
                left_df, right_df, how=join_type, on=merge_columns)

        return left_df.fillna('').values.tolist()
