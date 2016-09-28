# coding: utf-8


import datetime
import time
from collections import defaultdict
from operator import itemgetter
from itertools import groupby

import pandas
from dateutil.parser import parse
from xlrd import XLRDError

from etl.services.datasource.file.interfaces import File
from etl.services.exceptions import (SheetException, ColumnException)
from etl.services.datasource.source import SourceConvertTypes as SCT


class Excel(File):
    """
    Класс для работы с Excel файлами
    """

    def read_excel_necols(self, *args, **kwargs):
        """
        Открытие файла без пустых колонок(если без title)
        """
        try:
            df = pandas.read_excel(*args, **kwargs)#.dropna(axis=1, how='all')
        except XLRDError as e:
                raise SheetException(message=e.message)
        columns = df.columns
        ne_columns = [
            col for col in columns
            if not (str(col).startswith('Unnamed: ') and
                    not df[col].notnull().any())
            ]

        return df[ne_columns]

    def get_indent_dict(self, indents, sheet_name):
        """
        Инфа отступов и титулов
        """
        kwargs = {}
        indent = indents[sheet_name]['indent']
        header = indents[sheet_name]['header']

        if not header:
            kwargs['header'] = None
        if indent is not None:
            kwargs['skiprows'] = indent
        return kwargs

    def calc_indent(self, indents, sheet_name):
        """
        Для подкрашивания ячеек, считаем сколько отступов
        """
        indent = int(indents[sheet_name]['indent'])

        if indents[sheet_name]['header']:
            indent += 1

        return indent

    def get_tables(self):
        """
        Возвращает таблицы источника

        Returns:
            list: список таблиц
        """
        file_path = self.source.file
        excel = pandas.ExcelFile(file_path)
        sheet_names = excel.sheet_names

        return sheet_names

    def get_data(self, sheet_name, indents):

        file_path = self.source.get_file_path()
        kwargs = self.get_indent_dict(indents, sheet_name)

        df = self.read_excel_necols(file_path, sheet_name, **kwargs)
        return df[:50].fillna('null').to_dict(orient='records')

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
            kwargs = self.get_indent_dict(indents, sheet_name)

            try:
                sheet_df = self.read_excel_necols(
                    excel_path, sheet_name, **kwargs)
            except XLRDError as e:
                raise SheetException(message=e.message)

            col_names = sheet_df.columns
            for col_name in col_names:

                col_df = sheet_df[col_name]
                origin_type = col_df.dtype.name
                col_type = self.process_type(origin_type)

                # определяем типы дат вручную
                # if col_type != TIMESTAMP:
                #     try:
                #         parse(col_df.loc[0])
                #     except Exception:
                #         pass
                #     else:
                #         col_type = TIMESTAMP

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
            kwargs = self.get_indent_dict(indents, sheet_name)
            try:
                sheet_df = self.read_excel_necols(
                    excel_path, sheet_name, **kwargs)
            except XLRDError as e:
                raise SheetException(message=e.message)

            height, width = sheet_df.shape
            size = sheet_df.memory_usage(deep=True).sum().tolist()
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
            kwargs = self.get_indent_dict(indents, sheet_name)

            try:
                sheet_df = self.read_excel_necols(
                    excel_path, sheet_name, **kwargs)
            except XLRDError as e:
                raise SheetException(message=e.message)

            col_names = sheet_df.columns
            for col_name in col_names:
                col_df = sheet_df[col_name]
                col_type = self.process_type(col_df.dtype.name)
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

    # FIXME NO USAGE
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

    def validate_sheets(self, sheets, indents):
        """
        Валидация страниц
        """
        excel_path = self.source.get_file_path()

        for sheet_name in sheets:
            kwargs = self.get_indent_dict(indents, sheet_name)
            try:
                sheet_df = self.read_excel_necols(
                    excel_path, sheet_name, **kwargs)
            except XLRDError as e:
                raise SheetException(message=e.message)

            col_names = sheet_df.columns
            for col_name in col_names:
                col_df = sheet_df[col_name]
                print(col_name, col_df.dtype.name)

    DATES = {
        # FIXME потом '0000-00-00 00:00:00' засунуть for timestamp & datetime
        "timestamp": {'val': '0000-00-00'},
        "datetime": {'val': '0000-00-00'},
        "date": {'val': '0000-00-00'},
    }
    DATE_DEFAULT = {'val': '0000-00-00'}

    NUMERIC = {
        "integer": {'val': 0, 'type': int},
        "double precision": {'val': 0, 'type': float},
    }
    NUMERIC_DEFAULT = {'val': 0, 'type': int}

    def xls_convert_to_csv(self, sheet_name, columns, indents, csv_file_name):
        """
        Страницу экселя в csv
        """

        # indexes = [x['order'] for x in columns]
        select_cols = [x['name'] for x in sorted(
            columns, key=itemgetter('order'))]

        dates = [x for x in columns if x['type'] in self.DATES]
        numeric = [x for x in columns if x['type'] in self.NUMERIC]

        excel_path = self.source.get_file_path()
        kwargs = self.get_indent_dict(indents, sheet_name)

        # data_xls = self.read_excel_necols(
        #     excel_path, sheet_name,  parse_cols=indexes,
        #     index_col=False, **kwargs)

        data_xls = pandas.read_excel(
            excel_path, sheet_name, **kwargs)

        data_xls = data_xls[select_cols]

        # process columns here for click (dates, timestamps)
        for dat_col in dates:
            n = dat_col['name']
            t = dat_col['type']
            data_xls[n] = pandas.to_datetime(
                data_xls[n], errors='coerce').dt.date.fillna(# dt.date for date
                self.DATES.get(t, self.DATE_DEFAULT)['val'])

        for num_col in numeric:
            n = num_col['name']
            t = num_col['type']
            data_xls[n] = pandas.to_numeric(
                data_xls[n], errors='coerce').fillna(0).astype(
                self.NUMERIC.get(t, self.NUMERIC_DEFAULT)['type'])

        data_xls.to_csv(
            csv_file_name,
            header=list(range(len(select_cols))),
            encoding='utf-8', index=False)

    def validate_column(self, sheet_name, column, typ, indents):
        """
        Проверка колонки на соответствующий тип typ
        """
        excel_path = self.source.get_file_path()
        kwargs = self.get_indent_dict(indents, sheet_name)

        # отступ плюсуем к индексам ошибочных ячеек
        offset = self.calc_indent(indents, sheet_name)

        try:
            sheet_df = pandas.read_excel(
                excel_path, sheet_name, **kwargs)
        except XLRDError as e:
            raise SheetException(message=e.message)

        if column not in sheet_df.columns:
            raise ColumnException()

        col_df = sheet_df[column]

        if typ == SCT.DATE:
            nulls = col_df.loc[col_df.isnull()].index.tolist()
            to_dates = pandas.to_datetime(col_df, errors='coerce')
            more_nulls = to_dates.loc[to_dates.isnull()].index.tolist()
            errors = [x+1+offset for x in more_nulls if x not in nulls]

        elif typ in [SCT.INT, SCT.DOUBLE]:
            nulls = col_df.loc[col_df.isnull()].index.tolist()
            to_nums = pandas.to_numeric(col_df, errors='coerce')
            more_nulls = to_nums.loc[to_nums.isnull()].index.tolist()
            errors = [x+1+offset for x in more_nulls if x not in nulls]

        elif typ == SCT.TEXT:
            errors = []
        
        elif typ == SCT.BOOL:
            raise ColumnException("No implementation for bool!")

        else:
            raise ColumnException("Invalid type of column!")

        return errors
