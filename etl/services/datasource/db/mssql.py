# coding: utf-8

__author__ = 'damir(GDR)'

from collections import defaultdict
from itertools import groupby
from operator import itemgetter

import pymssql
from django.conf import settings

from etl.services.datasource.db.maps import mssql as mssql_map
from etl.services.datasource.db.interfaces import Database


class MsSql(Database):
    """Управление источником данных MSSQL"""

    db_map = mssql_map

    @staticmethod
    def get_connection(conn_info):
        """
        connection бд соурса
        :param conn_info:
        :return: connection
        """
        try:
            connection = {'database': str(conn_info['db']),
                          'host': str(conn_info['host']),
                          # 'port': int(conn_info['port']),
                          'user': str(conn_info['login']),
                          'password': str(conn_info['password']),
                          }
            conn = pymssql.connect(**connection)
        except pymssql.Error:
            return None
        return conn

    @staticmethod
    def get_separator():
        """
            Возвращает ковычки(") для запроса
        """
        return '\"'

    def get_rows_query(self, cols, structure):
        """
        достает строки из соурса для превью
        :param cols: list
        :param structure: dict
        :return: list
        """
        query_join = self.generate_join(structure,)

        separator = self.get_separator()

        sel_col1 = '{sep}{0}{sep}.{sep}{1}{sep} {sep}{0}.{1}{sep}'.format(
            '{table}', '{col}', sep=separator)
        sel_cols_str1 = ', '.join(
            [sel_col1.format(**x) for x in cols])

        group_cols = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)
        group_cols_str = ', '.join(
            [group_cols.format(**x) for x in cols])

        sel_col2 = '{sep}Results_CTE{sep}.{sep}{0}.{1}{sep}'.format(
            '{table}', '{col}', sep=separator)
        sel_cols_str2 = ', '.join(
            [sel_col2.format(**x) for x in cols])

        return self.db_map.row_query.format(
            sel_cols_str1, query_join,
            '{0}', '{1}',
            group_cols_str, sel_cols_str2)

    def get_rows(self, cols, structure):
        """
        Получаем записи из клиентской базы для предварительного показа

        Args:
            cols(dict): Название колонок
            structure(dict): Структура данных

        Returns:
            list of tuple: Данные по колонкам
        """
        query = self.get_rows_query(cols, structure)

        return self.get_query_result(
            query.format(settings.ETL_COLLECTION_PREVIEW_LIMIT, 0))

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов соурса
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        indexes = defaultdict(list)
        itable_name, index_name, primary, unique, icol_name, = range(5)
        grouper = itemgetter(itable_name, index_name, primary, unique)

        for key, igroup in groupby(index_records, grouper):
            ind_info = {
                "name": key[index_name],
                "is_primary": key[primary] == 't',
                "is_unique": key[unique] == 't',
            }
            cols = []
            for ig in igroup:
                cols.append(ig[icol_name])
            ind_info["columns"] = cols
            indexes[key[itable_name].lower()].append(ind_info)

        constraints = defaultdict(list)
        (c_table_name, c_col_name, c_name, c_type,
         c_foreign_table, c_foreign_col, c_update, c_delete) = range(8)

        for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
            for ig in igroup:
                constraints[ikey.lower()].append({
                    "c_col_name": ig[c_col_name],
                    "c_name": ig[c_name],
                    "c_type": ig[c_type],
                    "c_f_table": ig[c_foreign_table],
                    "c_f_col": ig[c_foreign_col],
                    "c_upd": ig[c_update],
                    "c_del": ig[c_delete],
                })

        columns = defaultdict(list)
        foreigns = defaultdict(list)

        table_name, col_name, col_type, is_nullable, extra_, max_length = range(6)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key.lower()]
            t_consts = constraints[key.lower()]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

                for i in t_indexes:
                    if col in i['columns']:
                        is_index = True
                        for c in t_consts:
                            const_type = c['c_type']
                            if col == c['c_col_name']:
                                if const_type == 'UNIQUE':
                                    is_unique = True
                                elif const_type == 'PRIMARY KEY':
                                    is_unique = True
                                    is_primary = True

                columns[key.lower()].append({
                    "name": col,
                    "type": (
                        mssql_map.MSSQL_TYPES[cls.lose_brackets(x[col_type])] or x[col_type]),
                    "is_index": is_index,
                    "is_unique": is_unique,
                    "is_primary": is_primary,
                    "origin_type": x[col_type],
                    "is_nullable": x[is_nullable],
                    "extra": x[extra_],
                    "max_length": x[max_length],
                })

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'FOREIGN KEY':
                    foreigns[key.lower()].append({
                        "name": c['c_name'],
                        "source": {"table": key, "column": c["c_col_name"]},
                        "destination":
                            {"table": c["c_f_table"], "column": c["c_f_col"]},
                        "on_delete": c["c_del"],
                        "on_update": c["c_upd"],
                    })
        return columns, indexes, foreigns

    def get_structure_rows_number(self, structure, cols):
        """
        возвращает примерное кол-во строк в запросе для планирования
        """
        # FIXME не понятно как доставать, поэтому тупо count

        query_join = self.generate_join(structure)
        select_query = self.get_select_query()
        explain_query = select_query.format(
            'count(1)', query_join)

        result = self.get_query_result(explain_query)
        count = result[0][0]

        return count

    @staticmethod
    def get_fetchall_result(connection, query, **kwargs):
        """
        возвращает результат fetchall преобразованного запроса с аргументами,
        появляются проблемы когда вместо формата %s есть {0}, {1} ...
        """
        cursor = connection.cursor()

        if 'limit' and 'offset' in kwargs:
            query = query.format(kwargs['limit'], kwargs['offset'])

        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def get_processed_indexes(exist_indexes):
        """
        Получает инфу об индексах, возвращает преобразованную инфу
        для создания триггеров
        """
        # indexes_query смотреть
        index_name_i, index_col_i = 1, 4
        # группировка по названию индекса, в группе названия колонок
        indexes = []

        for ind_name, ind_group in groupby(exist_indexes, lambda x: x[index_name_i]):
            cols = [ig[index_col_i] for ig in ind_group]
            indexes.append([','.join(cols), ind_name, ])

        return indexes
