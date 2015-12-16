# coding: utf-8
from collections import defaultdict
from itertools import groupby
from django.conf import settings
from etl.services.db.interfaces import Database
import cx_Oracle
from etl.services.db.maps import oracle as oracle_map


class Oracle(Database):
    """Управление источником данных Oracle"""

    db_map = oracle_map

    @staticmethod
    def get_separator():
        """
        Возвращает ковычки(") для запроса
        """
        return '\"'

    @staticmethod
    def get_connection(conn_info):
        """
        connection бд соурса
        :param conn_info:
        :return: connection
        """

        try:
            cont_str = '{0}/{1}@{2}:{3}/{4}'.format(
                str(conn_info['login']), str(conn_info['password']),
                str(conn_info['host']), int(conn_info['port']),
                str(conn_info['db']))
            return cx_Oracle.connect(cont_str)
        except cx_Oracle.OperationalError:
            return None

    @staticmethod
    def _get_columns_query(source, tables):
        """
        запросы для колонок, констраинтов, индексов соурса
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        cols_query = oracle_map.columns_query.format(tables_str)
        constraints_query = oracle_map.constraints_query.format(tables_str)
        indexes_query = oracle_map.indexes_query.format(tables_str)

        return cols_query, constraints_query, indexes_query

    def get_columns(self, source, tables):
        """
        Получение списка колонок в таблицах

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            Кортеж из списков, в каждом из которых возращается результат запроса
            выборки из базы данных о колонках, индексах и ограничениях таблиц.
            Например::
                (
                    [('DEPARTMENTS', 'DEPARTMENT_ID', 'NUMBER'), ...],
                    [('LOCATIONS', 'LOCATION_ID', 'LOC_ID_PK', 't', 't'),...],
                    [('DEPARTMENTS', 'LOCATION_ID', 'DEPT_LOC_FK', 'R',
                        'LOCATIONS', 'LOCATION_ID', None, 'NO ACTION'), ...],
                )

        """
        columns_query, consts_query, indexes_query = self._get_columns_query(
            source, tables)

        col_records = self.get_query_result(columns_query)
        index_records = []
        for record in self.get_query_result(indexes_query):
            index_records.append(
                (record[0], record[1], record[2],
                 't' if record[2].endswith('PK') else 'f',
                 't' if record[3] == 'UNIQUE' else 'f'))

        const_records = self.get_query_result(consts_query)

        return col_records, index_records, const_records

    @classmethod
    def get_statistic_query(cls, source, tables):
        """
        Запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'
        return cls.db_map.stat_query.format(tables_str, source.db)

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов соурса
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        # fixme: повторяет код
        indexes = defaultdict(list)
        itable_name, icol_names, index_name, primary, unique = xrange(5)

        for ikey, igroup in groupby(index_records, lambda x: x[itable_name]):
            for ig in igroup:
                indexes[ikey].append({
                    "name": ig[index_name],
                    "columns": ig[icol_names].split(','),
                    "is_primary": ig[primary] == 't',
                    "is_unique": ig[unique] == 't',
                })

        constraints = defaultdict(list)
        (c_table_name, c_col_name, c_name, c_type,
         c_foreign_table, c_foreign_col, c_update, c_delete) = xrange(8)

        for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
            for ig in igroup:
                constraints[ikey].append({
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

        table_name, col_name, col_type = xrange(3)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

                for i in t_indexes:
                    if col in i['columns']:
                        is_index = True
                        index_name = i['name']
                        for c in t_consts:
                            const_type = c['c_type']
                            if index_name == c['c_name']:
                                if const_type == 'U':
                                    is_unique = True
                                elif const_type == 'P':
                                    is_unique = True
                                    is_primary = True

                columns[key].append({"name": col,
                                     "type": (cls.db_map.DB_TYPES[
                                                  cls.lose_brackets(x[col_type])]
                                              or x[col_type]),
                                     "is_index": is_index,
                                     "is_unique": is_unique,
                                     "is_primary": is_primary})

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'R':
                    foreigns[key].append({
                        "name": c['c_name'],
                        "source": {"table": key, "column": c["c_col_name"]},
                        "destination":
                            {"table": c["c_f_table"], "column": c["c_f_col"]},
                        "on_delete": c["c_del"],
                        "on_update": c["c_upd"],
                    })
        return columns, indexes, foreigns

    def get_rows_query(self, cols, structure):
        """
        Формирования строки запроса на получение данных из базы
        """
        query_join = self.generate_join(structure,)

        separator = self.get_separator()

        # Расширяем информацию о колонках значением алиаса
        alias_list = []
        for each in cols:
            alias = '%s__%s' % (each['table'], each['col'])
            alias_list.append(alias)
            each.update(alias=alias)

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep} {2}'.format(
            '{table}', '{col}', '{alias}', sep=separator)

        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        return self.db_map.row_query.format(
            ', '.join(alias_list), cols_str, query_join,
            '{1}', '{0}')

