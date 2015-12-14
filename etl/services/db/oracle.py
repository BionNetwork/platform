# coding: utf-8
from collections import defaultdict
from itertools import groupby
from django.conf import settings
from etl.services.db.interfaces import Database
import cx_Oracle
from etl.services.db.maps import oracle as oracle_map

class Oracle(Database):
    """Управление источником данных MySQL"""


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
            conn = cx_Oracle.connect(cont_str)
        except cx_Oracle.OperationalError:
            return None
        return conn

    def get_tables(self, source):
        """
        возвращает таблицы соурса
        :param source: Datasource
        :return: list
        """
        query = """select table_name from user_tables"""
        records = self.get_query_result(query)
        records = map(lambda x: {'name': x[0], },
                      sorted(records, key=lambda y: y[0]))

        return records


    @staticmethod
    def _get_columns_query(source, tables):
        """
        запросы для колонок, констраинтов, индексов соурса
        :param source: Datasource
        :param tables:
        :return: tuple
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'
        cols_query = """select table_name, column_name, data_type from user_tab_columns
          where table_name in {0}""".format(tables_str)

        constraints_query = oracle_map.constraints_query.format(tables_str)
        #
        indexes_query = oracle_map.indexes_query.format(tables_str)

        return cols_query, constraints_query, indexes_query


    def get_columns(self, source, tables):
        """
        Получение списка колонок в таблицах
        :param source:
        :param tables:
        :return:
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

    @staticmethod
    def get_statistic_query(source, tables):
        """
        запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        stat_query = """
    SELECT relname, reltuples as count, relpages*8192 as size FROM pg_class
    where oid in {0};
"""
        tables_str = '(' + ', '.join(["'{0}'::regclass::oid".format(y) for y in tables]) + ')'
        stats_query = stat_query.format(tables_str)
        return stats_query

    def get_statistic(self, source, tables):
        # TODO: Переписать
        return {x: None for x in tables}

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
                                     "type": (oracle_map.ORACLE_TYPES[
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



    @staticmethod
    def get_rows_query():
        """
        возвращает селект запрос c лимитом, оффсетом
        :return: str
        """
        query = 'SELECT {0} FROM (SELECT {1}, ROW_NUMBER() OVER (ORDER BY ROWNUM) AS rn FROM {2}) WHERE rn BETWEEN {3} AND {4}'
        return query

    def get_rows(self, cols, structure):
        """
        Получаем записи из клиентской базы для предварительного показа

        Args:
            cols(dict): Название колонок
            structure(dict): Структура данных

        Returns:
            list of tuple: Данные по колонкам
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

        query = self.get_rows_query().format(
            ', '.join(alias_list), cols_str, query_join,
            0, settings.ETL_COLLECTION_PREVIEW_LIMIT)

        records = self.get_query_result(query)
        return records


