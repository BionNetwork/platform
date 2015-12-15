# coding: utf-8

from .interfaces import Database
import MySQLdb
from collections import defaultdict
from itertools import groupby
from etl.services.db.maps import mysql as mysql_map


class Mysql(Database):
    """Управление источником данных MySQL"""

    @staticmethod
    def get_connection(conn_info):
        """
        connection бд соурса
        :param conn_info:
        :return: connection
        """
        try:
            connection = {'db': str(conn_info['db']),
                          'host': str(conn_info['host']),
                          'port': int(conn_info['port']),
                          'user': str(conn_info['login']),
                          'passwd': str(conn_info['password']),
                          'use_unicode': True,
                          }
            conn = MySQLdb.connect(**connection)
        except MySQLdb.OperationalError:
            return None
        return conn

    @staticmethod
    def get_separator():
        """
            Возвращает ковычки(') для запроса
        """
        return '`'

    def get_tables(self, source):
        """
        Получение списка таблиц
        :param source:
        :return:
        """
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='{0}' order by table_name;
        """.format(source.db)

        records = self.get_query_result(query)
        records = map(lambda x: {'name': x[0], }, records)

        return records

    def get_structure_rows_number(self, structure, cols):

        """
        возвращает примерное кол-во строк в запросе для планирования
        :param structure:
        :param cols:
        :return:
        """
        separator = self.get_separator()
        query_join = self.generate_join(structure)

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)
        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        select_query = self.get_select_query()

        explain_query = 'explain ' + select_query.format(
            cols_str, query_join)

        records = self.get_query_result(explain_query)
        total = 1
        for rec in records:
            total *= int(rec[8])
        # если total меньше 100000, то делаем count запрос
        # иначе возвращаем перемноженное кол-во в каждой строке,
        # возвращенной EXPLAIN-ом
        if total < 100000:
            rows_count_query = select_query.format(
                'count(1) ', query_join)
            records = self.get_query_result(rows_count_query)
            # records = ((100L,),)
            return int(records[0][0])
        return total

    @staticmethod
    def _get_columns_query(source, tables):
        """
        запросы для колонок, констраинтов, индексов соурса
        :param source: Datasource
        :param tables:
        :return: tuple
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        cols_query = mysql_map.cols_query.format(tables_str, source.db)

        constraints_query = mysql_map.constraints_query.format(tables_str, source.db)

        indexes_query = mysql_map.indexes_query.format(tables_str, source.db)

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
        index_records = self.get_query_result(indexes_query)
        const_records = self.get_query_result(consts_query)

        return col_records, index_records, const_records

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

        table_name, col_name, col_type, is_nullable, extra_ = xrange(5)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]
                extra = x[extra_]

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

                columns[key].append({"name": col,
                                     "type": x[col_type],
                                     "is_index": is_index,
                                     "is_unique": is_unique,
                                     "is_primary": is_primary,
                                     "is_nullable": x[is_nullable].lower(),
                                     "extra": extra,
                                     })

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'FOREIGN KEY':
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
        query = "SELECT {0} FROM {1} LIMIT {2} OFFSET {3};"
        return query

    @staticmethod
    def get_select_query():
        """
        возвращает селект запрос
        :return: str
        """
        query = "SELECT {0} FROM {1};"
        return query

    @staticmethod
    def get_statistic_query(source, tables):
        """
        запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'
        stats_query = mysql_map.stat_query.format(tables_str, source.db)
        return stats_query

    @staticmethod
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        return mysql_map.remote_table_query

    @staticmethod
    def remote_triggers_create_query():
        """
        запрос на создание триггеров в БД клиента
        """
        return mysql_map.remote_triggers_query

