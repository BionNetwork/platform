# coding: utf-8

from .interfaces import Database
import MySQLdb
from collections import defaultdict
from itertools import groupby
from etl.services.db.maps import mysql as mysql_map


class Mysql(Database):
    """Управление источником данных MySQL"""

    db_map = mysql_map

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
        Возвращает кавычки(') для запроса
        """
        return '`'

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

    @classmethod
    def get_statistic_query(cls, source, tables):
        """
        запрос для статистики
        :param source: Datasource
        :param tables: list
        :return: str
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'
        return mysql_map.stat_query.format(tables_str, source.db)

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

    @staticmethod
    def get_primary_key(table, db):
        """
        запрос на получение Primary Key
        """
        return mysql_map.pr_key_query.format("('{0}')".format(table), db)

    @staticmethod
    def delete_primary_query(table, primary):
        return mysql_map.delete_primary_key.format(table, primary)

    @staticmethod
    def get_remote_trigger_names(table_name):
        return {
            "trigger_name_0": "cdc_{0}_insert".format(table_name),
            "trigger_name_1": "cdc_{0}_update".format(table_name),
            "trigger_name_2": "cdc_{0}_delete".format(table_name),
        }
