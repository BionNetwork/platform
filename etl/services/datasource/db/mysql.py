# coding: utf-8

import MySQLdb

from etl.services.datasource.db.maps import mysql as mysql_map
from etl.services.datasource.db.interfaces import Database


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
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        return mysql_map.remote_table_query

    @staticmethod
    def get_remote_trigger_names(table_name):
        return {
            "trigger_name_0": "cdc_{0}_insert".format(table_name),
            "trigger_name_1": "cdc_{0}_update".format(table_name),
            "trigger_name_2": "cdc_{0}_delete".format(table_name),
        }
