# coding: utf-8
from .interfaces import Database
from etl.services.db.maps import postgresql as pgsql_map
from collections import defaultdict
from itertools import groupby
import psycopg2


class Postgresql(Database):
    """Управление источником данных Postgres"""

    @staticmethod
    def get_connection(conn_info):
        """
        возвращает коннект бд
        :param conn_info: dict
        :return: connection
        """
        try:
            conn_str = (u"host='{host}' dbname='{db}' user='{login}' "
                        u"password='{password}' port={port}").format(**conn_info)
            conn = psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            return None
        return conn

    @staticmethod
    def get_separator():
        """
            Возвращает ковычки(") для запроса
        """
        return '\"'

    def get_tables(self, source):
        """
        возвращает таблицы соурса
        :param source: Datasource
        :return: list
        """
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='public' order by table_name;
        """
        records = self.get_query_result(query)
        records = map(lambda x: {'name': x[0], },
                      sorted(records, key=lambda y: y[0]))

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

        select_query = self.get_select_query().format(
            cols_str, query_join)

        explain_query = 'explain analyze ' + select_query
        records = self.get_query_result(explain_query)
        data = records[0][0].split()
        count = None
        for d in data:
            if d.startswith('rows='):
                count = d
        return int(count[5:])

    @staticmethod
    def _get_columns_query(source, tables):
        """
        запросы для колонок, констраинтов, индексов соурса
        :param source: Datasource
        :param tables:
        :return: tuple
        """
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        # public - default scheme for postgres
        cols_query = pgsql_map.cols_query.format(tables_str, source.db, 'public')

        constraints_query = pgsql_map.constraints_query.format(tables_str)

        indexes_query = pgsql_map.indexes_query.format(tables_str)

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
                        index_name = i['name']
                        for c in t_consts:
                            const_type = c['c_type']
                            if index_name == c['c_name']:
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
                                     "extra": (
                                         'serial' if extra is not None and
                                         extra.startswith('nextval')
                                         else extra),
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
        tables_str = '(' + ', '.join(["'{0}'::regclass::oid".format(y) for y in tables]) + ')'
        stats_query = pgsql_map.stat_query.format(tables_str)
        return stats_query

    @staticmethod
    def local_table_create_query(key_str, cols_str):
        """
        запрос на создание новой таблицы в локал хранилище
        :param key_str:
        :param cols_str:
        :return:
        """
        create_query = "CREATE TABLE {0} ({1})".format(key_str, cols_str)

        return create_query

    @staticmethod
    def local_table_insert_query(key_str):
        """
        запрос на инсерт в новую таблицу локал хранилища
        :param key_str:
        :return:
        """
        insert_query = "INSERT INTO {0} VALUES {1}".format(key_str, '{0}')
        return insert_query

    @staticmethod
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        return pgsql_map.remote_table_query

    @staticmethod
    def remote_triggers_create_query():
        """
        запрос на создание триггеров в БД клиента
        """
        return pgsql_map.remote_triggers_query
