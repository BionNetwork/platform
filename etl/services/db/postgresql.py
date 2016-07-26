# coding: utf-8
from __future__ import unicode_literals
from .interfaces import Database, Operations
from etl.services.db.maps import postgresql as pgsql_map
from collections import defaultdict
from itertools import groupby
import psycopg2


class Postgresql(Database):
    """Управление источником данных Postgres"""

    db_map = pgsql_map

    @staticmethod
    def get_connection(conn_info):
        """
        Получение соединения к базе данных
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
            Возвращает кавычки(") для запроса
        """
        return '\"'

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

    @classmethod
    def get_columns_query(cls, tables_str, source):
        # public - default scheme for postgres
        # FIXME 'public' не всегда есть
        # return cls.db_map.cols_query.format(tables_str, source.db, 'public')
        return cls.db_map.cols_query.format(tables_str, source.db)

    @staticmethod
    def local_table_create_query(key_str, cols_str):
        """
        запрос на создание новой таблицы в локал хранилище
        :param key_str:
        :param cols_str:
        :return:
        """
        create_query = """DROP TABLE IF EXISTS {0} CASCADE; CREATE TABLE {0} ({1})""".format(key_str, cols_str)

        return create_query

    @classmethod
    def check_table_exists_query(cls, table, db):
        """
        Проверка на существование таблицы
        """
        table_exists_query = cls.db_map.check_table_exists.format(table, db)

        return table_exists_query

    @classmethod
    def get_page_select_query(cls, table_name, cols):
        fields_str = '"'+'", "'.join(cols)+'"'
        return cls.db_map.row_query.format(
            fields_str, table_name, '%(limit)s', '%(offset)s')

    @classmethod
    def get_select_dates_query(cls, date_table):
        return cls.db_map.select_dates_query.format(date_table)

    @staticmethod
    def local_table_insert_query(table_name, cols_num):
        """
        Запрос на добавление в новую таблицу локал хранилища

        Args:
            table_name(str): Название таблиц
            cols_num(int): Число столбцов
        Returns:
            str: Строка на выполнение
        """
        cols = '(%s)' % ','.join(['%({0})s'.format(i) for i in xrange(
                cols_num)])
        return "INSERT INTO {0} VALUES {1}".format(table_name, cols)

    @staticmethod
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        return pgsql_map.remote_table_query

    @staticmethod
    def reload_datasource_trigger_query(params):
        """
        запрос на создание триггеров в БД локально для размерностей и мер
        """
        return pgsql_map.dimension_measure_triggers_query.format(**params)

    @staticmethod
    def get_remote_trigger_names(table_name):
        return {
            "trigger_name_0": "cdc_{0}_audit".format(table_name),
        }

    def fdw_server_create_query(self, name, source_params):
        """
        Создание fdw-сервера

        Args:
            name(str): Название сервера
            source_params(dict): Данные для создания сервера
            ::
            'source_params': {
                'source_type': 'mongodb',
                'connection': {
                    'address': '127.0.0.1',
                    'port': '27017'
                    ...
                    },
                'user': {
                    'user': 'bi_user',
                    'password': 'bi_user'
                }
            }

        Returns:
            str: строка запроса для создания fdw-сервера
        """
        fdw_map = {
            1: 'postgres_fdw'
        }

        source_type = source_params['source_type']
        fdw_name = fdw_map[source_type]
        conn_params = ', '.join("{name} '{value}'".format(name=key, value=value)
                                for key, value in source_params['connection'].iteritems())
        query = self.db_map.fdw_server_create_query.format(
            name=name, fdw_name=fdw_name, conn_params=conn_params)
        # Создаем соответствие пользователей удаленной и нашей базой
        if source_params['user']:
            user_params = ', '.join("{name} '{value}'".format(name=key, value=value)
                for key, value in source_params['user'].iteritems())
            query += self.db_map.fdw_mapping_create_query.format(name=name, user_params=user_params)

        return query

    def foreign_table_create_query(self, server_name, options, cols_meta):
        """
        Создание "удаленной таблицы"
        Args:
            server_name(str): Название сервера
            table_name(str): Название таблицы
            cols_meta(dict): Информация о колонках
            ::
            'cols_meta': {
                <column_name>:
                    'type': int,
                    ...
            }

        Returns:
            str: строка запроса для создания удаленной таблицы (foreign table)
        """
        col_names = []
        for field_name, field in cols_meta.iteritems():
            col_names.append(u'"{0}" {1}'.format(
                field_name, field['type']))

        table_name = 'schema_17.sttm__1_8_2675133954039524792'

        options = ', '.join("{name} '{value}'".format(name=key, value=value)
                            for key, value in options.iteritems())

        return self.db_map.foreign_table_create_query.format(
            server_name=server_name, table_name=table_name, options=options, cols=','.join(col_names))

    def create_foreign_view_query(self, view_name, sub_tree):
        """
        Запрос на создание представления
        Args:
            view_name:

        Returns:

        """
        view_name = 'view__{view_hash}'.format(view_hash=sub_tree['collection_hash'])
        table_name = 'sttm__{view_hash}'.format(view_hash=sub_tree['collection_hash'])

        columns = sub_tree['columns_types']

        query_column = []
        time_joins_map = {}
        for index, column in enumerate(columns):
            if columns[column]['type'] in ['date', 'datetime', 'timestamp']:
                time_joins_map.update({index: column})
                s = 't{index}."time_id" as "{column_name}"'.format(index=index, column_name=column)
                query_column.append(s)
            else:
                query_column.append('"{table}"."{column}"'.format(table=table_name, column=column))

        select_line = ', '.join(query_column)

        joins_line = ''
        if time_joins_map:
            for index, column in time_joins_map.iteritems():
                joins_line += 'JOIN time_table_name as t{index} ON "{foreign_table_name}"."{column}"::date = t{index}."the_date"'.format(index=index, foreign_table_name=table_name, column=column)

        query = """DROP VIEW IF EXISTS  {view_name} CASCADE; CREATE VIEW {view_name} AS SELECT {select} FROM {table_name} {joins};""".format(
            view_name=view_name, select=select_line, table_name=table_name, joins=joins_line)
        return query


    def create_materialized_view_query(self, name, relations):
        """
        Запрос на создание материализованного представления

        Args:
            name(str): название представления
            relations(list): Информация об объединяемых таблицах
        """
        table_names = [x['table_hash'] for x in relations]
        query = """DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE; CREATE MATERIALIZED VIEW {view_name} AS SELECT {columns} FROM {first_table_name}""".format(
            view_name=name,
            columns=','.join(['"view__%s".*' % x for x in table_names]),
            first_table_name='"view__%s"' % table_names[0])

        for node in relations[1:]:
            query += u' INNER JOIN "{table_name}" ON {condition}'.format(
                table_name=u'sttm__%s' % node['table_hash'],
                condition=u'{l}{operation}{r}'.format(
                    l=node['conditions'][0]['l'], operation=Operations.values[node['conditions'][0]['operation']], r=node['conditions'][0]['r'])
            )
        return query

    def create_schema_query(self, card_id):

        return self.db_map.create_schema_query.format(card_id=card_id)




