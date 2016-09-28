# coding: utf-8


import os
import requests

from etl.constants import *
from etl.services.datasource.base import DataSourceService


class WareHouse(object):
    """
    Базовый класс, описывающий хранилище данных
    """

    def __init__(self, context):
        """
        Args:
            context(dict): Контекст выполнения
        """
        self.context = context

    def run(self):
        raise NotImplementedError


class ClickHouse(WareHouse):
    """
    Загрузка в ClickHouse
    """

    field_map = {
        'text': 'String',
        'integer': 'Int64',
        'datetime': 'DateTime',
        # FIXME TEMP problems with dates
        # 'timestamp': 'DateTime',
        'timestamp': 'Date',
        'double precision': 'Float64',
        'date': 'Date',
    }

    def __init__(self, context, file_path='/tmp/',
                 db_url='http://localhost:8123/'):
        self.context = context
        self.db_url = db_url
        self.file_path = file_path
        self.table_name = self.context["cube_id"]
        super(ClickHouse, self).__init__(context=context)

    def create_csv(self):
        """
        Создание csv-файла из запроса в Postgres
        """
        file_name = self.table_name
        local_service = DataSourceService.get_local_instance()
        local_service.create_sttm_select_query(
            self.file_path, file_name, self.context['relations'])

    def create_table(self):
        """
        Запрос на создание таблицы в Сlickhouse
        """
        col_types = []

        for tree in self.context['sub_trees']:
            for col in tree['columns']:
                col_types.append('{0} {1}'.format(
                    col['click_column'], self.field_map[col['type']]))

        drop_query = """DROP TABLE IF EXISTS t_{table_name}""".format(
            table_name=self.table_name)

        create_query = """CREATE TABLE {table_name} ({columns}) engine = Log
            """.format(
            table_name=self.context['warehouse'],
            columns=','.join(col_types))

        print(create_query)

        self._send([drop_query, create_query])

    def load_csv(self):
        """
        Загрузка данных из csv в Clickhouse
        """
        os.system(
            """
            cat /tmp/{file}.csv |
            clickhouse-client --query="INSERT INTO t_{table} FORMAT CSV"
            """.format(
                file=self.table_name, table=self.table_name))

    def _send(self, data, settings=None, stream=False):
        """
        """
        for query in data:
            r = requests.post(self.db_url, data=query, stream=stream)
            if r.status_code != 200:
                raise Exception(r.text)

    def run(self):
        self.create_csv()
        self.create_table()
        self.load_csv()


class PostgresWarehouse(WareHouse):
    """
    Загрузка конечных данных в Postgres
    """

    def create_table(self):
        warehouse = self.context['warehouse']
        local_service = DataSourceService.get_local_instance()
        local_service.create_posgres_warehouse(
            warehouse=warehouse, relations=self.context['relations'])

    def run(self):
        self.create_table()


class MaterializedView(WareHouse):
    """
    Класс описывает материализованное представление.

    !!Загрузка в материализованное представление на данный момент является
    альтернативой загрузки в clickhouse
    """

    def run(self):
        # fixme needs to check status of all subtasks
        # если какой нить таск упал, то сюда не дойдет
        # нужны декораторы на обработку ошибок
        local_service = DataSourceService.get_local_instance()
        cube_key = self.context["cube_key"]
        local_service.create_materialized_view(
            DIMENSIONS_MV.format(cube_key),
            MEASURES_MV.format(cube_key),
            self.context['relations']
        )

    print('MEASURES AND DIMENSIONS ARE MADE!')
