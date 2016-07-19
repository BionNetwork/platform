# coding: utf-8
from __future__ import unicode_literals, division

import os
import sys
import json
import time
import logging
import random

from django.conf import settings

from itertools import groupby, izip
from datetime import datetime
from djcelery import celery
from celery import Celery, chord, chain
from kombu import Queue

from etl.constants import *
from etl.services.datasource.base import DataSourceService
from etl.services.queue.base import *
from etl.services.db.factory import DatabaseService, LocalDatabaseService
from etl.services.file.factory import FileService
from etl.helpers import *
from core.models import (
    Datasource, Dimension, Measure, DatasourceMeta,
    DatasourceMetaKeys, DatasourceSettings, Dataset, DatasetToMeta,
    DatasetStateChoices, DatasourcesTrigger, DatasourcesJournal)
from etl.services.queue.base import *
from etl.services.middleware.base import EtlEncoder

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

logger = logging.getLogger(__name__)

ASC = 1


# FIXME chord на amqp бомбашится, паэтаму redis
app = Celery('multi', backend='redis://localhost:6379/0',
                   broker='redis://localhost:6379/0')


@app.task(name=CREATE_DATASET_MULTI)
def create_dataset_multi(task_id, channel, context):
    return CreateDatasetMulti(task_id, channel, context).load_data()


@app.task(name=MONGODB_DATA_LOAD_MULTI)
def load_mongo_db_multi(task_id, channel, context):
    return LoadMongodbMulti(task_id, channel, context).load_data()


class CreateDatasetMulti(TaskProcessing):
    """
    Создание Dataset
    """
    def processing(self):
        self.next_task_params = (load_mongo_db_multi, self.context)


class LoadMongodbMulti(TaskProcessing):
    """
    Создание Dataset
    """

    def processing(self):

        sub_trees = self.context['sub_trees']
        local_db_service = DataSourceService.get_local_instance()
        #
        # параллель из последований, в конце колбэк
        local_db_service.create_date_tables(
            "time_table_name", json.loads(self.context['meta_info']), False
        )

        # chord(
        #     chain(
        #         load_to_mongo.subtask((sub_tree, )),
        #         # получает, то что вернул load_to_mongo
        #         create_foreign_table.subtask(),
        #         create_view.subtask()
        #     )
        #     for sub_tree in sub_trees)(
        #         mongo_callback.subtask(self.context))

        local_db_service.create_date_tables(
            "time_table_name", json.loads(self.context['meta_info']), False)
        for sub_tree in sub_trees:
            load_to_mongo(sub_tree)
            create_foreign_table(sub_tree)
            create_view(sub_tree)

        mongo_callback(self.context)


@app.task(name=MONGODB_DATA_LOAD_MONO)
def load_to_mongo(sub_tree):
    """
    """
    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

    page = 1
    sid = sub_tree['sid']

    source = Datasource.objects.get(id=sid)
    source_service = DataSourceService.get_source_service(source)

    col_names = ['_state', '_date']
    col_names += sub_tree["joined_columns"]

    # FIXME temporary
    cols_updated = False

    # FIXME temporary
    key = sub_tree["collection_hash"]

    # создаем коллекцию и индексы в Mongodb
    collection = MongodbConnection(
        '{0}_{1}'.format(STTM, key), indexes=[
            ('_id', ASC), ('_state', ASC), ('_date', ASC)]
        ).collection

    # Коллекция с текущими данными
    current_collection_name = '{0}_{1}'.format(STTM_DATASOURCE_KEYS, key)
    MongodbConnection.drop(current_collection_name)

    loaded_count = 0

    columns = sub_tree['columns']

    while True:
        rows = source_service.get_source_rows(
            sub_tree, cols=columns, limit=limit, offset=(page-1)*limit)

        if not rows:
            break
        data_to_insert = []

        for ind, record in enumerate(rows, start=1):

            record_normalized = (
                [STSE.IDLE, EtlEncoder.encode(datetime.now())] +
                # [EtlEncoder.encode(rec_field) for rec_field in record])
                [rec_field for rec_field in record])

            data_to_insert.append(dict(izip(col_names, record_normalized)))
        # try:
        collection.insert_many(data_to_insert, ordered=False)
        loaded_count += ind
        print 'inserted %d rows to mongodb. Total inserted %s/%s.' % (
            ind, loaded_count, 'rows_count')
        # except Exception as e:
        #     print e.message
        #     self.error_handling(e.message)

        page += 1

        # FIXME у файлов прогон 1 раз
        # Fixme not true
        if sub_tree['type'] == 'file':
            break

    # ft_names.append(self.get_table(MULTI_STTM))

    return sub_tree


@app.task(name=PSQL_FOREIGN_TABLE)
def create_foreign_table(sub_tree):
    """
    Создание Удаленной таблицы
    """

    fdw = ForeignDataWrapper(tree=sub_tree, is_mongodb=True)
    fdw.create()

@app.task(name=PSQL_VIEW)
def create_view(sub_tree):
    """
    Создание View для Foreign Table со ссылкой на Дату
    Args:
        sub_tree(): Описать

    Returns:
    """

    local_service = DataSourceService.get_local_instance()

    local_service.create_foreign_view(sub_tree)


@app.task(name=MONGODB_DATA_LOAD_CALLBACK)
def mongo_callback(context):
    """
    Работа после создания
    """
    # fixme needs to check status of all subtasks
    # если какой нить таск упал, то сюда не дойдет
    # нужны декораторы на обработку ошибок
    local_service = DataSourceService.get_local_instance()
    local_service.create_materialized_view(
        'my_view4', context['relations'])

    print 'results', context


class UpdateMongodb(TaskProcessing):

    def processing(self):
        """
        1. Процесс обновленения данных в коллекции `sttm_datasource_delta_{key}`
        новыми данными с помощью `sttm_datasource_keys_{key}`
        2. Создание коллекции `sttm_datasource_keys_{key}` c ключами для
        текущего состояния источника
        """

        Dataset.update_state(
            self.context['dataset_id'], DatasetStateChoices.FILLUP)

        self.key = self.context['checksum']
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
        structure = self.context['tree']
        source = Datasource.objects.get(id=self.context['source']['id'])
        meta_info = json.loads(self.context['meta_info'])

        source_service = DataSourceService.get_source_service(source)
        rows_count, loaded_count = source_service.get_structure_rows_number(
            structure,  cols), 0

        # общее количество строк в запросе
        self.publisher.rows_count = rows_count
        self.publisher.publish(TLSE.START)

        col_names = ['_id', '_state', '_date']
        for t_name, col_group in groupby(cols, lambda x: x["table"]):
            for x in col_group:
                col_names.append(
                    STANDART_COLUMN_NAME.format(x["table"], x["col"]))

        # находим бинарные данные для 1) создания ключей 2) инсерта в монго
        binary_types_list = get_binary_types_list(cols, col_types)

        collection = MongodbConnection(self.get_table(STTM_DATASOURCE)).collection

        # Коллекция с текущими данными
        current_collection_name = self.get_table(STTM_DATASOURCE_KEYS)
        MongodbConnection.drop(current_collection_name)
        current_collection = MongodbConnection(
            current_collection_name, indexes=[('_id', ASC)]).collection

        # Дельта-коллекция
        delta_collection = MongodbConnection(
            self.get_table(STTM_DATASOURCE_DELTA), indexes=[
                ('_id', ASC), ('_state', ASC), ('_date', ASC)]).collection

        tables_key_creator = [
            RowKeysCreator(table=table, cols=cols, meta_data=value)
            for table, value in meta_info.iteritems()]

        # Выявляем новые записи в базе и записываем их в дельта-коллекцию
        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        page = 1
        while True:
            rows = source_service.get_source_rows(
                structure, cols, limit=limit, offset=(page-1)*limit)
            if not rows:
                break
            data_to_insert = []
            data_to_current_insert = []
            for ind, record in enumerate(rows, start=1):
                row_key = calc_key_for_row(
                    record, tables_key_creator, (page - 1) * limit + ind,
                    binary_types_list)

                # бинарные данные оборачиваем в Binary(), если они имеются
                new_record = process_binary_data(record, binary_types_list)

                if not collection.find({'_id': row_key}).count():
                    delta_rows = (
                        [row_key, DTSE.NEW, EtlEncoder.encode(datetime.now())] +
                        [EtlEncoder.encode(rec_field) for rec_field in new_record])
                    data_to_insert.append(dict(izip(col_names, delta_rows)))

                data_to_current_insert.append(dict(_id=row_key))
            loaded_count += ind
            print 'updated %d rows to mongodb. Total inserted %s/%s.' % (
                ind, loaded_count, rows_count)

            try:
                if data_to_insert:
                    delta_collection.insert_many(data_to_insert, ordered=False)
                current_collection.insert_many(
                    data_to_current_insert, ordered=False)
            except Exception as e:
                self.error_handling(e.message)
            page += 1

        # Обновляем основную коллекцию новыми данными
        page = 1
        while True:
            delta_data = delta_collection.find(
                {'_state': DTSE.NEW},
                limit=limit, skip=(page-1)*limit).sort('_date', ASC)
            to_ins = []
            for record in delta_data:
                record['_state'] = STSE.IDLE
                to_ins.append(record)
            if not to_ins:
                break
            try:
                collection.insert_many(to_ins, ordered=False)
            except Exception as e:
                self.error_handling(e.message)

            page += 1

        # Обновляем статусы дельты-коллекции
        delta_collection.update_many(
            {'_state': DTSE.NEW}, {'$set': {'_state': DTSE.SYNCED}})

        self.context['rows_count'] = rows_count
        # self.next_task_params = (DB_DATA_LOAD, load_db, self.context)


class ForeignDataWrapper(object):
    """
    Закачка данных из сторонней базы (db, file, mongodb)

    Attributes:
        tree(dict): Мета-информация об извлекаемых данных
        service(`LocalDatabaseService`): объект сервиса работы с локальной БД
        is_mongodb(bool): Привязка "удаленной таблицы" к кокальному mongodb
    """

    def __init__(self, tree, is_mongodb=False):
        """
        Args:
            tree(dict): Мета-информация об извлекаемых данных
            is_mongodb(bool): Привязка "удаленной таблицы" к кокальному mongodb
        """

        self.tree = tree
        self.is_mongodb = is_mongodb
        self.service = LocalDatabaseService()

    @property
    def table(self):
        """
        Название создаваемой таблицы
        """
        key = self.tree["collection_hash"]
        return '{0}_{1}'.format(STTM, key)

    @property
    def server_name(self):
        """
        Название создаваемого сервера
        """
        key = self.tree["collection_hash"]
        return '{0}_{1}'.format(FDW, key)

    def generate_params(self, source=None):
        """
        Подготовка контекста для создания "удаленной таблицы"
        Args:
            source(`Datasource`): объект источника

        Returns:
            dict: данные об источнике
        """
        if self.is_mongodb:
            return {
                "source_type": MONGODB,
                "connection": {
                    "address": '127.0.0.1',
                    'port': '27017',
                },
                "user": {
                    'user': 'bi_user',
                    'password': 'bi_user'
                }
            }

        if not source:
            raise Exception(u'Не указан источник')

        return {
            "source_type": source.conn_type,
            "connection": {
                "host": source.host,
                'port': source.port,
            },
            "user": {
                'user': source.login,
                'password': source.password
            }
        }

    def create_server(self, source_params):
        """
        Создание fdw-сервера

        Args:
            source_params(dict): Данные для создания сервера
            ::
            'source_params': {
                'source_type': 'Mongodb'
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
        self.service.create_fdw_server(self.server_name, source_params)

    def create_foreign_table(self):

        """
        Создание "удаленной таблицы"
        """

        self.service.create_foreign_table(
            self.server_name, self.table, self.tree['columns_types'])

    def create(self, source=None):
        """
        Процесс создания сервера и таблицы
        Args:
            source(`Datasource`): тип источника
        """
        self.create_server(self.generate_params(source))
        self.create_foreign_table()


# write in console: python manage.py celery -A etl.multitask worker
#                   --loglevel=info --concurrency=10 (--concurrency=10 eventlet)
