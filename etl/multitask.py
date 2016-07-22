# coding: utf-8
from __future__ import unicode_literals, division

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

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
from bisect import bisect_left

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

        # FIXME проверить работу таблицы дат
        # создание таблицы дат
        local_db_service = DataSourceService.get_local_instance()
        local_db_service.create_date_tables(
            "time_table_name", json.loads(self.context['meta_info']), False
        )

        # параллель из последований, в конце колбэк
        # chord(
        #     chain(
        #         load_to_mongo.subtask((sub_tree, )),
        #         # получает, то что вернул load_to_mongo
        #         create_foreign_table.subtask(),
        #         create_view.subtask()
        #     )
        #     for sub_tree in sub_trees)(
        #         mongo_callback.subtask(self.context))

        for sub_tree in sub_trees:
            load_to_mongo(sub_tree)
            create_foreign_table(sub_tree)
            create_view(sub_tree)

        mongo_callback(self.context)


@app.task(name=MONGODB_DATA_LOAD_MONO)
def load_to_mongo(sub_tree):
    """
    """
    t1 = time.time()

    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

    page = 1
    sid = sub_tree['sid']

    source = Datasource.objects.get(id=sid)
    source_service = DataSourceService.get_source_service(source)

    col_names = ['_id', '_state', ]
    col_names += sub_tree["joined_columns"]

    key = sub_tree["collection_hash"]

    _ID, _STATE = '_id', '_state'

    # создаем коллекцию и индексы в Mongodb
    collection = MongodbConnection(
        '{0}_{1}'.format(STTM, key),
        indexes=[(_ID, ASC), (_STATE, ASC), ]
        ).collection

    loaded_count = 0

    columns = sub_tree['columns']

    while True:
        rows = source_service.get_source_rows(
            sub_tree, cols=columns, limit=limit, offset=(page-1)*limit)

        if not rows:
            break

        keys = []
        key_records = {}

        for ind, record in enumerate(rows, start=1):
            row_key = simple_key_for_row(record, (page - 1) * limit + ind)
            keys.append(row_key)

            record_normalized = (
                [row_key, TRSE.NEW, ] +
                [EtlEncoder.encode(rec_field) for rec_field in record])

            key_records[row_key] = dict(izip(col_names, record_normalized))

        exist_docs = collection.find({_ID: {'$in': keys}}, {_ID: 1})
        exist_ids = map(lambda x: x[_ID], exist_docs)
        exists_len = len(exist_ids)

        # при докачке, есть совпадения
        if exist_ids:
            exist_ids.sort()

            data_to_insert = []

            for id_ in keys:
                ind = bisect_left(exist_ids, id_)
                if ind == exists_len or not exist_ids[ind] == id_:
                    data_to_insert.append(key_records[id_])

            # смена статусов предыдущих на NEW
            collection.update_many(
                {_ID: {'$in': exist_ids}}, {'$set': {_STATE: TRSE.NEW}, }
            )

        # вся пачка новых
        else:
            data_to_insert = [key_records[id_] for id_ in keys]

        if data_to_insert:
            try:
                collection.insert_many(data_to_insert, ordered=False)
                loaded_count += ind
                print 'inserted %d rows to mongodb. Total inserted %s/%s.' % (
                    ind, loaded_count, 'rows_count')
            except Exception as e:
                print 'Exception', loaded_count, loaded_count + ind, e.message

        page += 1

        # FIXME у файлов прогон 1 раз
        # Fixme not true
        # Fixme подумать над пагинацией
        if sub_tree['type'] == 'file':
            break

    t2 = time.time()
    print 'xrange', t2 - t1

    # удаление всех со статусом PREV
    collection.delete_many({_STATE: TRSE.PREV},)

    # проставление всем статуса PREV
    collection.update_many({}, {'$set': {_STATE: TRSE.PREV}, })

    t3 = time.time()
    print 'xrange2', t3 - t2
    print 'xrange3', t3 - t1

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
#                   --loglevel=info --concurrency=10
#                   (--concurrency=1000 -P (eventlet, gevent)
