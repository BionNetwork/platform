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
from etl.services.db.factory import DatabaseService
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
        # # параллель из последований, в конце колбэк
        # header = [chain(
        #         load_to_mongo.subtask((sub_tree, )),
        #         # получает, то что вернул load_to_mongo
        #         create_foreign_table.subtask()
        #     )
        #     for sub_tree in sub_trees]
        # header.append(local_db_service.create_date_tables(
        #     "time_table_name", json.loads(self.context['meta_info']), self.context['db_update']))
        #
        # chord(
        #     chain(
        #         load_to_mongo.subtask((sub_tree, )),
        #         # получает, то что вернул load_to_mongo
        #         create_foreign_table.subtask()
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
    Создание таблиц Postgres
    """
    key = sub_tree["collection_hash"]
    local_service = DataSourceService.get_local_instance()

    local_service.create_foreign_table(
        '{0}_{1}'.format(STTM, key), sub_tree['columns_types'])

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
    local_service = DataSourceService.get_local_instance()
    local_service.create_materialized_view(
        'my_view4', context['relations'])

    print 'results', context


# write in console: python manage.py celery -A etl.multitask worker
#                   --loglevel=info --concurrency=10 (--concurrency=10 eventlet)
