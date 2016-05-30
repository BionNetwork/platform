# coding: utf-8
from __future__ import unicode_literals, division
import json

import logging
import random

from itertools import groupby, izip
from datetime import datetime
from djcelery import celery
from django.conf import settings

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


logger = logging.getLogger(__name__)

ASC = 1


@celery.task(name=CREATE_DATASET_MULTI)
def create_dataset_multi(task_id, channel):
    return LoadMongodbMulti(task_id, channel).load_data()


class LoadMongodbMulti(TaskProcessing):
    """
    Создание Dataset
    """

    def processing(self):

        context = self.context
        sub_trees = context['sub_trees']

        # print 'sub_trees', sub_trees

        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        l_service = DataSourceService.get_local_instance()
        ft_names = []

        # FIXME need multiprocessing
        for sub_tree in sub_trees:
            page = 1
            sid = sub_tree['sid']

            source = Datasource.objects.get(id=sid)
            source_service = DataSourceService.get_source_service(source)

            col_names = ['_state', '_date']
            col_names += sub_tree["joined_columns"]

            # FIXME temporary
            cols_updated = False

            # FIXME temporary
            self.key = sub_tree["collection_hash"]

            # создаем коллекцию и индексы в Mongodb
            collection = MongodbConnection(
                self.get_table(MULTI_STTM), indexes=[
                    ('_id', ASC), ('_state', ASC), ('_date', ASC)]
                ).collection

            # Коллекция с текущими данными
            current_collection_name = self.get_table(STTM_DATASOURCE_KEYS)
            MongodbConnection.drop(current_collection_name)
            current_collection = MongodbConnection(
                current_collection_name, indexes=[('_id', ASC)]).collection

            loaded_count = 0

            columns = sub_tree['columns']

            while True:
                rows = source_service.get_source_rows(
                    sub_tree, cols=columns, limit=limit, offset=(page-1)*limit)

                # print 'rows', rows

                if not rows:
                    break
                data_to_insert = []
                data_to_current_insert = []

                for ind, record in enumerate(rows, start=1):

                    # row_key = calc_key_for_row(
                    #     record, tables_key_creator, (page - 1) * limit + ind,
                    #     # FIXME binary
                    #     binary_types_list=None)
                    # # FIXME binary
                    # бинарные данные оборачиваем в Binary(), если они имеются
                    # new_record = process_binary_data(record, binary_types_list)

                    # # FIXME temporary
                    # if not cols_updated:
                    #     cols_updated = True
                    #     col_names += map(str, range(len(record)))

                    # new_record = record

                    # row_key = '%.6f' % random.random()

                    record_normalized = (
                        [STSE.IDLE, EtlEncoder.encode(datetime.now())] +
                        [EtlEncoder.encode(rec_field) for rec_field in record])

                    data_to_insert.append(dict(izip(col_names, record_normalized)))
                    # data_to_current_insert.append(dict(_id=row_key))
                # try:
                collection.insert_many(data_to_insert, ordered=False)
                # current_collection.insert_many(
                #     data_to_current_insert, ordered=False)
                loaded_count += ind
                print 'inserted %d rows to mongodb. Total inserted %s/%s.' % (
                    ind, loaded_count, 'rows_count')
                # except Exception as e:
                #     print e.message
                #     self.error_handling(e.message)

                page += 1

                # FIXME у файлов прогон 1 раз
                if sub_tree['type'] == 'file':
                    break

            l_service.create_foreign_table(
                self.get_table(MULTI_STTM),
                self.context['sub_trees'][0]['columns_types'])

            ft_names.append(self.get_table(MULTI_STTM))
        l_service.create_postgres_server()
        l_service.create_materialized_view('my_view', ft_names)



        # self.next_task_params = (
        #     MONGODB_DATA_LOAD, load_mongo_db, self.context)
