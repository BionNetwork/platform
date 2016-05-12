# coding: utf-8
from __future__ import unicode_literals, division


import logging
import random

from itertools import groupby, izip
from datetime import datetime
from djcelery import celery
from django.conf import settings

from etl.constants import *
from etl.services.queue.base import *
from etl.services.db.factory import DatabaseService
from etl.services.file.factory import FileService
from etl.helpers import DataSourceService
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
    return CreateDatasetMulti(task_id, channel).load_data()


class CreateDatasetMulti(TaskProcessing):
    """
    Создание Dataset
    """

    def processing(self):

        context = self.context
        sub_trees = context['sub_trees']

        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

        for sub_tree in sub_trees:
            page = 1
            sid = sub_tree['sid']
            table = sub_tree['val']

            source = Datasource.objects.get(id=sid)
            source_service = DataSourceService.get_source_service(source)

            if isinstance(source_service, DatabaseService):
                print sid, sub_tree

                col_names = ['_id', '_state', '_date', '1', '2', '3', '4', '5', '6']

                # создаем коллекцию и индексы в Mongodb
                collection = MongodbConnection(
                    self.get_table(STTM_DATASOURCE), indexes=[
                        ('_id', ASC), ('_state', ASC), ('_date', ASC)]
                ).collection

                # Коллекция с текущими данными
                current_collection_name = self.get_table(STTM_DATASOURCE_KEYS)
                MongodbConnection.drop(current_collection_name)
                current_collection = MongodbConnection(
                    current_collection_name, indexes=[('_id', ASC)]).collection

                tables_key_creator = [
                    RowKeysCreator(table=table, cols=[]), ]

                loaded_count = 0

                while True:
                    rows = source_service.get_source_rows(
                        sub_tree, cols=[], limit=limit, offset=(page-1)*limit)

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
                        new_record = record

                        row_key = '%.6f' % random.random()

                        record_normalized = (
                            [row_key, STSE.IDLE, EtlEncoder.encode(datetime.now())] +
                            [EtlEncoder.encode(rec_field) for rec_field in new_record])

                        print 'record', record_normalized

                        data_to_insert.append(dict(izip(col_names, record_normalized)))
                        data_to_current_insert.append(dict(_id=row_key))
                    # try:
                    collection.insert_many(data_to_insert, ordered=False)
                    current_collection.insert_many(
                        data_to_current_insert, ordered=False)
                    loaded_count += ind
                    print 'inserted %d rows to mongodb. Total inserted %s/%s.' % (
                        ind, loaded_count, 'rows_count')
                    # except Exception as e:
                    #     self.error_handling(e.message)

                    page += 1

            elif isinstance(source_service, FileService):
                print 'file'

        # self.next_task_params = (
        #     MONGODB_DATA_LOAD, load_mongo_db, self.context)