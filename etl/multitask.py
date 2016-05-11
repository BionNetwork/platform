# coding: utf-8
from __future__ import unicode_literals, division

from djcelery import celery

from etl.constants import *
from etl.services.queue.base import *
from etl.services.db.factory import DatabaseService
from etl.services.file.factory import FileService
from etl.helpers import DataSourceService
from core.models import (
    Datasource, Dimension, Measure, DatasourceMeta,
    DatasourceMetaKeys, DatasourceSettings, Dataset, DatasetToMeta,
    DatasetStateChoices, DatasourcesTrigger, DatasourcesJournal)


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

        for sub_tree in sub_trees:
            sid = sub_tree['sid']
            source = Datasource.objects.get(id=sid)
            source_service = DataSourceService.get_source_service(source)
            print sid, source_service

            if isinstance(source_service, DatabaseService):
                print 'db'
            elif isinstance(source_service, FileService):
                print 'file'

        # self.next_task_params = (
        #     MONGODB_DATA_LOAD, load_mongo_db, self.context)
