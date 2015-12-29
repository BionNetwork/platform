# coding: utf-8
from itertools import groupby, izip
import json
from datetime import datetime
import celery
from celery.contrib.methods import task_method
from django.conf import settings
from pymongo import ASCENDING
from core.models import Datasource
from etl.services.datasource.base import DataSourceService
from etl.services.middleware.base import get_table_name, EtlEncoder
from etl.services.queue.base import (
    TLSE, DTSE, DelTSE, STSE, RowKeysCreator, calc_key_for_row,
    MongodbConnection, DeleteQuery)

from etl.tasks import TaskProcessing
from etl.constants import *


class CreateTriggers(TaskProcessing):

    def processing(self):
        """
        Создание триггеров в БД пользователя
        """
        tables_info = self.context['tables_info']
        db_instance = self.context['db_instance']
        sep = db_instance.get_separator()
        remote_table_create_query = db_instance.remote_table_create_query()
        remote_triggers_create_query = db_instance.remote_triggers_create_query()

        connection = db_instance.connection
        cursor = connection.cursor()

        for table, columns in tables_info.iteritems():

            table_name = '_etl_datasource_cdc_{0}'.format(table)
            cols_str = ''
            new = ''
            old = ''
            cols = ''

            for col in columns:
                name = col['name']
                new += 'NEW.{0}, '.format(name)
                old += 'OLD.{0}, '.format(name)
                cols += ('{name}, '.format(name=name))
                cols_str += ' {sep}{name}{sep} {typ},'.format(
                    sep=sep, name=name, typ=col['type']
                )

            # multi queries of mysql, delimiter $$
            for query in remote_table_create_query.format(
                    table_name, cols_str).split('$$'):
                cursor.execute(query)

            connection.commit()

            trigger_commands = remote_triggers_create_query.format(
                orig_table=table, new_table=table_name, new=new, old=old,
                cols=cols)

            # multi queries of mysql, delimiter $$
            for query in trigger_commands.split('$$'):
                cursor.execute(query)

            connection.commit()


class UpdateMongodb(TaskProcessing):

    @celery.task(name=MONGODB_DELTA_LOAD, filter=task_method)
    def load_data(self):
        return super(UpdateMongodb, self).load_data()

    def processing(self):
        """
        1. Процесс обновленения данных в коллекции `sttm_datasource_delta_{key}`
        новыми данными с помощью `sttm_datasource_keys_{key}`
        2. Создание коллекции `sttm_datasource_keys_{key}` c ключами для
        текущего состояния источника
        """
        self.key = self.context['checksum']
        cols = json.loads(self.context['cols'])
        structure = self.context['tree']
        source_model = Datasource()
        source_model.set_from_dict(**self.context['source'])

        # общее количество строк в запросе
        self.publisher.rows_count = DataSourceService.get_structure_rows_number(
            source_model, structure,  cols)
        self.publisher.publish(TLSE.START)

        col_names = ['_id', '_state', '_date']
        for t_name, col_group in groupby(cols, lambda x: x["table"]):
            for x in col_group:
                col_names.append(x["table"] + FIELD_NAME_SEP + x["col"])

        collection = MongodbConnection().get_collection(
            'etl', get_table_name(STTM_DATASOURCE, self.key))

        # Дельта-коллекция
        delta_mc = MongodbConnection()
        delta_collection = delta_mc.get_collection(
            'etl', get_table_name(STTM_DATASOURCE_DELTA, self.key))
        delta_mc.set_indexes([('_id', ASCENDING), ('_state', ASCENDING),
                         ('_date', ASCENDING)])

        # Коллекция с текущими данными
        current_mc = MongodbConnection()
        current_collection = current_mc.get_collection(
            'etl', get_table_name(STTM_DATASOURCE_KEYS, self.key))
        current_mc.set_indexes([('_id', ASCENDING)])

        query = DataSourceService.get_rows_query_for_loading_task(
            source_model, structure, cols)

        source_connection = DataSourceService.get_source_connection(source_model)

        tables_key_creator = []
        for table, value in json.loads(self.context['meta_info']).iteritems():
            rkc = RowKeysCreator(table=table, cols=cols)
            rkc.set_primary_key(value)
            tables_key_creator.append(rkc)

        #  Выявляем новые записи в базе и записываем их в дельта-коллекцию
        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        page = 1
        while True:
            cursor = source_connection.cursor()
            cursor.execute(query.format(limit, (page-1)*limit))
            result = cursor.fetchall()
            if not result:
                break
            data_to_insert = []
            data_to_current_insert = []
            for ind, record in enumerate(result):
                row_key = calc_key_for_row(
                    record, tables_key_creator, (page-1)*limit + ind)
                if not collection.find({'_id': row_key}).count():
                    delta_rows = (
                        [row_key, DTSE.NEW, EtlEncoder.encode(datetime.now())] +
                        [EtlEncoder.encode(rec_field) for rec_field in record])
                    data_to_insert.append(dict(izip(col_names, delta_rows)))

                data_to_current_insert.append(dict(_id=row_key))

            if data_to_insert:
                delta_collection.insert_many(data_to_insert, ordered=False)
            current_collection.insert_many(data_to_current_insert, ordered=False)
            page += 1

        # Обновляем основную коллекцию новыми данными
        page = 1
        while True:
            delta_data = delta_collection.find(
                {'_state': DTSE.NEW},
                limit=limit, skip=(page-1)*limit).sort('_date', ASCENDING)
            to_ins = []
            for record in delta_data:
                record['_state'] = STSE.IDLE
                to_ins.append(record)
            if not to_ins:
                break
            collection.insert_many(to_ins, ordered=False)
            page += 1

        # Обновляем статусы дельты-коллекции
        delta_collection.update_many(
            {'_state': DTSE.NEW}, {'$set': {'_state': DTSE.SYNCED}})


class DetectRedundant(TaskProcessing):

    @celery.task(name=DB_DETECT_REDUNDANT, filter=task_method)
    def load_data(self):
        return super(DetectRedundant, self).load_data()

    def processing(self):
        """
        Выявление записей на удаление
        """
        self.key = self.context['checksum']
        source_collection = MongodbConnection().get_collection(
                    'etl', get_table_name(STTM_DATASOURCE, self.key))
        current_collection = MongodbConnection().get_collection(
                    'etl', get_table_name(STTM_DATASOURCE_KEYS, self.key))

        # Дельта-коллекция
        delete_mc = MongodbConnection()
        delete_collection = delete_mc.get_collection(
            'etl', get_table_name(STTM_DATASOURCE_KEYSALL, self.key))
        delete_mc.set_indexes([('_state', ASCENDING),
                         ('_deleted', ASCENDING)])

        source_collection.aggregate(
            [{"$match": {"_state": STSE.LOADED}},
             {"$project": {"_id": "$_id", "_state": {"$literal": "new"}}},
             {"$out": "%s" % get_table_name(STTM_DATASOURCE_KEYSALL, self.key)}])
        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        page = 1
        while True:

            to_delete = []
            del_cursor = list(delete_collection.find(
                    {'_state': DelTSE.NEW}, limit=limit, skip=(page-1)*limit))
            if not len(del_cursor):
                break
            for record in del_cursor:
                row_key = record['_id']
                if not current_collection.find({'_id': row_key}).count():
                    to_delete.append(row_key)

            delete_collection.update_many(
                {'_id': {'$in': to_delete}},
                {'$set': {'_state': DelTSE.DELETED}})

            source_collection.delete_many({'_id': {'$in': to_delete}})

            page += 1


class DeleteRedundant(TaskProcessing):

    @celery.task(name=DB_DELETE_REDUNDANT, filter=task_method)
    def load_data(self):
        return super(DeleteRedundant, self).load_data()

    def processing(self):
        self.key = self.context['checksum']
        del_collection = MongodbConnection().get_collection(
                    'etl', get_table_name(STTM_DATASOURCE_KEYSALL, self.key))

        source_table_name = get_table_name(STTM_DATASOURCE, self.key)
        delete_query = DeleteQuery(DataSourceService())
        delete_query.set_query(
            table_name=source_table_name)

        limit = 100
        page = 1
        while True:
            delete_delta = del_collection.find(
                {'_state': DelTSE.DELETED},
                limit=limit, skip=(page-1)*limit)
            l = [record['_id'] for record in delete_delta]
            if not l:
                break
            delete_query.execute(keys=l)
            page += 1
