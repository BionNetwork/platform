# coding: utf-8

from __future__ import unicode_literals

import logging

import os
import sys
import lxml.etree as etree
import brukva
from datetime import datetime
import json
from operator import itemgetter

# from pymongo import ASCENDING
from psycopg2 import errorcodes
from etl.constants import *
from etl.services.db.factory import DatabaseService
from etl.services.middleware.base import (
    EtlEncoder, get_table_name)
from etl.services.olap.base import send_xml
from etl.services.queue.base import TLSE,  STSE, RPublish, RowKeysCreator, \
    calc_key_for_row, TableCreateQuery, InsertQuery, MongodbConnection, \
    DeleteQuery, AKTSE, DTSE, get_single_task, get_binary_types_list,\
    process_binary_data, get_binary_types_dict
from .helpers import (RedisSourceService, DataSourceService,
                      TaskService, TaskStatusEnum,
                      TaskErrorCodeEnum)
from core.models import (
    Datasource, Dimension, Measure, QueueList, DatasourceMeta,
    DatasourceMetaKeys, DatasourceSettings, Dataset, DatasetToMeta, Cube)
from django.conf import settings

from djcelery import celery
from itertools import groupby, izip

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

client = brukva.Client(host=settings.REDIS_HOST,
                       port=int(settings.REDIS_PORT),
                       selected_db=settings.REDIS_DB)
client.connect()

logger = logging.getLogger(__name__)

ASCENDING = 1


class TaskProcessing(object):
    """
    Базовый класс, отвечающий за про процесс выполнения celery-задач

    Attributes:
        task_id(int): id задачи
        channel(str): Канал передачи на клиент
        last_task(bool): Флаг последней задачи
        user_id(str): id пользователя
        context(dict): контекстные данные для задачи
        was_error(bool): Факт наличия ошибки
        err_msg(str): Текст ошибки, если случилась
        publisher(`RPublish`): Посыльный к клиенту о текущем статусе задачи
        queue_storage(`QueueStorage`): Посыльный к redis о текущем статусе задачи
        key(str): Ключ
        next_task_params(tuple): Набор данных для след. задачи
    """

    def __init__(self, task_id, channel, last_task=False):
        """
        Args:
            task_id(int): id задачи
            channel(str): Канал передачи на клиент
        """
        self.task_id = task_id
        self.channel = channel
        self.last_task=last_task
        self.user_id = None
        self.context = None
        self.was_error = False
        self.err_msg = ''
        self.publisher = RPublish(self.channel, self.task_id)
        self.queue_storage = None
        self.key = None
        self.next_task_params = None

    def prepare(self):
        """
        Подготовка к задаче:
            1. Получение контекста выполнения
            2. Инициализация служб, следящие за процессов выполения
        """
        task = QueueList.objects.get(id=self.task_id)
        self.context = json.loads(task.arguments)
        self.key = task.checksum
        self.user_id = self.context['user_id']
        self.queue_storage = TaskService.get_queue(self.task_id, self.user_id)
        TaskService.update_task_status(self.task_id, TaskStatusEnum.PROCESSING)

    def load_data(self):
        """
        Точка входа
        """
        self.prepare()
        try:
            self.processing()
        except Exception as e:
            # В любой непонятной ситуации меняй статус задачи на ERROR
            TaskService.update_task_status(
                self.task_id, TaskStatusEnum.ERROR,
                error_code=TaskErrorCodeEnum.DEFAULT_CODE,
                error_msg=e.message)
            self.publisher.publish(TLSE.ERROR, msg=e.message)
            RedisSourceService.delete_queue(self.task_id)
            RedisSourceService.delete_user_subscriber(
                self.user_id, self.task_id)
            logger.exception(self.err_msg)
            raise
        self.exit()
        if self.next_task_params:
            get_single_task(self.next_task_params)

    def processing(self):
        """
        Непосредственное выполнение задачи
        """
        raise NotImplementedError

    def error_handling(self, err_msg, err_code=None):
        """
        Обработка ошибки

        Args:
            err_msg(str): Текст ошибки
            err_code(str): Код ошибки
        """
        self.was_error = True
        # fixme перезаписывается при каждой ошибке
        self.err_msg = err_msg
        TaskService.update_task_status(
            self.task_id, TaskStatusEnum.ERROR,
            error_code=err_code or TaskErrorCodeEnum.DEFAULT_CODE,
            error_msg=self.err_msg)

        self.queue_storage['status'] = TaskStatusEnum.ERROR

        # сообщаем об ошибке
        self.publisher.publish(TLSE.ERROR, self.err_msg)
        logger.exception(self.err_msg)

    def exit(self):
        """
        Корректное завершение вспомогательных служб
        """
        if self.was_error:
            # меняем статус задачи на 'Ошибка'
            TaskService.update_task_status(
                self.task_id, TaskStatusEnum.ERROR,
                error_code=TaskErrorCodeEnum.DEFAULT_CODE,
                error_msg=self.err_msg)
            if not self.publisher.is_complete:
                self.publisher.publish(TLSE.FINISH)

        else:
            # меняем статус задачи на 'Выполнено'
            TaskService.update_task_status(self.task_id, TaskStatusEnum.DONE, )
            self.queue_storage.update(TaskStatusEnum.DONE)

        # удаляем инфу о работе таска
        RedisSourceService.delete_queue(self.task_id)
        # удаляем канал из списка каналов юзера
        RedisSourceService.delete_user_subscriber(self.user_id, self.task_id)


@celery.task(name=CREATE_DATASET)
def create_dataset(task_id, channel):
    return CreateDataset(task_id, channel).load_data()


@celery.task(name=MONGODB_DATA_LOAD)
def load_mongo_db(task_id, channel):
    return LoadMongodb(task_id, channel).load_data()


@celery.task(name=DB_DATA_LOAD)
def load_db(task_id, channel):
    return LoadDb(task_id, channel).load_data()


@celery.task(name=GENERATE_DIMENSIONS)
def load_dimensions(task_id, channel):
    return LoadDimensions(task_id, channel).load_data()


@celery.task(name=GENERATE_MEASURES)
def load_measures(task_id, channel):
    return LoadMeasures(task_id, channel).load_data()


@celery.task(name=MONGODB_DELTA_LOAD)
def update_mongo_db(task_id, channel):
    return UpdateMongodb(task_id, channel).load_data()


@celery.task(name=DB_DETECT_REDUNDANT)
def detect_redundant(task_id, channel):
    return DetectRedundant(task_id, channel).load_data()


@celery.task(name=DB_DELETE_REDUNDANT)
def delete_redundant(task_id, channel):
    return DeleteRedundant(task_id, channel).load_data()


@celery.task(name=CREATE_TRIGGERS)
def create_triggers(task_id, channel):
    return CreateTriggers(task_id, channel).load_data()


@celery.task(name=CREATE_CUBE)
def create_cube(task_id, channel):
    return CreateCube(task_id, channel).load_data()


class CreateDataset(TaskProcessing):
    """
    Создание Dataset
    """

    def processing(self):

        dataset, created = Dataset.objects.get_or_create(key=self.key)
        self.context['dataset_id'] = dataset.id

        is_meta_stats = self.context['is_meta_stats']

        if not is_meta_stats:
            self.next_task_params = (
                MONGODB_DATA_LOAD, load_mongo_db, self.context)
        else:
            self.context['db_update'] = True

            self.next_task_params = (
                MONGODB_DELTA_LOAD, update_mongo_db, self.context)


class LoadMongodb(TaskProcessing):
    """
    Первичная загрузка данных в Mongodb
    """

    def processing(self):
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
        structure = self.context['tree']
        source_model = Datasource()
        source_model.set_from_dict(**self.context['source'])

        page = 1
        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

        # общее количество строк в запросе
        self.publisher.rows_count = DataSourceService.get_structure_rows_number(
            source_model, structure,  cols)
        self.publisher.publish(TLSE.START)

        col_names = ['_id', '_state', '_date']
        for t_name, col_group in groupby(cols, lambda x: x["table"]):
            for x in col_group:
                col_names.append(x["table"] + FIELD_NAME_SEP + x["col"])

        # находим бинарные данные для 1) создания ключей 2) инсерта в монго
        binary_types_list = get_binary_types_list(cols, col_types)

        # создаем коллекцию и индексы в Mongodb
        mc = MongodbConnection()
        collection = mc.get_collection(
            'etl', get_table_name(STTM_DATASOURCE, self.key))
        mc.set_indexes([('_id', ASCENDING), ('_state', ASCENDING),
                        ('_date', ASCENDING)])

        # Коллекция с текущими данными
        current_collection_name = get_table_name(STTM_DATASOURCE_KEYS, self.key)
        MongodbConnection.drop('etl', current_collection_name)
        current_mc = MongodbConnection()
        current_collection = current_mc.get_collection(
            'etl', current_collection_name)
        current_mc.set_indexes([('_id', ASCENDING)])

        query = DataSourceService.get_rows_query_for_loading_task(
            source_model, structure, cols)

        source_connection = DataSourceService.get_source_connection(source_model)

        tables_key_creator = []
        for table, value in json.loads(self.context['meta_info']).iteritems():
            rkc = RowKeysCreator(table=table, cols=cols)
            rkc.set_primary_key(value)
            tables_key_creator.append(rkc)

        while True:
            cursor = source_connection.cursor()
            cursor.execute(query.format(limit, (page-1)*limit))
            result = cursor.fetchall()

            data_to_insert = []
            data_to_current_insert = []
            if not result:
                break

            for ind, record in enumerate(result):
                row_key = calc_key_for_row(
                        record, tables_key_creator, (page-1)*limit + ind,
                        binary_types_list)

                # бинарные данные оборачиваем в Binary(), если они имеются
                new_record = process_binary_data(record, binary_types_list)

                record_normalized = (
                    [row_key, STSE.IDLE, EtlEncoder.encode(datetime.now())] +
                    [EtlEncoder.encode(rec_field) for rec_field in new_record])
                data_to_insert.append(dict(izip(col_names, record_normalized)))
                data_to_current_insert.append(dict(_id=row_key))
            try:
                collection.insert_many(data_to_insert, ordered=False)
                current_collection.insert_many(data_to_current_insert, ordered=False)
                print 'inserted %d rows to mongodb' % len(data_to_insert)
            except Exception as e:
                self.error_handling(e.message)

            # обновляем информацию о работе таска
            self.queue_storage.update()
            self.publisher.loaded_count += limit
            self.publisher.publish(TLSE.PROCESSING)
            self.queue_storage['percent'] = (
                100 if self.publisher.is_complete else self.publisher.percent)

            page += 1

        self.next_task_params = (DB_DATA_LOAD, load_db, self.context)


class LoadDb(TaskProcessing):

    def processing(self):
        """
        Загрузка данных из Mongodb в базу данных
        """
        self.key = self.context['checksum']
        self.user_id = self.context['user_id']
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
        structure = self.context['tree']
        db_update = self.context['db_update']

        source = Datasource()
        source.set_from_dict(**self.context['source'])
        # общее количество строк в запросе
        self.publisher.rows_count = DataSourceService.get_structure_rows_number(
            source, structure,  cols)
        self.publisher.publish(TLSE.START)

        col_names = ['"cdc_key" text PRIMARY KEY']
        clear_col_names = ['cdc_key']
        for obj in cols:
            t = obj['table']
            c = obj['col']
            col_names.append('"{0}{1}{2}" {3}'.format(
                t, FIELD_NAME_SEP, c,
                TYPES_MAP.get(col_types['{0}.{1}'.format(t, c)])))
            clear_col_names.append('{0}{1}{2}'.format(t, FIELD_NAME_SEP, c))

        # инфа о бинарных данных для инсерта в постгрес
        binary_types_dict = get_binary_types_dict(cols, col_types)

        # инфа для колонки cdc_key, о том, что она не binary
        binary_types_dict['0'] = False

        source_collection = MongodbConnection().get_collection(
            'etl', get_table_name(STTM_DATASOURCE, self.key))

        source_table_name = get_table_name(STTM_DATASOURCE, self.key)
        if not db_update:
            table_create_query = TableCreateQuery(DataSourceService())
            table_create_query.set_query(
                table_name=source_table_name, cols=col_names)
            table_create_query.execute()

        insert_query = InsertQuery(DataSourceService())
        insert_query.set_query(
            table_name=source_table_name, cols_nums=len(clear_col_names))

        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        offset = 0
        last_row = None
        # Пишем данные в базу
        while True:
            try:
                collection_cursor = source_collection.find(
                    {'_state': STSE.IDLE},
                    limit=limit, skip=offset)
                rows_dict = []
                for record in collection_cursor:
                    temp_dict = {}
                    for ind, col_name in enumerate(clear_col_names):
                        temp_dict.update(
                            {str(ind): record['_id'] if col_name == 'cdc_key'
                                else record[col_name]})
                    rows_dict.append(temp_dict)
                if not rows_dict:
                    break
                insert_query.execute(data=rows_dict,
                                     binary_types_dict=binary_types_dict)
                print 'load in db %s records' % len(rows_dict)
                offset += limit
            except Exception as e:
                print 'Exception'
                insert_query.connection.rollback()
                self.was_error = True
                # код и сообщение ошибки
                pg_code = getattr(e, 'pgcode', None)

                err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
                err_msg += e.message
                self.error_handling(err_msg, pg_code)
            else:
                last_row = rows_dict[-1]  # получаем последнюю запись
                # обновляем информацию о работе таска
                self.queue_storage.update()
                self.publisher.loaded_count += limit
                self.publisher.publish(TLSE.PROCESSING)
                self.queue_storage['percent'] = (
                    100 if self.publisher.is_complete else self.publisher.percent)

        source_collection.update_many(
            {'_state': STSE.IDLE}, {'$set': {'_state': STSE.LOADED}})

        # работа с datasource_meta
        DataSourceService.update_datasource_meta(
            self.key, source, cols, json.loads(
                self.context['meta_info']), last_row, self.context['dataset_id'])
        if last_row:
            DataSourceService.update_collections_stats(
                self.context['collections_names'], last_row['0'])

        if self.context['cdc_type'] != DatasourceSettings.TRIGGERS:
            self.next_task_params = (DB_DETECT_REDUNDANT, detect_redundant, {
                'is_meta_stats': self.context['is_meta_stats'],
                'checksum': self.key,
                'user_id': self.user_id,
                'source_id': source.id,
                'cols': self.context['cols'],
                'col_types': self.context['col_types'],
                'dataset_id': self.context['dataset_id'],
            })
        else:
            self.next_task_params = (CREATE_TRIGGERS, create_triggers, {
                'checksum': self.key,
                'user_id': self.user_id,
                'tables_info': self.context['tables_info'],
                'source_id': source.id,
                'cols': self.context['cols'],
                'col_types': self.context['col_types'],
                'dataset_id': self.context['dataset_id'],
            })


class LoadDimensions(TaskProcessing):
    """
    Создание рамерности, измерения олап куба
    """
    table_prefix = DIMENSIONS
    actual_fields_type = ['text']

    def get_actual_fields(self, meta_data):
        """
        Фильтруем поля по необходимому нам типу

        Returns:
            list of tuple: Метаданные отфильтрованных полей
        """
        actual_fields = []
        for record in meta_data:

            for field in json.loads(record['meta__fields'])['columns']:
                if field['type'] in self.actual_fields_type:
                    actual_fields.append((
                        record['meta__collection_name'], field))

        return actual_fields

    def rows_query(self, fields):
        """
        Формируюм строку запроса

        Args:
            fields(list): Список именно необходимых имен

        Returns:
            str: Строка запроса
        """
        fields_str = '"'+'", "'.join(fields)+'"'
        query = "SELECT {0} FROM {1} LIMIT {2} OFFSET {3};"
        source_table_name = get_table_name(
            STTM_DATASOURCE, self.key)
        return query.format(
            fields_str, source_table_name, '{0}', '{1}')

    def processing(self):
        self.key = self.context['checksum']
        # Наполняем контекст
        source = Datasource.objects.get(id=self.context['source_id'])
        meta_tables = {
            k: v for (k, v) in
            DatasourceMeta.objects.filter(
                datasource=source).values_list('collection_name', 'id')}
        meta_data = DatasourceMetaKeys.objects.filter(
            value=self.key).values('meta__collection_name', 'meta__fields')
        self.actual_fields = self.get_actual_fields(meta_data)

        col_names = ['"cdc_key" text PRIMARY KEY']
        for table, field in self.actual_fields:
            col_names.append('"{0}{1}{2}" {3}'.format(
                table, FIELD_NAME_SEP, field['name'], field['type']))

        create_query = TableCreateQuery(DataSourceService())
        create_query.set_query(
            table_name=get_table_name(self.table_prefix, self.key), cols=col_names)
        create_query.execute()

        try:
            self.save_fields()
        except Exception as e:
            # код и сообщение ошибки
            pg_code = getattr(e, 'pgcode', None)

            err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
            err_msg += e.message
            self.error_handling(err_msg)

        # Сохраняем метаданные
        self.save_meta_data(
            self.user_id, self.key, self.actual_fields, meta_tables)
        if not self.last_task:
            self.set_next_task_params()

    def set_next_task_params(self):
        self.next_task_params = (
            GENERATE_MEASURES, load_measures, self.context)

    def save_meta_data(self, user_id, key, fields, meta_tables):
        """
        Сохранение метаинформации
0
        Args:
            user_id(int): id пользователя
            table_name(str): Название создаваемой таблицы
            fields(dict): данные о полях
            meta(DatasourceMeta): ссылка на метаданные хранилища
        """
        level = dict()
        for table, field in fields:
            datasource_meta_id = DatasourceMeta.objects.get(
                id=meta_tables[table])
            target_table_name = '{0}{1}{2}'.format(
                    table, FIELD_NAME_SEP, field['name'])
            level.update(dict(
                type=field['type'], level_type='regular', visible=True,
                column=target_table_name, unique_members=field['is_unique'],
                caption=target_table_name,
                )
            )

            # data = dict(
            #     name=target_table_name,
            #     has_all=True,
            #     table_name=target_table_name,
            #     level=level,
            #     primary_key='id',
            #     foreign_key=None
            # )

            Dimension.objects.get_or_create(
                name=target_table_name,
                title=target_table_name,
                user_id=user_id,
                datasources_meta=datasource_meta_id,
                # data=json.dumps(data)
            )

    def save_fields(self):
        """Заполняем таблицу данными

        Args:
            model: Модель к целевой таблице
        """
        column_names = ['cdc_key']
        for table, field in self.actual_fields:
            column_names.append('{0}{1}{2}'.format(
                    table, FIELD_NAME_SEP, field['name']))

        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])

        # инфа о бинарных данных для инсерта в постгрес
        binary_types_dict = get_binary_types_dict(cols, col_types)

        # инфа для колонки cdc_key, о том, что она не binary
        binary_types_dict['0'] = False

        insert_query = InsertQuery(DataSourceService())
        insert_query.set_query(
            table_name=get_table_name(self.table_prefix, self.key),
            cols_nums=len(column_names))
        offset = 0
        step = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        print 'load dim or measure'
        rows_query = self.rows_query(column_names)

        connection = DataSourceService.get_local_instance().connection
        while True:
            # index_to = offset+step
            cursor = connection.cursor()

            cursor.execute(rows_query.format(step, offset))
            rows = cursor.fetchall()
            if not rows:
                break
            rows_dict = []
            for record in rows:
                temp_dict = {}
                for ind in xrange(len(column_names)):
                    temp_dict.update({str(ind): record[ind]})
                rows_dict.append(temp_dict)
            insert_query.execute(data=rows_dict,
                                 binary_types_dict=binary_types_dict)
            print 'load in db %s records' % len(rows_dict)
            offset += step

            self.queue_storage.update()


class LoadMeasures(LoadDimensions):
    """
    Создание мер
    """
    table_prefix = MEASURES
    actual_fields_type = [
        Measure.INTEGER, Measure.TIME, Measure.DATE, Measure.TIMESTAMP]

    def save_meta_data(self, user_id, key, fields, meta_tables):
        """
        Сохранение информации о мерах
        """
        for table, field in fields:
            datasource_meta_id = DatasourceMeta.objects.get(
                id=meta_tables[table])
            target_table_name = '{0}{1}{2}'.format(
                    table, FIELD_NAME_SEP, field['name'])
            Measure.objects.get_or_create(
                name=target_table_name,
                title=target_table_name,
                type=field['type'],
                user_id=user_id,
                datasources_meta=datasource_meta_id
            )

    def set_next_task_params(self):
        self.next_task_params = (
            CREATE_CUBE, create_cube, self.context)


class UpdateMongodb(TaskProcessing):

    def processing(self):
        """
        1. Процесс обновленения данных в коллекции `sttm_datasource_delta_{key}`
        новыми данными с помощью `sttm_datasource_keys_{key}`
        2. Создание коллекции `sttm_datasource_keys_{key}` c ключами для
        текущего состояния источника
        """
        self.key = self.context['checksum']
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
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

        # находим бинарные данные для 1) создания ключей 2) инсерта в монго
        binary_types_list = get_binary_types_list(cols, col_types)

        collection = MongodbConnection().get_collection(
            'etl', get_table_name(STTM_DATASOURCE, self.key))

        # Коллекция с текущими данными
        current_collection_name = get_table_name(STTM_DATASOURCE_KEYS, self.key)
        MongodbConnection.drop('etl', current_collection_name)
        current_mc = MongodbConnection()
        current_collection = current_mc.get_collection(
            'etl', current_collection_name)
        current_mc.set_indexes([('_id', ASCENDING)])

        # Дельта-коллекция
        delta_mc = MongodbConnection()
        delta_collection = delta_mc.get_collection(
            'etl', get_table_name(STTM_DATASOURCE_DELTA, self.key))
        delta_mc.set_indexes([
            ('_id', ASCENDING), ('_state', ASCENDING), ('_date', ASCENDING)])

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
                    record, tables_key_creator, (page-1)*limit + ind,
                    binary_types_list)

                # бинарные данные оборачиваем в Binary(), если они имеются
                new_record = process_binary_data(record, binary_types_list)

                if not collection.find({'_id': row_key}).count():
                    delta_rows = (
                        [row_key, DTSE.NEW, EtlEncoder.encode(datetime.now())] +
                        [EtlEncoder.encode(rec_field) for rec_field in new_record])
                    data_to_insert.append(dict(izip(col_names, delta_rows)))

                data_to_current_insert.append(dict(_id=row_key))
            try:
                if data_to_insert:
                    delta_collection.insert_many(data_to_insert, ordered=False)
                current_collection.insert_many(data_to_current_insert, ordered=False)
            except Exception as e:
                self.error_handling(e.message)
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
            try:
                collection.insert_many(to_ins, ordered=False)
            except Exception as e:
                self.error_handling(e.message)

            page += 1

        # Обновляем статусы дельты-коллекции
        delta_collection.update_many(
            {'_state': DTSE.NEW}, {'$set': {'_state': DTSE.SYNCED}})

        self.next_task_params = (DB_DATA_LOAD, load_db, self.context)


class DetectRedundant(TaskProcessing):

    def processing(self):
        """
        Выявление записей на удаление
        """
        self.key = self.context['checksum']
        source_collection = MongodbConnection().get_collection(
                    'etl', get_table_name(STTM_DATASOURCE, self.key))
        current_collection = MongodbConnection().get_collection(
                    'etl', get_table_name(STTM_DATASOURCE_KEYS, self.key))

        # Обновляем коллекцию всех ключей
        all_keys_collection_name = get_table_name(STTM_DATASOURCE_KEYSALL, self.key)
        ak_collection = MongodbConnection()
        all_keys_collection = ak_collection.get_collection(
            'etl', all_keys_collection_name)
        ak_collection.set_indexes(
            [('_state', ASCENDING), ('_deleted', ASCENDING)])

        source_collection.aggregate(
            [{"$match": {"_state": STSE.LOADED}},
             {"$project": {"_id": "$_id", "_state": {"$literal": AKTSE.NEW}}},
             {"$out": "%s" % all_keys_collection_name}])

        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        page = 1
        while True:

            to_delete = []
            records_for_del = list(all_keys_collection.find(
                    {'_state': AKTSE.NEW}, limit=limit, skip=(page-1)*limit))
            if not len(records_for_del):
                break
            for record in records_for_del:
                row_key = record['_id']
                if not current_collection.find({'_id': row_key}).count():
                    to_delete.append(row_key)
            try:
                all_keys_collection.update_many(
                    {'_id': {'$in': to_delete}},
                    {'$set': {'_deleted': True}})

                source_collection.delete_many({'_id': {'$in': to_delete}})
            except Exception as e:
                self.error_handling(e.message)

            page += 1

        all_keys_collection.update_many(
            {'_state': AKTSE.NEW}, {'$set': {'_state': AKTSE.SYNCED}})

        self.next_task_params = (
            DB_DELETE_REDUNDANT, delete_redundant, self.context)


class DeleteRedundant(TaskProcessing):

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
                {'_deleted': True},
                limit=limit, skip=(page-1)*limit)
            l = [record['_id'] for record in delete_delta]
            if not l:
                break
            try:
                delete_query.execute(keys=l)
            except Exception as e:
                self.error_handling(e.message)
            page += 1

        if not self.context['is_meta_stats']:
            self.next_task_params = (
                        GENERATE_DIMENSIONS, load_dimensions, self.context)


class CreateTriggers(TaskProcessing):

    def processing(self):
        """
        Создание триггеров в БД пользователя
        """
        tables_info = self.context['tables_info']

        source = Datasource.objects.get(id=self.context['source_id'])

        db_instance = DatabaseService.get_source_instance(source)
        sep = db_instance.get_separator()
        remote_table_create_query = db_instance.remote_table_create_query()
        remote_triggers_create_query = db_instance.remote_triggers_create_query()

        connection = db_instance.connection
        cursor = connection.cursor()

        for table, columns in tables_info.iteritems():

            table_name = '_etl_datasource_cdc_{0}'.format(table)
            tables_str = "('{0}')".format(table_name)

            cdc_cols_query = db_instance.db_map.cdc_cols_query.format(
                tables_str, source.db, 'public')

            cursor.execute(cdc_cols_query)
            fetched_cols = cursor.fetchall()

            existing_cols = {k: v for (k, v) in fetched_cols}

            required_indexes = {k.format(table_name): v
                                for k, v in REQUIRED_INDEXES.iteritems()}

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

            # если таблица существует
            if existing_cols:
                # удаление primary key, если он есть
                primary_query = db_instance.get_primary_key(table_name, source.db)
                cursor.execute(primary_query)
                primary = cursor.fetchall()

                if primary:
                    primary_name = primary[0][0]
                    del_pr_query = db_instance.delete_primary_query(
                        table_name, primary_name)
                    cursor.execute(del_pr_query)

                # добавление недостающих колонок, не учитывая cdc-колонки
                new_came_cols = [(x['name'], x["type"]) for x in columns]

                diff_cols = [x for x in new_came_cols if x[0] not in existing_cols]

                if diff_cols:
                    add_cols_str = """
                        alter table {0} {1}
                    """.format(table_name, ', '.join(
                        ['add column {0} {1}'.format(x[0], x[1]) for x in diff_cols]))

                    cursor.execute(add_cols_str)
                    connection.commit()

                # проверка cdc-колонок на существование и типы
                cdc_required_types = db_instance.db_map.cdc_required_types

                add_col_q = db_instance.db_map.add_column_query
                del_col_q = db_instance.db_map.del_column_query

                for cdc_k, v in cdc_required_types.iteritems():
                    if not cdc_k in existing_cols:
                        cursor.execute(add_col_q.format(
                            table_name, cdc_k, v["type"], v["nullable"]))
                    else:
                        # если типы не совпадают
                        if not existing_cols[cdc_k].startswith(v["type"]):
                            cursor.execute(del_col_q.format(table_name, cdc_k))
                            cursor.execute(add_col_q.format(
                                table_name, cdc_k, v["type"], v["nullable"]))

                connection.commit()

                # проверяем индексы на колонки и существование,
                # лишние индексы удаляем

                indexes_query = db_instance.db_map.indexes_query.format(
                    tables_str, source.db)
                cursor.execute(indexes_query)
                exist_indexes = cursor.fetchall()

                index_cols_i, index_name_i = 1, 2

                create_index_q = db_instance.db_map.create_index_query
                drop_index_q = db_instance.db_map.drop_index_query

                allright_index_names = []

                for index in exist_indexes:
                    index_name = index[index_name_i]

                    if index_name not in required_indexes:
                        cursor.execute(drop_index_q.format(index_name, table_name))
                    else:
                        index_cols = sorted(index_name[index_cols_i].split(','))
                        if index_cols != required_indexes[index_name]:

                            cursor.execute(drop_index_q.format(index_name, table_name))
                            cursor.execute(create_index_q.format(
                                index_name, table_name,
                                ','.join(required_indexes[index_name]),
                                source.db))

                        allright_index_names.append(index_name)

                diff_indexes_names = [
                    x for x in required_indexes if x not in allright_index_names]

                for d_index in diff_indexes_names:
                    cursor.execute(
                        create_index_q.format(
                            d_index, table_name,
                            ','.join(required_indexes[d_index])))

                connection.commit()

            # если таблица не существует
            else:
                # создание таблицы у юзера
                cursor.execute(remote_table_create_query.format(
                        table_name, cols_str))

                # создание индексов
                create_index_q = db_instance.db_map.create_index_query

                for index_name, index_cols in required_indexes.iteritems():
                    cursor.execute(create_index_q.format(
                        index_name, table_name,
                        ','.join(index_cols), source.db))

                connection.commit()

            trigger_commands = remote_triggers_create_query.format(
                orig_table=table, new_table=table_name, new=new, old=old,
                cols=cols)

            # multi queries of mysql, delimiter $$
            for query in trigger_commands.split('$$'):
                cursor.execute(query)

            connection.commit()

        cursor.close()
        connection.close()

        self.next_task_params = (
            GENERATE_DIMENSIONS, load_dimensions, {
                'checksum': self.key,
                'user_id': self.user_id,
                'source_id': self.context['source_id'],
                'cols': self.context['cols'],
                'col_types': self.context['col_types'],
                'dataset_id': self.context['dataset_id'],
            })


class CreateCube(TaskProcessing):

    def processing(self):

        print 'Start cube creation'

        dataset_id = self.context['dataset_id']
        dataset = Dataset.objects.get(id=dataset_id)
        key = dataset.key

        meta_ids = DatasetToMeta.objects.filter(dataset_id=dataset_id).values_list(
            'meta_id', flat=True)

        dimensions = Dimension.objects.filter(datasources_meta_id__in=meta_ids)
        measures = Measure.objects.filter(datasources_meta_id__in=meta_ids)

        if not dimensions.exists() and not measures.exists():
            pass
        # <Schema>
        cube_key = "cube_{key}".format(key=key)
        schema = etree.Element('Schema', name=cube_key, metamodelVersion='4.0')

        # <Physical schema>
        physical_schema = etree.Element('PhysicalSchema')
        etree.SubElement(
            physical_schema, 'Table', name=get_table_name(MEASURES, key))
        etree.SubElement(
            physical_schema, 'Table', name=get_table_name(DIMENSIONS, key))

        cube_info = {
            'name': cube_key,
            'caption': cube_key,
            'visible': "true",
            'cache': "false",
            'enabled': "true",
        }

        # <Cube>
        cube = etree.Element('Cube', **cube_info)

        dimensions_tag = etree.SubElement(cube, 'Dimensions')

        # <Dimensions>
        for dim in dimensions:

            dim_type = dim.get_dimension_type()
            visible = 'true' if dim.visible else 'false'
            name = dim.name
            title = dim.title

            dim_info = {
                'table': get_table_name(DIMENSIONS, key),
                'type': dim_type,
                'visible': visible,
                'highCardinality': 'true' if dim.high_cardinality else 'false',
                'name': name,
                'caption': title,
            }
            dimension = etree.SubElement(dimensions_tag, 'Dimension', **dim_info)

            # <Attributes>
            attributes = etree.SubElement(dimension, 'Attributes')
            attr_info = {
                'name': title,
                'keyColumn': name,
                'hasHierarchy': 'false',
            }
            etree.SubElement(attributes, 'Attribute', **attr_info)

            # <Attributes>
            hierarchies = etree.SubElement(dimension, 'Hierarchies')
            hierarchy = etree.SubElement(hierarchies, 'Hierarchy', **{'name': title})

            level_info = {
                'attribute': title,
                'name': '%s level' % title,
                'visible': visible,
                'caption': title,
            }
            etree.SubElement(hierarchy, 'Level', **level_info)

        measure_groups = etree.SubElement(cube, 'MeasureGroups')

        measure_group_info = {
            'name': get_table_name(MEASURES, key),
            'table': get_table_name(MEASURES, key)
        }

        measure_group = etree.SubElement(
            measure_groups, 'MeasureGroup', **measure_group_info)
        measures_tag = etree.SubElement(measure_group, 'Measures')

        for measure in measures:
            measure_info = {
                'name': measure.name,
                'column': measure.name,
                'caption': measure.title,
                'visible': 'true' if measure.visible else 'false',
                'aggregator': 'sum',
            }
            etree.SubElement(measures_tag, 'Measure', **measure_info)

        dimension_links = etree.SubElement(measure_group, 'DimensionLinks')

        for dim in dimensions:

            etree.SubElement(dimension_links, 'NoLink', dimension=dim.name)
            # etree.SubElement(dimension_links, 'ForeignKeyLink', dimension=dim.name, foreignKeyColumn='dimension_id')

        schema.extend([physical_schema, cube])

        cube_string = etree.tostring(schema, pretty_print=True)

        cube = Cube.objects.create(
            name=cube_key,
            data=cube_string,
            user_id=self.context['user_id'],
            # user_id=11,
        )

        send_xml(key, cube.id, cube_string)

# write in console: python manage.py celery -A etl.tasks worker
