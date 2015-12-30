# coding: utf-8
import logging

import os
import sys
import brukva
from datetime import datetime
import pymongo
import json

from pymongo import IndexModel, ASCENDING
from psycopg2 import errorcodes
from etl.constants import *
from etl.services.middleware.base import (
    EtlEncoder, get_table_name)
from etl.services.model_creation import (
    get_django_model, install, get_field_settings)
from etl.services.queue.base import TLSE,  STSE, RPublish, RowKeysCreator, \
    calc_key_for_row, TableCreateQuery, InsertQuery, MongodbConnection, \
    DeleteQuery, DelTSE, DTSE
from .helpers import (RedisSourceService, DataSourceService,
                      TaskService, TaskStatusEnum,
                      TaskErrorCodeEnum)
from core.models import (
    Datasource, Dimension, Measure, QueueList, DatasourceMeta,
    DatasourceMetaKeys)
from django.conf import settings

from djcelery import celery
from celery.contrib.methods import task_method
from itertools import groupby, izip

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

client = brukva.Client(host=settings.REDIS_HOST,
                       port=int(settings.REDIS_PORT),
                       selected_db=settings.REDIS_DB)
client.connect()

logger = logging.getLogger(__name__)


class TaskProcessing(object):
    """
    Базовый класс, отвечающий за про процесс выполнения celery-задач

    Attributes:
        task_id(int): id задачи
        channel(str): Канал передачи на клиент
        user_id(str): id пользователя
        context(dict): контекстные данные для задачи
        was_error(bool): Факт наличия ошибки
        err_msg(str): Текст ошибки, если случилась
        publisher(`RPublish`): Посыльный к клиенту о текущем статусе задачи
        queue_storage(`QueueStorage`): Посыльный к redis о текущем статусе задачи
        key(str): Ключ
    """

    def __init__(self, task_id, channel):
        """
        Args:
            task_id(int): id задачи
            channel(str): Канал передачи на клиент
        """
        self.task_id = task_id
        self.channel = channel
        self.user_id = None
        self.context = None
        self.was_error = False
        self.err_msg = ''
        self.publisher = RPublish(self.channel, self.task_id)
        self.queue_storage = None
        self.key = None

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
        self.processing()
        self.exit()

    def processing(self):
        """
        Непосредственное выполнение задачи
        """
        raise NotImplementedError

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


@celery.task(name=MONGODB_DATA_LOAD)
def load_mongo_db(task_id, channel):
    LoadMongodb(task_id, channel).load_data()


@celery.task(name=DB_DATA_LOAD)
def load_db(task_id, channel):
    LoadDb(task_id, channel).load_data()


@celery.task(name=GENERATE_DIMENSIONS)
def load_dimensions(task_id, channel):
    LoadDimensions(task_id, channel).load_data()


@celery.task(name=GENERATE_MEASURES)
def load_measures(task_id, channel):
    LoadMeasures(task_id, channel).load_data()


@celery.task(name=MONGODB_DELTA_LOAD)
def update_mongo_db(task_id, channel):
    UpdateMongodb(task_id, channel).load_data()


@celery.task(name=DB_DETECT_REDUNDANT)
def detect_redundant(task_id, channel):
    DetectRedundant(task_id, channel).load_data()


@celery.task(name=DB_DELETE_REDUNDANT)
def delete_redundant(task_id, channel):
    DeleteRedundant(task_id, channel).load_data()


@celery.task(name=CREATE_TRIGGERS)
def create_triggers(task_id, channel):
    CreateTriggers(task_id, channel).load_data()


class LoadMongodb(TaskProcessing):
    """
    Первичная загрузка данных в Mongodb
    """

    # @celery.task(name=MONGODB_DATA_LOAD, filter=task_method)
    def load_data(self):
        super(LoadMongodb, self).load_data()

    def processing(self):
        cols = json.loads(self.context['cols'])
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

        # создаем коллекцию и индексы в Mongodb
        mc = MongodbConnection()
        collection = mc.get_collection(
            'etl', get_table_name(STTM_DATASOURCE, self.key))
        mc.set_indexes([('_id', ASCENDING), ('_state', ASCENDING),
                        ('_date', ASCENDING)])

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
            if not result:
                break

            for ind, record in enumerate(result):
                row_key = calc_key_for_row(
                        record, tables_key_creator, (page-1)*limit + ind)
                record_normalized = (
                    [row_key, STSE.IDLE, EtlEncoder.encode(datetime.now())] +
                    [EtlEncoder.encode(rec_field) for rec_field in record])
                data_to_insert.append(dict(izip(col_names, record_normalized)))
            try:
                collection.insert_many(data_to_insert, ordered=False)
                print 'inserted %d rows to mongodb' % len(data_to_insert)
            except Exception as e:
                self.was_error = True
                # fixme перезаписывается при каждой ошибке
                self.err_msg = e.message
                print "Unexpected error:", type(e), e
                self.queue_storage['status'] = TaskStatusEnum.ERROR

                # сообщаем об ошибке
                self.publisher.publish(TLSE.ERROR, self.err_msg)

            # обновляем информацию о работе таска
            self.queue_storage.update()
            self.publisher.loaded_count += limit
            self.publisher.publish(TLSE.PROCESSING)
            self.queue_storage['percent'] = (
                100 if self.publisher.is_complete else self.publisher.percent)

            page += 1


class LoadDb(TaskProcessing):

    # @celery.task(name=DB_DATA_LOAD, filter=task_method)
    def load_data(self):
        super(LoadDb, self).load_data()

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

        col_names = ['"_id" text UNIQUE', '"_state" text', '"_date" timestamp']
        clear_col_names = ['_id', '_state', '_date']
        for obj in cols:
            t = obj['table']
            c = obj['col']
            col_names.append('"{0}{1}{2}" {3}'.format(
                t, FIELD_NAME_SEP, c, col_types['{0}.{1}'.format(t, c)]))
            clear_col_names.append('{0}{1}{2}'.format(
                t, FIELD_NAME_SEP, c, col_types['{0}.{1}'.format(t, c)]))

        collection = MongodbConnection().get_collection(
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
                collection_cursor = collection.find(
                    {'_state': STSE.IDLE},
                    limit=limit, skip=offset)
                # last_row = collection_cursor.limit(1).sort('$natural', -1)[0]
                rows_dict = []
                for record in collection_cursor:
                    temp_dict = {}
                    for ind, col_name in enumerate(clear_col_names):
                        temp_dict.update({str(ind): record[col_name]})
                    rows_dict.append(temp_dict)
                if not rows_dict:
                    break
                insert_query.execute(data=rows_dict)
                offset += limit
            except Exception as e:
                print 'Exception'
                self.was_error = True
                # код и сообщение ошибки
                pg_code = getattr(e, 'pgcode', None)

                self.err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
                self.err_msg += e.message

                # меняем статус задачи на 'Ошибка'
                TaskService.update_task_status(
                    self.task_id, TaskStatusEnum.ERROR,
                    error_code=pg_code or TaskErrorCodeEnum.DEFAULT_CODE,
                    error_msg=self.err_msg)
                logger.exception(self.err_msg)
                self.queue_storage.update(TaskStatusEnum.ERROR)

                # сообщаем об ошибке
                self.publisher.publish(TLSE.ERROR, self.err_msg)
                break
            else:
                # TODO: Найти последнюю строку
                last_row = rows_dict[-1]  # получаем последнюю запись
                # обновляем информацию о работе таска
                self.queue_storage.update()
                self.publisher.loaded_count += limit
                self.publisher.publish(TLSE.PROCESSING)
                self.queue_storage['percent'] = (
                    100 if self.publisher.is_complete else self.publisher.percent)

        collection.update_many(
            {'_state': STSE.IDLE}, {'$set': {'_state': STSE.LOADED}})

        # работа с datasource_meta
        DataSourceService.update_datasource_meta(
            self.key, source, cols, json.loads(
                self.context['meta_info']), last_row)
        if last_row:
            DataSourceService.update_collections_stats(
                self.context['collections_names'], last_row['0'])


class LoadDimensions(TaskProcessing):
    """
    Создание сущностей(рамерности, измерения) олап куба
    """
    app_name = 'sttm'
    module = 'biplatform.etl.models'
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

    # @celery.task(name=GENERATE_DIMENSIONS, filter=task_method)
    def load_data(self):
        super(LoadDimensions, self).load_data()

    def processing(self):
        self.key = self.context['checksum']
        # Наполняем контекст
        source = Datasource.objects.get(id=self.context['datasource_id'])
        meta_tables = {
            k: v for (k, v) in
            DatasourceMeta.objects.filter(
                datasource=source).values_list('collection_name', 'id')}
        meta_data = DatasourceMetaKeys.objects.filter(
            value=self.key).values('meta__collection_name', 'meta__fields')
        self.actual_fields = self.get_actual_fields(meta_data)

        f_list = []
        table_name = get_table_name(self.table_prefix, self.key)
        for table, field in self.actual_fields:
            field_name = '{0}{1}{2}'.format(
                    table, FIELD_NAME_SEP, field['name'])

            f_list.append(get_field_settings(field_name, field['type']))

        model = get_django_model(
            table_name, f_list, self.app_name,
            self.module, table_name)
        install(model)
        try:
            self.save_fields(model)
        except Exception as e:
            # код и сообщение ошибки
            pg_code = getattr(e, 'pgcode', None)

            err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
            err_msg += e.message

            # меняем статус задачи на 'Ошибка'
            TaskService.update_task_status(
                self.task_id, TaskStatusEnum.ERROR, error_msg=err_msg,
                error_code=pg_code or TaskErrorCodeEnum.DEFAULT_CODE,)
            logger.exception(err_msg)
            self.queue_storage.update(TaskStatusEnum.ERROR)

        # Сохраняем метаданные
        self.save_meta_data(
            self.user_id, self.key, self.actual_fields, meta_tables)

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

    def save_fields(self, model):
        """Заполняем таблицу данными

        Args:
            model: Модель к целевой таблице
        """
        column_names = []
        for table, field in self.actual_fields:
            column_names.append('{0}{1}{2}'.format(
                    table, FIELD_NAME_SEP, field['name']))
        offset = 0
        step = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        print 'load dim or measure'
        while True:
            rows_query = self.rows_query(column_names)
            index_to = offset+step
            connection = DataSourceService.get_local_instance().connection
            cursor = connection.cursor()

            cursor.execute(rows_query.format(index_to, offset))
            rows = cursor.fetchall()
            if not rows:
                break
            column_data = [model(
                **{column_names[i]: v for (i, v) in enumerate(x)})
                        for x in rows]
            model.objects.bulk_create(column_data)
            offset = index_to

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

    # @celery.task(name=GENERATE_MEASURES, filter=task_method)
    def load_data(self):
        """
        Создание размерностей
        """
        super(LoadMeasures, self).load_data()

    def processing(self):
        super(LoadMeasures, self).processing()


class UpdateMongodb(TaskProcessing):

    # @celery.task(name=MONGODB_DELTA_LOAD, filter=task_method)
    def load_data(self):
        super(UpdateMongodb, self).load_data()

    def processing(self):
        """
        1. Процесс обновленения данных в коллекции `sttm_datasource_delta_{key}`
        новыми данными с помощью `sttm_datasource_keys_{key}`
        2. Создание коллекции `sttm_datasource_keys_{key}` c ключами для
        текущего состояния источника
        """
        print 'hi'
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

    # @celery.task(name=DB_DETECT_REDUNDANT, filter=task_method)
    def load_data(self):
        super(DetectRedundant, self).load_data()

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

    # @celery.task(name=DB_DELETE_REDUNDANT, filter=task_method)
    def load_data(self):
        super(DeleteRedundant, self).load_data()

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


class CreateTriggers(TaskProcessing):

    # @celery.task(name=CREATE_TRIGGERS, filter=task_method)
    def load_data(self):
        super(CreateTriggers, self).load_data()

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

# write in console: python manage.py celery -A etl.tasks worker
