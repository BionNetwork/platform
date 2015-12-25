# coding: utf-8
import logging

import os
import sys
import brukva
import pymongo
import json

import operator
import binascii
from psycopg2 import errorcodes
from etl.constants import FIELD_NAME_SEP
from etl.services.model_creation import OlapEntityCreation
from etl.services.middleware.base import (EtlEncoder, generate_table_name_key, get_table_name, datetime_now_str)
from .helpers import (RedisSourceService, DataSourceService,
                      TaskService, TaskStatusEnum,
                      TaskErrorCodeEnum)
from etl.services.cdc.factory import CdcFactroy

from core.models import Datasource, Dimension, Measure, QueueList, \
    DatasourceMeta
from django.conf import settings

from djcelery import celery
from celery import group
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


def load_data_mongo(user_id, task_id, data, channel):
    """
    Загрузка данных в mongo
    :param user_id: integer
    :param task_id: integer
    :param data: dict
    :param source: dict
    """

    print 'upload data to mongo'
    cols = json.loads(data['cols'])
    tables = json.loads(data['tables'])
    structure = data['tree']

    source_dict = data['source']
    source_model = Datasource()
    source_model.set_from_dict(**source_dict)

    col_names = []

    for t_name, col_group in groupby(cols, lambda x: x["table"]):
        for x in col_group:
            col_names.append(x["table"] + FIELD_NAME_SEP + x["col"])

    # название новой таблицы
    key = binascii.crc32(
        reduce(operator.add,
               [source_model.host, str(source_model.port), str(source_model.user_id),
                ','.join(sorted(tables))], ''))

    # collection
    collection_name = get_table_name('sttm_datasource', key)
    connection = pymongo.MongoClient(settings.MONGO_HOST, settings.MONGO_PORT)
    # database name
    db = connection.etl

    collection = db[collection_name]

    query = DataSourceService.get_rows_query_for_loading_task(
        source_model, structure, cols)

    # общее количество строк в запросе
    max_rows_count = DataSourceService.get_structure_rows_number(
        source_model, structure,  cols)

    instance = DataSourceService.get_source_connection(source_model)

    # создаем информацию о работе таска
    queue_storage = TaskService.get_queue(task_id)

    queue_storage['id'] = task_id
    queue_storage['user_id'] = user_id
    queue_storage['date_created'] = datetime_now_str()
    queue_storage['date_updated'] = None
    queue_storage['status'] = TaskStatusEnum.PROCESSING
    queue_storage['percent'] = 0

    # меняем статус задачи на 'В обработке'
    TaskService.update_task_status(task_id, TaskStatusEnum.PROCESSING)

    page = 1
    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    loaded_count = 0.0
    was_error = False
    up_to_100 = False
    err_msg = ''
    percent = 0

    # сообщаем о начале загрузке
    client.publish(channel, json.dumps(
        {'percent': 0, 'taskId': task_id, 'event': 'start'}))

    while True:
        query_load = query.format(limit, (page-1)*limit)
        cursor = instance.cursor()
        cursor.execute(query_load)
        result = cursor.fetchall()

        data_to_insert = []
        if not result:
            break
        # заполняем массив для вставки
        for record in result:
            record_normalized = [EtlEncoder.encode(rec_field) for rec_field in record]
            record_row = dict(izip(col_names, record_normalized))
            data_to_insert.append(record_row)
        try:
            collection.insert_many(data_to_insert, ordered=False)
            print 'inserted %d rows to mongo' % len(data_to_insert)
        except Exception as e:
            was_error = True
            # fixme перезаписывается при каждой ошибке
            err_msg = e.message
            print "Unexpected error:", type(e), e
            queue_storage['status'] = TaskStatusEnum.ERROR

            # сообщаем об ошибке
            client.publish(channel, json.dumps(
                {'percent': percent, 'taskId': task_id,
                 'event': 'error', 'message': err_msg}))

        # обновляем информацию о работе таска
        queue_storage['date_updated'] = datetime_now_str()

        loaded_count += limit
        percent = int(round(loaded_count/max_rows_count*100))

        if percent >= 100:
            queue_storage['percent'] = 100
            up_to_100 = True
            client.publish(
                channel, json.dumps(
                    {'percent': 100, 'taskId': task_id, 'event': 'finish'}))

        else:
            queue_storage['percent'] = percent
            client.publish(
                channel, json.dumps(
                    {'percent': percent, 'taskId': task_id, 'event': 'process'}))

        page += 1

    if was_error:
        # меняем статус задачи на 'Ошибка'
        TaskService.update_task_status(
            task_id, TaskStatusEnum.ERROR,
            error_code=TaskErrorCodeEnum.DEFAULT_CODE, error_msg=err_msg)
    else:
        # меняем статус задачи на 'Выполнено'
        TaskService.update_task_status(task_id, TaskStatusEnum.DONE, )

        queue_storage['date_updated'] = datetime_now_str()
        queue_storage['status'] = TaskStatusEnum.DONE

    if not was_error and not up_to_100:
        client.publish(channel, json.dumps(
            {'percent': 100, 'taskId': task_id, 'event': 'finish'}))

    # удаляем инфу о работе таска
    RedisSourceService.delete_queue(task_id)

    # удаляем канал из списка каналов юзера
    RedisSourceService.delete_user_subscriber(user_id, task_id)


@celery.task(name='etl.tasks.load_data')
def load_data(user_id, task_id, channel):

    task = QueueList.objects.get(id=task_id)

    # обрабатываем таски со статусом 'В ожидании'
    if task.queue_status.title == TaskStatusEnum.IDLE:

        data = json.loads(task.arguments)
        name = task.queue.name

        if name == 'etl:load_data:mongo':
            load_data_mongo(user_id, task_id, data, channel)
        elif name == 'etl:load_data:database':
            load_data_database(user_id, task_id, data, channel)


class RowKeysCreator(object):
    """
    Расчет ключа для таблицы
    """

    def __init__(self, table, cols, primary_key=None):
        self.table = table
        self.cols = cols
        self.primary_key = primary_key

    def calc_key(self, row, row_num):
        """
        Расчет ключа для строки таблицы либо по первичному ключу
        либо по номеру и значениям этой строки

        Args:
            row(tuple): Строка данных
            row_num(int): Номер строки

        Returns:
            int: Ключ строки для таблицы
        """
        if self.primary_key:
            return binascii.crc32(str(self.primary_key))
        l = [y for (x, y) in zip(self.cols, row) if x['table'] == self.table]
        l.append(row_num)
        return binascii.crc32(
                reduce(lambda res, x: '%s%s' % (res, x), l))

    def set_primary_key(self, data):
        """
        Установка первичного ключа, если есть

        Args:
            data(dict): метаданные по колонкам таблицы
        """

        for record in data['columns']:
            if record['is_primary']:
                self.primary_key = record['name']
                break


class Query(object):
    """
    Класс формирования и совершения запроса
    """

    def __init__(self, source_service, cursor=None, query=None):
        self.source_service = source_service
        self.cursor = cursor
        self.query = query

    def set_query(self, **kwargs):
        raise NotImplemented

    def execute(self, **kwargs):
        raise NotImplemented


class TableCreateQuery(Query):

    def set_connection(self):
        local_instance = self.source_service.get_local_instance()
        self.connection = local_instance.connection

    def set_query(self, **kwargs):
        local_instance = self.source_service.get_local_instance()

        self.query = self.source_service.get_table_create_query(
            local_instance,
            kwargs['table_name'],
            ', '.join(kwargs['cols'])
        )

    def execute(self):
        self.set_connection()
        self.cursor = self.connection.cursor()

        # create new table
        self.cursor.execute(self.query)
        self.connection.commit()
        return


class InsertQuery(TableCreateQuery):

    def set_query(self, **kwargs):
        local_instance = self.source_service.get_local_instance()
        insert_table_query = self.source_service.get_table_insert_query(
            local_instance, kwargs['table_name'])
        self.query = insert_table_query.format(
            '(%s)' % ','.join(['%({0})s'.format(i) for i in xrange(
                kwargs['cols_nums'])]))

    def execute(self, **kwargs):
        self.set_connection()
        self.cursor = self.connection.cursor()

        # create new table
        self.cursor.executemany(self.query, kwargs['data'])
        self.connection.commit()
        return


class RowsQuery(Query):

    def set_query(self, **kwargs):
        self.query = self.source_service.get_rows_query_for_loading_task(
            kwargs['source'], kwargs['structure'],  kwargs['cols'])

    def execute(self, **kwargs):
        connection = self.source_service.get_source_connection(kwargs['source'])
        self.cursor = connection.cursor()

        self.cursor.execute(self.query.format(kwargs['limit'], kwargs['offset']))
        return self.cursor.fetchall()


def load_data_database(user_id, task_id, data, channel):
    """Загрузка данных во временное хранилище
    Args:
        user_id: id пользователя
        task_id: id задачи
        data(dict): Данные

    Returns:
        func
    """

    def calc_key_for_row(row, tables_key_creator, row_num):
        """
        Расчет ключа для отдельно взятой строки

        Args:
            row(tuple): строка данных
            tables_key_creator(list of RowKeysCreator): список создателей ключей
            row_num(int): Номер строки

        Returns:
            tuple: Модифицированная строка с ключом в первой позиции
        """
        row_values_for_calc = [
            str(each.calc_key(row, row_num)) for each in tables_key_creator]
        return (binascii.crc32(''.join(row_values_for_calc)),) + row

    print 'load_data_database'

    cols = json.loads(data['cols'])
    col_types = json.loads(data['col_types'])
    structure = data['tree']
    tables_info_for_meta = json.loads(data['meta_info'])

    source_dict = data['source']
    source = Datasource()
    source.set_from_dict(**source_dict)

    col_names = ['"cdc_key" text']

    cols_str = ''

    for obj in cols:
        t = obj['table']
        c = obj['col']

        cols_str += '{0}-{1};'.format(t, c)

        dotted = '{0}.{1}'.format(t, c)

        col_names.append('"{0}{1}{2}" {3}'.format(
            t, FIELD_NAME_SEP, c, col_types[dotted]))

    # название новой таблицы
    key = generate_table_name_key(source, cols_str)

    source_table_name = get_table_name('sttm_datasource', key)

    # общее количество строк в запросе
    max_rows_count = DataSourceService.get_structure_rows_number(
        source, structure,  cols)

    # меняем статус задачи на 'В обработке'
    TaskService.update_task_status(task_id, TaskStatusEnum.PROCESSING)

    # создаем информацию о работе таска
    queue_storage = TaskService.get_queue(task_id)
    queue_storage['id'] = task_id
    queue_storage['user_id'] = user_id
    queue_storage['date_created'] = datetime_now_str()
    queue_storage['date_updated'] = None
    queue_storage['status'] = TaskStatusEnum.PROCESSING
    queue_storage['percent'] = 0

    table_create_query = TableCreateQuery(DataSourceService())
    table_create_query.set_query(table_name=source_table_name, cols=col_names)
    table_create_query.execute()

    cols_nums = len(cols)+1
    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    offset = 0

    rows_query = RowsQuery(DataSourceService())
    rows_query.set_query(source=source, structure=structure,  cols=cols)

    insert_query = InsertQuery(DataSourceService())
    insert_query.set_query(table_name=source_table_name, cols_nums=cols_nums)

    last_row = None
    settings_limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    loaded_count = 0.0
    was_error = False
    up_to_100 = False
    percent = 0

    tables_key_creator = []
    for key, value in tables_info_for_meta.iteritems():
        rkc = RowKeysCreator(table=key, cols=cols)
        rkc.set_primary_key(value)
        tables_key_creator.append(rkc)

    # сообщаем о начале загрузке
    client.publish(channel, json.dumps(
        {'percent': 0, 'taskId': task_id, 'event': 'start'}))

    while True:

        try:
            rows = rows_query.execute(
                source=source, limit=limit, offset=offset)
            if not rows:
                break
            # добавляем ключ в каждую запись таблицы
            rows_with_keys = map(lambda (ind, row): calc_key_for_row(
                row, tables_key_creator, offset + ind), enumerate(rows))
            # приходит [(1, 'name'), ...],
            # преобразуем в [{0: 1, 1: 'name'}, ...]
            rows_dict = map(
                lambda x: {str(k): v for (k, v) in izip(xrange(cols_nums), x)},
                rows_with_keys)

            insert_query.execute(data=rows_dict)
            offset += limit
        except Exception as e:
            print 'Exception'
            was_error = True
            # код и сообщение ошибки
            pg_code = getattr(e, 'pgcode', None)

            err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
            err_msg += e.message

            # меняем статус задачи на 'Ошибка'
            TaskService.update_task_status(
                task_id,
                TaskStatusEnum.ERROR,
                error_code=pg_code or TaskErrorCodeEnum.DEFAULT_CODE,
                error_msg=err_msg)
            logger.exception(err_msg)
            queue_storage['date_updated'] = datetime_now_str()
            queue_storage['status'] = TaskStatusEnum.ERROR

            # сообщаем об ошибке
            client.publish(channel, json.dumps(
                {'percent': percent, 'taskId': task_id,
                 'event': 'error', 'message': err_msg}))

            break
        else:
            last_row = rows[-1]  # получаем последнюю запись
            # обновляем информацию о работе таска
            loaded_count += settings_limit
            queue_storage['date_updated'] = datetime_now_str()

            percent = int(round(loaded_count/max_rows_count*100))
            if percent >= 100:
                queue_storage['percent'] = 100
                up_to_100 = True
                client.publish(
                    channel, json.dumps(
                        {'percent': 100, 'taskId': task_id, 'event': 'finish'}))

            else:
                queue_storage['percent'] = percent
                client.publish(
                    channel, json.dumps(
                        {'percent': percent, 'taskId': task_id, 'event': 'process'}))

    if not was_error:
        # меняем статус задачи на 'Выполнено'
        TaskService.update_task_status(task_id, TaskStatusEnum.DONE, )

        queue_storage['date_updated'] = datetime_now_str()
        queue_storage['status'] = TaskStatusEnum.DONE

        if not up_to_100:
            client.publish(channel, json.dumps(
                {'percent': 100, 'taskId': task_id, 'event': 'finish'}))

    # удаляем инфу о работе таска
    RedisSourceService.delete_queue(task_id)

    # удаляем канал из списка каналов юзера
    RedisSourceService.delete_user_subscriber(user_id, task_id)

    # работа с datasource_meta
    meta_tables = DataSourceService.update_datasource_meta(
        key, source, cols, tables_info_for_meta, last_row)

    create_load_mechanism.apply_async((source_dict, data['for_triggers'], ),)

    return create_dimensions_and_measures(
        user_id, source, source_table_name, meta_tables, key)


@celery.task(name='etl.tasks.create_triggers')
def create_load_mechanism(source_dict, tables_info):
    print 'create_triggers is started'

    source = Datasource()
    source.set_from_dict(**source_dict)

    CdcFactroy.create_load_mechanism(source, tables_info)


def create_dimensions_and_measures(
        user_id=None, source=None, source_table_name=None, meta_tables=None, key=None):
    """Создание таблиц размерностей

    Args:
        user_id(int): идентификатор пользователя
        key(int): ключ к метаданным обрабатываемой таблицы
    Returns:

    """
    # arguments = dict(
    #     user_id=11,
    #     datasource_id=3,
    #     source_table='sttm_datasource_948655626',
    #     key=948655626,
    #     target_table='dimensions_948655626'
    # )
    arguments = dict(
        user_id=user_id,
        datasource_id=source.id,
        source_table=source_table_name,
        meta_tables=meta_tables,
        key=key,
        target_table=get_table_name('dimensions', key)
    )

    dimension_task = TaskService('etl:database:generate_dimensions')
    dimension_task_id, channel = dimension_task.add_task(arguments)
    dimension = DimensionCreation()
    arguments.update({'target_table': get_table_name('measures', key)})

    measure_task = TaskService('etl:database:generate_measures')
    measure_task_id, channel = measure_task.add_task(arguments)
    measure = MeasureCreation()
    # dimension.load_data(dimension_task_id)
    # measure.load_data(measure_task_id)

    group([
        dimension.load_data.s(dimension_task_id),
        measure.load_data.s(measure_task_id)
    ])()


class DimensionCreation(OlapEntityCreation):
    """
    Создание размерностей
    """

    actual_fields_type = ['text']

    def save_meta_data(self, user_id, key, fields, meta_tables):
        """
        Сохраняем информацию о размерности
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

    @celery.task(name='etl:database:generate_dimensions', filter=task_method)
    def load_data(self, task_id):
        """
        Создание размерностей
        """
        super(DimensionCreation, self).load_data(task_id)


class MeasureCreation(OlapEntityCreation):
    """
    Создание мер
    """

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

    @celery.task(name='etl:database:generate_measures', filter=task_method)
    def load_data(self, task_id):
        """
        Создание размерностей
        """
        super(MeasureCreation, self).load_data(task_id)

# write in console: python manage.py celery -A etl.tasks worker
