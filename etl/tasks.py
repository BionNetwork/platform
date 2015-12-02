# coding: utf-8

import os
import sys
import brukva
import pymongo
import json

import operator
import binascii
from psycopg2 import errorcodes
from etl.constants import FIELD_NAME_SEP
from etl.services.db.factory import DatabaseService
from etl.services.model_creation import OlapEntityCreation
from etl.services.middleware.base import (EtlEncoder, generate_table_name_key, get_table_name, datetime_now_str)
from .helpers import (RedisSourceService, DataSourceService,
                      TaskService, TaskStatusEnum,
                      TaskErrorCodeEnum)
from core.models import Datasource, DatasourceMetaKeys, Dimension, Measure, QueueList
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

        # обновляем информацию о работе таска
        queue_storage['date_updated'] = datetime_now_str()

        loaded_count += limit
        percent = int(round(loaded_count/max_rows_count*100))

        if percent >= 100:
            queue_storage['percent'] = 100
            up_to_100 = True
            client.publish(
                channel, json.dumps({'percent': 100, 'taskId': task_id}))

        else:
            queue_storage['percent'] = percent
            client.publish(
                channel, json.dumps({'percent': percent, 'taskId': task_id}))

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
        client.publish(channel, json.dumps({'percent': 100, 'taskId': task_id}))

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


def load_data_database(user_id, task_id, data, channel):
    """Загрузка данных во временное хранилище
    Args:
        user_id: id пользователя
        task_id: id задачи
        data(dict): Данные

    Returns:
        func
    """

    print 'load_data_database'

    cols = json.loads(data['cols'])
    col_types = json.loads(data['col_types'])
    structure = data['tree']
    tables_info_for_meta = json.loads(data['meta_info'])

    source_dict = data['source']
    source = Datasource()
    source.set_from_dict(**source_dict)

    col_names_create = []

    cols_str = ''

    for obj in cols:
        t = obj['table']
        c = obj['col']

        cols_str += '{0}-{1};'.format(t, c)

        dotted = '{0}.{1}'.format(t, c)

        col_names_create.append('"{0}{1}{2}" {3}'.format(
            t, FIELD_NAME_SEP, c, col_types[dotted]))

    # название новой таблицы
    key = generate_table_name_key(source, cols_str)

    table_key = get_table_name('sttm_datasource', key)

    rows_query = DataSourceService.get_rows_query_for_loading_task(
            source, structure,  cols)

    # общее количество строк в запросе
    max_rows_count = DataSourceService.get_structure_rows_number(
        source, structure,  cols)

    # инстанс подключения к локальному хранилищу данных
    local_instance = DataSourceService.get_local_instance()

    create_table_query = DatabaseService.get_table_create_query(
        local_instance, table_key, ', '.join(col_names_create))

    insert_table_query = DatabaseService.get_table_insert_query(
        local_instance, table_key)

    connection = local_instance.connection
    cursor = connection.cursor()

    # create new table
    cursor.execute(create_table_query)
    connection.commit()

    # достаем первую порцию данных
    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    offset = 0

    # конекшн, курсор пользовательского коннекта
    source_conn = DataSourceService.get_source_connection(source)
    rows_cursor = source_conn.cursor()

    rows_cursor.execute(rows_query.format(limit, offset))
    rows = rows_cursor.fetchall()

    len_ = len(rows[0]) if rows else 0

    # преобразуем строку инсерта в зависимости длины вставляемой строки
    insert_query = insert_table_query.format(
        '(' + ','.join(['%({0})s'.format(i) for i in xrange(len_)]) + ')')

    last_row = None

    settings_limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    loaded_count = 0.0
    was_error = False
    up_to_100 = False

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

    while rows:
        try:
            # приходит [(1, 'name'), ...],
            # преобразуем в [{0: 1, 1: 'name'}, ...]
            dicted = map(
                lambda x: {str(k): v for (k, v) in izip(xrange(len_), x)}, rows)

            cursor.executemany(insert_query, dicted)
        except Exception as e:
            err_msg = ''
            err_code = TaskErrorCodeEnum.DEFAULT_CODE

            # код и сообщение ошибки
            pg_code = getattr(e, 'pgcode', None)
            if pg_code is not None:
                err_code = pg_code
                err_msg = errorcodes.lookup(pg_code) + ': '

            err_msg += e.message

            print 'Exception'
            was_error = True

            # меняем статус задачи на 'Ошибка'
            TaskService.update_task_status(
                task_id, TaskStatusEnum.ERROR,
                error_code=err_code, error_msg=err_msg)

            queue_storage['date_updated'] = datetime_now_str()
            queue_storage['status'] = TaskStatusEnum.ERROR

            break
        else:
            # коммитим пачку в бд
            connection.commit()
            # достаем последнюю запись
            last_row = rows[-1]
            # достаем новую порцию данных
            offset += limit
            rows_cursor.execute(rows_query.format(limit, offset))
            rows = rows_cursor.fetchall()

            loaded_count += settings_limit

            # обновляем информацию о работе таска
            queue_storage['date_updated'] = datetime_now_str()

            percent = int(round(loaded_count/max_rows_count*100))
            if percent >= 100:
                queue_storage['percent'] = 100
                up_to_100 = True
                client.publish(
                    channel, json.dumps({'percent': 100, 'taskId': task_id}))

            else:
                queue_storage['percent'] = percent
                client.publish(
                    channel, json.dumps({'percent': percent, 'taskId': task_id}))

    if not was_error:
        # меняем статус задачи на 'Выполнено'
        TaskService.update_task_status(task_id, TaskStatusEnum.DONE, )

        queue_storage['date_updated'] = datetime_now_str()
        queue_storage['status'] = TaskStatusEnum.DONE

    if not was_error and not up_to_100:
        client.publish(channel, json.dumps({'percent': 100, 'taskId': task_id}))

    # удаляем инфу о работе таска
    RedisSourceService.delete_queue(task_id)

    # удаляем канал из списка каналов юзера
    RedisSourceService.delete_user_subscriber(user_id, task_id)

    # работа с datasource_meta
    datasource_meta = DataSourceService.update_datasource_meta(
        table_key, source, cols, tables_info_for_meta, last_row)
    DatasourceMetaKeys.objects.get_or_create(
        meta=datasource_meta,
        value=key,
    )
    return create_dimensions_and_measures(user_id, source, table_key, key)


def create_dimensions_and_measures(user_id, source, table_key, key):
    """Создание таблиц размерностей

    Args:
        user_id(int): идентификатор пользователя
        key(int): ключ к метаданным обрабатываемой таблицы
    Returns:

    """

    arguments = dict(
        user_id=user_id,
        datasource_id=source.id,
        source_table=table_key,
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

    group([
        dimension.load_data.s(dimension_task_id),
        measure.load_data.s(measure_task_id)
    ])()


class DimensionCreation(OlapEntityCreation):
    """
    Создание размерностей
    """

    actual_fields_type = ['text']

    def save_meta_data(self, user_id, table_name, fields):
        """
        Сохраняем информацию о размерности
        """
        level = dict()
        for field in fields:
            level.update(dict(
                type=field['type'], level_type='regular', visible=True,
                column=field['name'], unique_members=field['is_unique'],
                caption=field['name'],
                )
            )

            data = dict(
                name=field['name'],
                has_all=True,
                table_name=field['name'],
                level=level,
                primary_key='id',
                foreign_key=None
            )

            Dimension.objects.create(
                name=field['name'],
                title=field['name'],
                user_id=user_id,
                datasources_meta=self.meta,
                data=json.dumps(data)
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

    def save_meta_data(self, user_id, table_name, fields):
        """
        Сохранение информации о мерах
        """
        for field in fields:

            Measure.objects.create(
                name=field['name'],
                title=field['name'],
                type=field['type'],
                user_id=user_id,
                datasources_meta=self.meta
            )

    @celery.task(name='etl:database:generate_measures', filter=task_method)
    def load_data(self, task_id):
        """
        Создание размерностей
        """
        super(MeasureCreation, self).load_data(task_id)

# write in console: python manage.py celery -A etl.tasks worker
