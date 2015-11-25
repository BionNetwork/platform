# coding: utf-8

import os
import sys
import brukva
import pymongo
import json

import operator
import binascii
from psycopg2 import errorcodes

from etl.services.model_creation import install, get_django_model, \
    get_field_settings, DataStore, OlapEntityCreation
from .helpers import (DataSourceService, EtlEncoder,
                      TaskService, generate_table_name_key, TaskStatusEnum,
                      TaskErrorCodeEnum)
from core.models import (
    Datasource, DatasourceMetaKeys, Dimension, Measure, QueueList)
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


def get_table_key(key):
    """
    название новой таблицы
    :param key: str
    :return:
    """
    return '{0}{1}{2}'.format('sttm_datasource_', '_' if key < 0 else '', abs(key))


def load_data_mongo(user_id, task_id, data):
    """
    Загрузка данных в mongo
    :param user_id: integer
    :param task_id: integer
    :param data: dict
    :param source: dict
    """
    client.publish('jobs:etl:extract:{0}:{1}'.format(user_id, task_id), 'loading task enabled')
    print 'upload data to mongo'
    cols = json.loads(data['cols'])
    tables = json.loads(data['tables'])
    structure = data['tree']

    source_dict = data['source']
    source_model = Datasource()
    source_model.set_from_dict(**source_dict)

    columns = []
    col_names = []

    for t_name, col_group in groupby(cols, lambda x: x["table"]):
        for x in col_group:
            columns += col_group
            col_names.append(x["table"] + "__" + x["col"])

    # название новой таблицы
    key = binascii.crc32(
        reduce(operator.add,
               [source_model.host, str(source_model.port), str(source_model.user_id),
                ','.join(sorted(tables))], ''))

    # collection
    collection_name = get_table_key(key)
    connection = pymongo.MongoClient(settings.MONGO_HOST, settings.MONGO_PORT)
    # database name
    db = connection.etl

    collection = db[collection_name]

    query = DataSourceService.get_rows_query_for_loading_task(source_model, structure, columns)
    instance = DataSourceService.get_source_connection(source_model)
    page = 1
    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

    # меняем статус задачи на 'В обработке'
    TaskService.update_task_status(task_id, TaskStatusEnum.PROCESSING)

    was_error = False
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
        page += 1

    if was_error:
        # меняем статус задачи на 'Ошибка'
        TaskService.update_task_status(
            task_id, TaskStatusEnum.ERROR,
            error_code=TaskErrorCodeEnum.DEFAULT_CODE, error_msg=err_msg)
    else:
        # меняем статус задачи на 'Выполнено'
        TaskService.update_task_status(task_id, TaskStatusEnum.DONE, )


@celery.task(name='etl.tasks.load_data')
def load_data(user_id, task_id):

    task = QueueList.objects.get(id=task_id)

    # обрабатываем таски со статусом 'В ожидании'
    if task.queue_status.title == TaskStatusEnum.IDLE:

        data = json.loads(task.arguments)
        name = task.queue.name

        if name == 'etl:load_data:mongo':
            load_data_mongo(user_id, task_id, data)
        elif name == 'etl:load_data:database':
            load_data_database(user_id, task_id, data)


def load_data_database(user_id, task_id, data):
    """Загрузка данных во временное хранилище
    Args:
        data(dict): Данные
        source_dict(dict): Словарь с параметрами источника

    Returns:
        func
    """

    print 'load_data_database'

    # сокет канал
    chanel = 'jobs:etl:extract:{0}:{1}'.format(user_id, task_id)

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

        col_names_create.append('"{0}--{1}" {2}'.format(t, c, col_types[dotted]))

    # название новой таблицы
    key = generate_table_name_key(source, cols_str)

    table_key = get_table_key(key)

    rows_query = DataSourceService.get_rows_query_for_loading_task(
            source, structure,  cols)

    # общее количество строк в запросе
    max_rows_count = DataSourceService.get_structure_rows_number(
        source, structure,  cols)

    # инстанс подключения к локальному хранилищу данных
    local_instance = DataSourceService.get_local_instance()

    create_table_query = TaskService.table_create_query_for_loading_task(
        local_instance, table_key, ', '.join(col_names_create))

    insert_table_query = TaskService.table_insert_query_for_loading_task(
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

            percent = int(round(loaded_count/max_rows_count*100))

            if percent >= 100:
                up_to_100 = True
                client.publish(chanel, 100)
            else:
                client.publish(chanel, percent)

    if not was_error:
        # меняем статус задачи на 'Выполнено'
        TaskService.update_task_status(task_id, TaskStatusEnum.DONE, )

    if not was_error and not up_to_100:
        client.publish(chanel, 100)

    # работа с datasource_meta

    datasource_meta = DataSourceService.update_datasource_meta(
        table_key, source, cols, tables_info_for_meta, last_row)

    DatasourceMetaKeys.objects.get_or_create(
        meta=datasource_meta,
        value=key,
    )
    return create_dimensions_and_measures(user_id, key)


def create_dimensions_and_measures(user_id, key):
    """Создание таблиц размерностей

    Args:
        user_id(int): идентификатор пользователя
        key(str): ключ к метаданным обрабатываемой таблицы
    Returns:

    """
    store = DataStore(key)

    dimension_task = TaskService('etl:database:generate_dimensions')
    dimension_task_id = dimension_task.add_dim_task(user_id, key)
    dimension = DimensionCreation(store)

    measure_task = TaskService('etl:database:generate_measures')
    measure_task_id = measure_task.add_dim_task(user_id, key)
    measure = MeasureCreation(store)

    group([
        dimension.load_data.s(user_id, dimension_task_id),
        measure.load_data.s(user_id, measure_task_id)
    ])()


class DimensionCreation(OlapEntityCreation):
    """
    Создание размерностей
    """

    actual_fields_type = ['text']

    def save_meta_data(self, user_id, field):
        """
        Сохраняем информацию о размерности
        """
        f_name = field['name']
        data = dict(
            name=f_name,
            has_all=True,
            table_name=f_name,
            level=dict(
                type=f_name, level_type='regular', visible=True,
                column=f_name, unique_members=field['is_unique'],
                caption=f_name,
            ),
            primary_key='id',
            foreign_key=f_name
        )

        Dimension.objects.create(
            name=f_name,
            title=f_name,
            user_id=user_id,
            datasources_meta=self.source.meta,
            data=json.dumps(data)
        )

    @celery.task(name='etl:database:generate_dimensions', filter=task_method)
    def load_data(self, user_id, task_id):
        """
        Создание размерностей
        """
        super(DimensionCreation, self).load_data(user_id, task_id)


class MeasureCreation(OlapEntityCreation):
    """
    Создание мер
    """

    actual_fields_type = ['integer', 'timestamp']

    def save_meta_data(self, user_id, field):
        """
        Сохранение информации о мерах
        """
        f_name = field['name']
        # TODO: Разобраться с соотв. типов
        Measure.objects.create(
            name=f_name,
            title=f_name,
            user_id=user_id,
            datasources_meta=self.source.meta
        )

    @celery.task(name='etl:database:generate_measures', filter=task_method)
    def load_data(self, user_id, task_id):
        """
        Создание размерностей
        """
        super(MeasureCreation, self).load_data(user_id, task_id)

# write in console: python manage.py celery -A etl.tasks worker
