# coding: utf-8

import os
import sys
import brukva
import pymongo
import json

import operator
import binascii

from etl.models import App, Model, Field, Setting
from etl.services.model_creation import type_match, create_model, install
from .helpers import (RedisSourceService, DataSourceService, EtlEncoder,
                      TaskService, generate_table_name_key)
from core.models import Datasource, DatasourceMetaKeys, Dimension, User
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


def get_table_key(key):
    """
    название новой таблицы
    :param key: str
    :return:
    """
    return '{0}{1}{2}'.format('sttm_datasource_', '_' if key < 0 else '', abs(key))


def load_data_mongo(user_id, task_id, data, source):
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
    source_model = Datasource()
    source_model.set_from_dict(**source)

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
            print "Unexpected error:", type(e), e
        page += 1


@celery.task(name='etl.tasks.load_data')
def load_data(user_id, task_id):

    task = RedisSourceService.get_user_task_by_id(user_id, task_id)

    if task['name'] == 'etl:load_data:mongo':
        load_data_mongo(user_id, task_id, task['data'], task['source'])
    elif task['name'] == 'etl:load_data:database':
        load_data_database(user_id, task_id, task['data'], task['source'])


def load_data_database(user_id, task_id, data, source_dict):
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
    source = Datasource()
    source.set_from_dict(**source_dict)

    col_names_create = []

    cols_str = ''

    for obj in cols:
        t = obj['table']
        c = obj['col']

        cols_str += '{0}-{1};'.format(t, c)

        dotted = '{0}.{1}'.format(t, c)

        col_names_create.append('"{0}__{1}" {2}'.format(t, c, col_types[dotted]))

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

    while rows:
        try:
            # приходит [(1, 'name'), ...],
            # преобразуем в [{0: 1, 1: 'name'}, ...]
            dicted = map(
                lambda x: {str(k): v for (k, v) in izip(xrange(len_), x)}, rows)

            cursor.executemany(insert_query, dicted)
        except Exception as e:
            # FIXME обработать еррор
            print e
            rows = []
            was_error = True
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

    if not was_error and not up_to_100:
        client.publish(chanel, 100)

    # работа с datasource_meta

    datasource_meta = DataSourceService.update_datasource_meta(
        table_key, source, cols, tables_info_for_meta, last_row)

    DatasourceMetaKeys.objects.get_or_create(
        meta=datasource_meta,
        value=key,
    )
    return create_dimensions(user_id, key)


def create_dimensions(user_id, key):
    """Создание таблиц размерностей

    Args:
        key(str): ключ к метаданным обрабатываемой таблицы
    Returns:

    """
    task = TaskService('etl:database:generate_dimensions')
    task_id = task.add_dim_task(user_id, key)
    load_dim_data.apply_async((user_id, task_id),)


REGULAR = 'regular'


def save_dim_info(user_id, field, meta):
    """
    Сохраняем информацию о размерности

    Args:
        user_id(int): id пользователя
        field(dict): данные о поле
        meta(DatasourceMeta): ссылка на метаданные хранилища
    """
    dim = Dimension()
    dim.name = field['name']
    dim.title = field['name']
    dim.user_id = user_id
    dim.datasources_meta = meta

    data = dict(
        name=field['name'],
        has_all=True,
        table_name=field['name'],
        level=dict(
            type=field['type'],
            level_type=REGULAR,
            visible=True,
            column=field['name'],
            unique_members=field['is_unique'],
            caption=field['name'],
        ),
        primary_key='id',
        foreign_key=field['name']
    )

    dim.data = json.dumps(data)
    dim.save()


@celery.task(name='etl:database:generate_dimensions')
def load_dim_data(user_id, task_id):
    """
    Создание размерностей

    Args:
        user_id(int): id Пользователя
        task_id(int): id Задачи
    """
    task = RedisSourceService.get_user_task_by_id(user_id, task_id)

    # Получаем метаданные
    key = str(task['meta_db_key'])
    meta_key = DatasourceMetaKeys.objects.get(value=key)
    key = key if not key.startswith('-') else '_%s' % key[1:]
    meta = meta_key.meta
    meta_data = json.loads(meta.fields)

    # Создаем мнимое приложение
    app, app_create = App.objects.get_or_create(
        name=u'dimensions', module=u'biplatform.dimensions.models')

    # Получаем доступ к обрабатываемой таблице как к django-модели
    table_name = 'sttm_datasource_%s' % key
    model, model_create = Model.objects.get_or_create(app=app, name=table_name)

    django_fields_dict = {}
    all_fields = []
    for table, fields in meta_data['columns'].iteritems():

        for field in fields:
            print field
            field['name'] = '%s_%s' % (table, field['name'])
            all_fields.append(field)
            f, field_create = Field.objects.get_or_create(
                model=model, name=field['name'], type=type_match[field['type']])
            if field['type'] in ['text']:
                Setting.objects.get_or_create(
                    field=f, name='max_length', value=255)
            django_fields_dict.update({field['name']: f.get_django_field()})

    table_model = create_model(
        name=u'EtlTable', app_label=u'dimensions', fields=django_fields_dict,
        options={'db_table': table_name})

    # Отбираем текстовые поля
    actual_fields = [
        element for element in all_fields if element['type'] == 'text']

    # Создаем размерности
    dimensions = {}
    for field in actual_fields:
        field_name = field['name']
        dim_table_name = '%s_%s' % (field_name, key)
        dim_model, dim_model_create = Model.objects.get_or_create(
            app=app, name=dim_table_name)
        if not dim_model_create:
            continue
        dim_field, field_create = Field.objects.create(
            model=dim_model, name=field_name, type='CharField')
        Setting.objects.bulk_create([
            Setting(field=dim_field, name='max_length', value=255),
            Setting(field=dim_field, name='null', value=True),
            Setting(field=dim_field, name='blank', value=True),
            ]
        )
        dimension_table = dim_model.get_django_model()
        install(dimension_table)

        # Сохраняем метаданные об измерении
        save_dim_info(user_id, field, meta)

        dimensions.update({field_name: dimension_table})

    # Заполняем измерения данными
    actual_fields_name = [
        element['name'] for element in all_fields if element['type'] == 'text']
    index = 0
    while True:
        index_to = index+settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        data = table_model.objects.values(*actual_fields_name)[index:index_to]
        if not data:
            break
        for field, dim in dimensions.iteritems():
            dim_data = [dim(**{k: v for (k, v) in x.iteritems() if k == field})
                        for x in data]
            dim.objects.bulk_create(dim_data)
        index = index_to


# write in console: python manage.py celery -A etl.tasks worker
