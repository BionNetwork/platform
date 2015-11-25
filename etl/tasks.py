# coding: utf-8

import os
import sys
import brukva
import pymongo
import json

import operator
import binascii
from psycopg2 import errorcodes

from etl.models import App, Model, Field, Setting
from etl.services.model_creation import type_match, create_model, install
from .helpers import (RedisSourceService, DataSourceService, EtlEncoder,
                      TaskService, generate_table_name_key, TaskStatusEnum, TaskErrorCodeEnum)
from core.models import Datasource, DatasourceMetaKeys, Dimension, User, Measure
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

    # меняем статус задачи на 'В обработке'
    RedisSourceService.update_task_status(
        user_id, task_id, TaskStatusEnum.PROCESSING)

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
        RedisSourceService.update_task_status(
            user_id, task_id, TaskStatusEnum.ERROR,
            error_code=TaskErrorCodeEnum.DEFAULT_CODE, error_msg=err_msg)
    else:
        # меняем статус задачи на 'Выполнено'
        RedisSourceService.update_task_status(
            user_id, task_id, TaskStatusEnum.DONE, )


@celery.task(name='etl.tasks.load_data')
def load_data(user_id, task_id):

    task = RedisSourceService.get_user_task_by_id(user_id, task_id)

    # обрабатываем таски со статусом 'В ожидании'
    if task['status_id'] == TaskStatusEnum.IDLE:
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
    RedisSourceService.update_task_status(
        user_id, task_id, TaskStatusEnum.PROCESSING)

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
            RedisSourceService.update_task_status(
                user_id, task_id, TaskStatusEnum.ERROR,
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
        RedisSourceService.update_task_status(
            user_id, task_id, TaskStatusEnum.DONE, )

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


def create_dimensions(user_id=11, key='-767084929'):
    """Создание таблиц размерностей

    Args:
        user_id(int): идентификатор пользователя
        key(str): ключ к метаданным обрабатываемой таблицы
    Returns:

    """
    store = DataStore(key)
    store.create_store_model()
    dimension_task = TaskService('etl:database:generate_dimensions')
    measure_task = TaskService('etl:database:generate_measures')
    dimension_task_id = dimension_task.add_dim_task(user_id, key)
    measure_task_id = measure_task.add_dim_task(user_id, key)
    dimension, measure = DimensionCreation(store), MeasureCreation(store)

    # dimension.load_data(user_id, dimension_task_id)
    # measure.load_data(user_id, measure_task_id)

    res = group([
        dimension.load_data.s(user_id, dimension_task_id),
        measure.load_data.s(user_id, measure_task_id)
    ])()


REGULAR = 'regular'


class DataStore(object):

    def __init__(self, key, module='biplatform.etl.models', app_name='sttm',
                 table_prefix='datasource'):
        self.module = module
        self.app_name = app_name

        self.key = key if not key.startswith('-') else '_%s' % key[1:]
        self.meta = DatasourceMetaKeys.objects.get(value=key).meta
        self.meta_data = json.loads(self.meta.fields)

        self.model = None
        self.table_name = '{prefix}_{key}'.format(
            prefix=table_prefix, key=self.key)

    @property
    def fields_list(self):
        """
        Получение списка полей таблицы
        """
        all_fields = []
        for table, fields in self.meta_data['columns'].iteritems():
            all_fields.extend([field for field in fields])
        return all_fields

    def create_store_model(self):
        # Создаем мнимое приложение
        app, app_create = App.objects.get_or_create(
            name=self.app_name, module=self.module)

        # Получаем доступ к обрабатываемой таблице как к django-модели
        self.model, model_create = Model.objects.get_or_create(
            app=app, name=self.table_name)

        # django_fields_dict = {}
        all_fields = []
        for table, fields in self.meta_data['columns'].iteritems():

            for field in fields:
                field['name'] = '%s--%s' % (table, field['name'])
                all_fields.append(field)
                f, field_create = Field.objects.get_or_create(
                    model=self.model, name=field['name'],
                    type=type_match[field['type']])
                if field['type'] in ['text']:
                    Setting.objects.get_or_create(
                        field=f, name='max_length', value=255)


class OlapEntityCreation(object):
    """
    Создание сущностей(рамерности, измерения) олап куба
    """

    actual_fields_type = []

    def __init__(self, source):
        self.source = source
        # self.table_model = source.model.get_django_model()

    @property
    def actual_fields(self):
        return [element for element in self.source.fields_list
                if element['type'] in self.actual_fields_type]

    def load_data(self, user_id, task_id):
        raise NotImplementedError

    def save_meta_data(self, user_id, field):
        raise NotImplementedError

    def save_fields(self, field_models, fields_info):
        """Заполняем таблицу данными

        Args:
            table_model():
        """
        print field_models
        dim_fields_name = [element['name'] for element in fields_info]
        index = 0
        while True:
            index_to = index+settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
            data = self.source.model.get_django_model().objects.values(
                *dim_fields_name)[index:index_to]
            print data[:2]
            if not data:
                break
            for field, dim in field_models.iteritems():
                dim_data = [dim(**{k: v for (k, v) in x.iteritems() if k == field})
                            for x in data]
                print dim
                dim.objects.bulk_create(dim_data)
            index = index_to
            print index


class DimensionCreation(OlapEntityCreation):
    """
    Создание размерностей
    """

    actual_fields_type = ['text']

    def save_meta_data(self, user_id, field):
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
        dim.datasources_meta = self.source.meta

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

    @celery.task(name='etl:database:generate_dimensions', filter=task_method)
    def load_data(self, user_id, task_id):
        """
        Создание размерностей

        Args:
            user_id(int): id Пользователя
            task_id(int): id Задачи
        """
        task = RedisSourceService.get_user_task_by_id(user_id, task_id)

        # Отбираем текстовые поля

        # Создаем размерности
        dimensions = {}
        actual_fields = self.actual_fields
        for field in actual_fields:
            field_name = field['name']
            dim_table_name = '%s_%s' % (field_name, self.source.key)
            dim_model, dim_model_create = Model.objects.get_or_create(
                app=App.objects.get(name=self.source.app_name), name=dim_table_name)
            if not dim_model_create:
                print 'continue'
                continue
            dim_field = Field.objects.create(
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
            self.save_meta_data(user_id, field)

            dimensions.update({field_name: dimension_table})

        self.save_fields(dimensions, actual_fields)


class MeasureCreation(OlapEntityCreation):
    """
    Создание мер
    """

    actual_fields_type = ['integer', 'timestamp']

    def save_meta_data(self, user_id, field):
        """
        Сохранение информации о мерах

        Args:
            user_id(int): id пользователя
            field(dict): данные о поле
            meta(DatasourceMeta): ссылка на метаданные хранилища
        """
        measure = Measure()
        measure.name = field['name']
        measure.title = field['name']
        measure.user_id = user_id
        measure.datasources_meta = self.source.meta
        measure.save()

    @celery.task(name='etl:database:generate_dimensions', filter=task_method)
    def load_data(self, user_id, task_id):
        """
        Создание размерностей

        Args:
            user_id(int): id Пользователя
            task_id(int): id Задачи
        """
        task = RedisSourceService.get_user_task_by_id(user_id, task_id)

        # Отбираем текстовые поля
        actual_fields = self.actual_fields

        # Создаем размерности
        measures = {}
        for field in actual_fields:
            field_name = field['name']
            measure_table_name = '%s_%s' % (field_name, self.source.key)
            measure_model, measure_model_create = Model.objects.get_or_create(
                app=App.objects.get(name=self.source.app_name),
                name=measure_table_name)
            if not measure_model_create:
                continue
            measure_field, field_create = Field.objects.create(
                model=measure_model, name=field_name,
                type=type_match[field['type']])
            Setting.objects.bulk_create([
                Setting(field=measure_field, name='null', value=True),
                Setting(field=measure_field, name='blank', value=True),
                ]
            )
            measure_table = measure_model.get_django_model()
            install(measure_table)

            # Сохраняем метаданные о мере
            self.save_meta_data(user_id, field)

            measures.update({field_name: measure_table})

        self.save_fields(measures, actual_fields)

# write in console: python manage.py celery -A etl.tasks worker
