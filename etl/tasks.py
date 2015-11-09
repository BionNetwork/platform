# coding: utf-8

import os
import sys
import brukva
import psycopg2
import psycopg2.extensions
import pymongo
import json

import operator
import binascii

from .helpers import RedisSourceService, DataSourceService, EtlEncoder
from core.models import Datasource
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
            columns.append(x["table"] + "." + x["col"])
            col_names.append(x["table"] + "__" + x["col"])

    # название новой таблицы
    key = binascii.crc32(
        reduce(operator.add,
               [source_model.host, str(source_model.port), str(source_model.user_id),
                ','.join(sorted(tables))], ''))

    # collection
    collection_name = '{0}{1}{2}'.format('sttm_datasoruce_', '_' if key < 0 else '', abs(key))
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
            record_row = dict(zip(col_names, record_normalized))
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


def load_data_database(user_id, task_id, source, table_name, create_query,
              rows_query, insert_query, cols):

    # сокет канал
    chanel = 'jobs:etl:extract:{0}:{1}'.format(user_id, task_id)
    # for i in range(1, 11):
    client.publish(chanel, 10*10)
    # time.sleep(2)

    connection = DataSourceService.get_local_connection()
    cursor = connection.cursor()

    # для юникода
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, connection)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, connection)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cursor)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, cursor)

    # create new table
    cursor.execute(create_query)
    connection.commit()

    # достаем первую порцию данных
    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    offset = 0

    # конекшн, курсор пользовательского коннекта
    source_conn = DataSourceService.get_source_connection(source)
    rows_cursor = source_conn.cursor()

    # для юникода
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, source_conn)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, source_conn)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, rows_cursor)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, rows_cursor)

    rows_cursor.execute(rows_query.format(limit, offset))
    rows = rows_cursor.fetchall()
    len_ = len(rows[0])

    # преобразуем строку инсерта в зависимости длины вставляемой строки
    insert_query = insert_query.format('(' + ','.join(['%({0})s'.format(i) for i in xrange(len_)]) + ')')

    last_row = None

    while rows:
        try:
            # приходит [(1, 'name'), ...], преобразуем в [{0: 1, 1: 'name'}, ...]
            dicted = map(lambda x: {str(k): v for (k, v) in izip(xrange(len_), x)}, rows)
            cursor.executemany(insert_query, dicted)
        except Exception:
            # FIXME обработать еррор
            print 'Exception'
            rows = []
        else:
            # коммитим пачку в бд
            connection.commit()
            # достаем последнюю запись
            last_row = rows[-1]
            # достаем новую порцию данных
            offset += limit
            rows_cursor.execute(rows_query.format(limit, offset))
            rows = rows_cursor.fetchall()

    # работа с datasource_meta
    DataSourceService.update_datasource_meta(table_name, source, cols, last_row)


# write in console: python manage.py celery -A etl.tasks worker
