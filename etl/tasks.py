# coding: utf-8

import os
import sys
import brukva
import psycopg2
import pymongo
import json

import operator
import binascii

from . import r_server
from .helpers import RedisSourceService, DataSourceService, EtlEncoder
from core.models import Datasource
from django.conf import settings

from djcelery import celery
from itertools import groupby

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

client = brukva.Client(host=settings.REDIS_HOST,
                       port=int(settings.REDIS_PORT),
                       selected_db=settings.REDIS_DB)
client.connect()

# подключение к локальной БД
DB_INFO = settings.DATABASES['default']
conn_str = (u"host='{host}' dbname='{db}' user='{login}' "
            u"password='{password}' port={port}").format(
    host=DB_INFO['HOST'], db=DB_INFO['NAME'], login=DB_INFO['USER'],
    password=DB_INFO['PASSWORD'], port=str(DB_INFO['PORT']), )


def get_local_connection():
    try:
        conn = psycopg2.connect(conn_str)
    except psycopg2.OperationalError:
        conn = None
    return conn


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

    # rows_query = helpers.DataSourceService.get_rows_query_for_loading_task(
    #     source, col_names_select)
    # print rows_query
    #
    # create_table_query = helpers.DataSourceService.table_create_query_for_loading_task(
    #     table_key, ', '.join(col_names_create))
    # print create_table_query
    #
    # insert_table_query = helpers.DataSourceService.table_insert_query_for_loading_task(
    #     table_key)
    # print insert_table_query
    #
    # tasks.load_data.apply_async(
    #     (request.user.id, task_id, source_conn, create_table_query,
    #      rows_query, insert_table_query), )

# from django.db import connection as djc
# @celery.task(name='etl.tasks.load_data')
# def load_data(user_id, task_id, source_conn, create_query, rows_query, insert_query):
#     pass
    # сокет канал
    # chanel = 'jobs:etl:extract:{0}:{1}'.format(user_id, task_id)
    # for i in range(1, 11):
    #     client.publish(chanel, i*10)
    #     time.sleep(3)

    # FIXME смущает правильность работы функции!!!
    # FIXME проверить на разных данных!!!
    # FIXME возможно нужна отдельная обработка каждого типа!!!
    # def processing(rows_):
    #     str_ = ''
    #     for t in rows_:
    #         l = []
    #         for el in t:
    #             # обработка None
    #             if el is None:
    #                 l.append('')
    #                 continue
    #             l.append(str(el))
    #
    #         str_ += '(' + str(l)[1:-1] + '),'
    #     return str_
    # print 'start1'
    # connection = get_local_connection()
    # cursor = connection.cursor()
    # print 'asdfa2'
    # # print djc.introspection.table_names()
    # # create new table
    # cursor.execute(create_query)
    # print 'asdfa3'
    # connection.commit()
    # print 'asdfa'
    # # print djc.introspection.table_names()
    # # достаем первую порцию данных
    # limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
    # offset = 0
    #
    # # курсор пользовательского коннекта
    # print source_conn
    #
    # rows_cursor = source_conn.cursor()
    # print rows_cursor
    # rows_cursor.execute(rows_query.format(limit, offset))
    # rows = rows_cursor.fetchall()
    #
    # while rows:
    #     # формируем данные для инсерта
    #     str_rows = processing(rows)
    #     if str_rows.endswith(','):
    #         str_rows = str_rows[:-1]
    #
    #     # суем данные
    #     cursor.execute(insert_query.format(str_rows))
    #
    #     # достаем новую порцию данных
    #     offset += limit
    #     rows_cursor.execute(rows_query.format(limit, offset))
    #     rows = rows_cursor.fetchall()
    #
    # connection.commit()
    # cursor.close()
    # connection.close()

# write in console: python manage.py celery -A etl.tasks worker
