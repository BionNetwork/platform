# coding: utf-8

import os
import sys
import brukva
import datetime
import time
import psycopg2

from django.conf import settings

from djcelery import celery

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
