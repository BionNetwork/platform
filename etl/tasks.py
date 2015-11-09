# coding: utf-8

import os
import sys
import brukva
import time
import psycopg2
import psycopg2.extensions
from itertools import izip

from django.conf import settings

from djcelery import celery

from helpers import DataSourceService

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

client = brukva.Client(host=settings.REDIS_HOST,
                       port=int(settings.REDIS_PORT),
                       selected_db=settings.REDIS_DB)
client.connect()


@celery.task(name='etl.tasks.load_data')
def load_data(user_id, task_id, source, table_name, create_query,
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
