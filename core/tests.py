# coding: utf-8


from decimal import Decimal
from threading import Thread
import time

from django.contrib.sessions.models import Session
from django.test import TestCase, Client, TransactionTestCase
from django.core.urlresolvers import reverse
from django.db import connection
from django.conf import settings

from core.db.services import retry_query
from core.models import User
from core.helpers import convert_milliseconds_to_seconds
from etl.helpers import HashEncoder


class BaseCoreTest(TestCase):

    fixtures = ['initial_data.json', ]

    def test_locking(self):
        """
        Тест блокировки таблицы.
        Время блокировки 25 сек.
        Время ожидания(DATABASE_WAIT_TIMEOUT) 10 сек
        Количество попыток(RETRY_COUNT) 3
        """
        THREADS_INTERVAL = 2  # сек

        class DataQuery(Thread):

            @retry_query('default')
            def run(self):
                """Запрос к залоченной таблице"""
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        'BEGIN WORK; SELECT * FROM users FOR UPDATE NOWAIT;')
                    cursor.execute('COMMIT WORK;')
                finally:
                    cursor.execute('COMMIT WORK;')

        class LockThread(Thread):

            def __init__(self, timeout=0):
                self.timeout = timeout
                super(LockThread, self).__init__()

            def run(self):
                """Блокировка таблицы"""
                cursor = connection.cursor()
                # print 'lock table start'
                try:
                    cursor.execute(
                          'BEGIN WORK; SELECT * FROM users FOR UPDATE NOWAIT;')

                    time.sleep(self.timeout)
                    cursor.execute('COMMIT WORK;')
                finally:
                    cursor.execute('COMMIT WORK;')
                # print 'lock table finish'

        timeout = convert_milliseconds_to_seconds(
            settings.DATABASE_WAIT_TIMEOUT) * (
            settings.RETRY_COUNT - 0.5) - THREADS_INTERVAL
        t = LockThread(timeout=timeout)
        t.start()
        time.sleep(THREADS_INTERVAL)
        t2 = DataQuery()
        t2.start()
        t.join()
        t2.join()


class HashTest(BaseCoreTest):
    """
    Тесты на хэш функции
    """

    def test_hash(self):
        row_data = [
            924, 904, 399, 0, 6, Decimal('8.1000'), Decimal('2.7540'),
            Decimal('3.0000'), 28320,
        ]

        row_data2 = [
            970, 945, 6632, 621, 8, Decimal('12.4400'), Decimal('5.7224'),
            Decimal('4.0000'), 47845,
        ]

        def proccess_data(data):
            # обработка данных
            return reduce(lambda res, x: '%s%s' % (res, x), data).encode("utf8")

        hash1 = HashEncoder.encode(proccess_data(row_data))
        hash2 = HashEncoder.encode(proccess_data(row_data2))

        self.assertNotEqual(hash1, hash2, "Коллизия! Хэш разных данных совпал!")
