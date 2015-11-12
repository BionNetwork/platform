# coding: utf-8
from __future__ import unicode_literals

from threading import Thread
import time

from django.contrib.sessions.models import Session
from django.test import TestCase, Client
from django.core.urlresolvers import reverse
from django.db import connection
from django.conf import settings

from core.db.services import retry_query
from core.models import User


class AuthenticationTest(TestCase):

    def setUp(self):
        self.client = Client()

    def test_login(self):
        user = User.objects.get()  # пароль 'test'
        sessions = Session.objects.all()

        self.assertEqual(sessions.count(), 0, 'Количество сессий непусто!')

        self.client.post(
            reverse('core:login'),
            {'username': user.username, 'password': 'test'}
        )
        self.assertEqual(sessions.count(), 1, 'Количество сессий неравно 1!')

    def test_registration(self):

        users = User.objects.all()

        self.assertEqual(users.count(), 1, 'Количество пользователей не 1!')

        self.client.post(
            reverse('core:registration'),
            {'login': 'NewLog', 'email': 'email@mail.ru', 'password': 'newpass'}
        )
        self.assertEqual(users.count(), 2, 'Количество пользователей не 2!')


class DatabaseErrorsCheckTestCase(TestCase):

    # def setUp(self):
    #     User.objects.create(username='test', password='test')
    #     User.objects.create(username='test2', password='test2')
    #
    # TODO: В тестах deadlock не возникает
    # def test_deadlocking(self):
    #
    #     class DeadLockThread(Thread):
    #
    #         def __init__(self, id_1, id_2, timeout):
    #             self.id_1 = id_1
    #             self.id_2 = id_2
    #             self.timeout = timeout
    #             super(DeadLockThread, self).__init__()
    #
    #         def run(self):
    #
    #             with transaction.atomic():
    #                 try:
    #                     print 'start %s' % self.getName(), self.id_1
    #                     User.objects.filter(id=self.id_1).update(
    #                         password='new_password')
    #                     time.sleep(self.timeout)
    #                     print 'between %s' % self.getName()
    #                     User.objects.filter(id=self.id_2).update(
    #                         password='new_password')
    #                     print 'finish %s' % self.getName(), self.id_2
    #                     # transaction.commit()
    #                 finally:
    #                     connection.close()
    #
    #     user_1_id = User.objects.get(username='test').id
    #     user_2_id = User.objects.get(username='test2').id
    #
    #     t = DeadLockThread(user_1_id, user_2_id, 10)
    #     t.start()
    #     time.sleep(7)
    #     t2 = DeadLockThread(user_2_id, user_1_id, 10)
    #     t2.start()
    #     t.join()
    #     t2.join()

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

        timeout = settings.DATABASE_WAIT_TIMEOUT * (
            settings.RETRY_COUNT - 0.5) - THREADS_INTERVAL
        t = LockThread(timeout=timeout)
        t.start()
        time.sleep(THREADS_INTERVAL)
        t2 = DataQuery()
        t2.start()
        t.join()
        t2.join()

