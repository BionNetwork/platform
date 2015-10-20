# coding: utf-8
from __future__ import unicode_literals

from django.test import TestCase
from etl.helpers import Postgresql

"""
Тестирование etl методов
Работа с базой
Загрузка данных
Проверка метаданных
"""


class DatabaseTest(TestCase):

    def setUp(self):
        connection = {'host': 'localhost', 'port': 5432, 'db': 'test', 'login': 'foo', 'password': 'bar'}
        self.database = Postgresql(connection)

    def test_generate_join(self):
        structure = {'childs': [{'childs': [], 'joins': [
            {'right': {'column': 'billing_bank_packet_status_id', 'table': 'billing_bank_packet'},
             'join': {'type': 'inner', 'value': 'eq'},
             'left': {'column': 'id', 'table': 'billing_bank_packet_status'}}], 'join_type': 'inner',
                                 'val': 'billing_bank_packet'}], 'joins': [], 'join_type': 'inner',
                     'val': 'billing_bank_packet_status'}
        join_query = self.database.generate_join(structure)
        expected_join_query = ("billing_bank_packet_status INNER JOIN billing_bank_packet",
                               "ON billing_bank_packet_status.id = billing_bank_packet.billing_bank_packet_status_id")
        self.assertEqual(' '.join(expected_join_query), join_query)

