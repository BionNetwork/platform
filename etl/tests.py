# coding: utf-8
from __future__ import unicode_literals

from django.test import TestCase
from etl.helpers import Postgresql, TablesTree

"""
Тестирование etl методов
Работа с базой
Загрузка данных
Проверка метаданных
"""


class DatabaseTest(TestCase):

    def setUp(self):
        connection = {'host': 'localhost', 'port': 5432, 'db': 'test', 'user': 'foo', 'passwd': 'bar'}
        # connection = {'host': 'localhost', 'port': 5432, 'db': 'biplatform',
        #               'user': 'biplatform', 'passwd': 'biplatform'}
        self.database = Postgresql(connection)
        self.maxDiff = None

    def test_check_connection(self):
        self.assertNotEqual(self.database.connection, None, u"Подключение к СУБД не удалось!")

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

    def test_generate_join_multiple_tables(self):
        structure = {
            "childs": [
                {
                    "childs": [
                        {
                            "childs": [
                            ],
                            "join_type": "inner",
                            "joins": [
                                {
                                    "join": {
                                        "type": "inner",
                                        "value": "eq"
                                    },
                                    "left": {
                                        "column": "billing_bank_packet_id",
                                        "table": "billing_bank_packet_operation"
                                    },
                                    "right": {
                                        "column": "id",
                                        "table": "billing_bank_packet"
                                    }
                                }
                            ],
                            "val": "billing_bank_packet_operation"
                        }
                    ],
                    "join_type": "inner",
                    "joins": [
                        {
                            "join": {
                                "type": "inner",
                                "value": "eq"
                            },
                            "left": {
                                "column": "id",
                                "table": "billing_bank_packet_status"
                            },
                            "right": {
                                "column": "billing_bank_packet_status_id",
                                "table": "billing_bank_packet"
                            }
                        }
                    ],
                    "val": "billing_bank_packet"
                }
            ],
            "join_type": "inner",
            "joins": [
            ],
            "val": "billing_bank_packet_status"
        }
        join_query = self.database.generate_join(structure)
        expected_join_query = ("billing_bank_packet_status INNER JOIN billing_bank_packet",
                               "ON billing_bank_packet_status.id = billing_bank_packet.billing_bank_packet_status_id",
                               "INNER JOIN billing_bank_packet_operation ON",
                               "billing_bank_packet_operation.billing_bank_packet_id = billing_bank_packet.id")
        self.assertEqual(' '.join(expected_join_query), join_query)


class TablesTreeTest(TestCase):
    def setUp(self):

        self.tables = ['auth_group_permissions', 'auth_group', 'auth_permission', 'datasources']

        self.tables_info = {
            u'auth_permission':
                {
                    u'foreigns': [
                        {u'on_update': u'NO ACTION', u'source': {u'column': u'content_type_id', u'table': u'auth_permission'},
                         u'destination': {u'column': u'id', u'table': u'django_content_type'},
                         u'name': u'auth_content_type_id_508cf46651277a81_fk_django_content_type_id', u'on_delete': u'NO ACTION'}],
                    u'columns': [
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'content_type_id', u'is_primary': False},
                        {u'is_index': True, u'is_unique': True, u'type': u'text', u'name': u'codename', u'is_primary': False},
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'id', u'is_primary': True},
                        {u'is_index': False, u'is_unique': False, u'type': u'text', u'name': u'name', u'is_primary': False}],
                    u'indexes': [
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_permission_417f1b1c', u'columns': [u'content_type_id']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_permission_content_type_id_codename_key',
                         u'columns': [u'content_type_id', u'codename']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_permission_pkey', u'columns': [u'id']}]
                },
            u'auth_group':
                {
                    u'foreigns': [],
                    u'columns': [
                        {u'is_index': True, u'is_unique': True, u'type': u'text', u'name': u'name', u'is_primary': False},
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'id', u'is_primary': True}],
                    u'indexes': [
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_name_253ae2a6331666e8_like', u'columns': [u'name']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_name_key', u'columns': [u'name']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_pkey', u'columns': [u'id']}]
                },
            u'auth_group_permissions':
                {
                    u'foreigns': [
                        {u'on_update': u'NO ACTION', u'source': {u'column': u'group_id', u'table': u'auth_group_permissions'},
                         u'destination': {u'column': u'id', u'table': u'auth_group'},
                         u'name': u'auth_group_permissio_group_id_689710a9a73b7457_fk_auth_group_id', u'on_delete': u'NO ACTION'},
                        {u'on_update': u'NO ACTION', u'source': {u'column': u'permission_id', u'table': u'auth_group_permissions'},
                         u'destination': {u'column': u'id', u'table': u'auth_permission'},
                         u'name': u'auth_group_permission_id_1f49ccbbdc69d2fc_fk_auth_permission_id', u'on_delete': u'NO ACTION'}],
                    u'columns': [
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'id', u'is_primary': True},
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'permission_id', u'is_primary': False},
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'group_id', u'is_primary': False}],
                    u'indexes': [
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_permissions_0e939a4f',
                         u'columns': [u'group_id']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_permissions_8373b171',
                         u'columns': [u'permission_id']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_permissions_group_id_permission_id_key',
                         u'columns': [u'group_id', u'permission_id']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'auth_group_permissions_pkey', u'columns': [u'id']}]
                },
            u'datasources':
                {
                    u'foreigns': [],
                    u'columns': [
                        {u'is_index': True, u'is_unique': True, u'type': u'text', u'name': u'db', u'is_primary': False},
                        {u'is_index': True, u'is_unique': False, u'type': u'date', u'name': u'create_date', u'is_primary': False},
                        {u'is_index': False, u'is_unique': False, u'type': u'int', u'name': u'user_id', u'is_primary': False},
                        {u'is_index': False, u'is_unique': False, u'type': u'int', u'name': u'conn_type', u'is_primary': False},
                        {u'is_index': False, u'is_unique': False, u'type': u'text', u'name': u'login', u'is_primary': False},
                        {u'is_index': False, u'is_unique': False, u'type': u'text', u'name': u'password', u'is_primary': False},
                        {u'is_index': False, u'is_unique': False, u'type': u'int', u'name': u'port', u'is_primary': False},
                        {u'is_index': True, u'is_unique': True, u'type': u'text', u'name': u'host', u'is_primary': False},
                        {u'is_index': True, u'is_unique': True, u'type': u'int', u'name': u'id', u'is_primary': True}],
                    u'indexes': [
                        {u'is_primary': False, u'is_unique': False, u'name': u'datasources_3d8252a0', u'columns': [u'create_date']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'datasources_67b3dba8', u'columns': [u'host']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'datasources_host_303054fe224cb4d4_like',
                         u'columns': [u'host']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'datasources_host_620e1720a93098d_uniq',
                         u'columns': [u'host', u'db']},
                        {u'is_primary': False, u'is_unique': False, u'name': u'datasources_pkey', u'columns': [u'id']}]
                }
        }

    def test_tree_functionality(self):
        self.tree = TablesTree('auth_group_permissions')  # root

        #       auth_group_permissions
        #         /                \
        #    auth_group      auth_permission
        #                           \!!!(without bind)
        #                       datasources

        # строим дерево
        remains = TablesTree.build_tree(
            [self.tree.root, ], self.tables, self.tables_info)

        self.assertEqual(remains, ['datasources'],
                         u'Должна быть 1 таблица без связей datasources!')

        ordered_nodes = TablesTree.get_tree_ordered_nodes([self.tree.root, ])
        self.assertEqual(len(ordered_nodes), 3, u'Количество нодов в дереве не 3!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission'],
                         u'Cбит порядок нодов в дереве!')

        counts = TablesTree.get_nodes_count_by_level([self.tree.root, ])
        self.assertEqual(counts, [1, 2], u'Дерево построено неправильно!')

        structure = TablesTree.get_tree_structure(self.tree.root)

        expected_structure = {
            u'childs': [
                {u'childs': [],
                 u'join_type': u'inner',
                 u'joins': [
                     {u'right': {u'column': u'id', u'table': u'auth_group'},
                      u'join': {u'type': u'inner', u'value': u'eq'},
                      u'left': {u'column': u'group_id', u'table': u'auth_group_permissions'}}
                 ],
                 u'val': u'auth_group'},
                {u'childs': [],
                 u'join_type': u'inner',
                 u'joins': [
                     {u'right': {u'column': u'id', u'table': u'auth_permission'},
                      u'join': {u'type': u'inner', u'value': u'eq'},
                      u'left': {u'column': u'permission_id', u'table': u'auth_group_permissions'}}],
                 u'val': u'auth_permission'}
            ],
            u'join_type': u'inner',
            u'joins': [],
            u'val': u'auth_group_permissions'
        }

        self.assertEqual(structure, expected_structure, u'Структура дерева построена неправильно!')

        # строим новое дерево
        new_tree = TablesTree.build_tree_by_structure(structure)

        ordered_nodes = TablesTree.get_tree_ordered_nodes([new_tree.root, ])
        self.assertEqual(len(ordered_nodes), 3, u'Количество нодов в новом дереве не 3!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission'],
                         u'Cбит порядок нодов в новом дереве!')

        new_counts = TablesTree.get_nodes_count_by_level([new_tree.root, ])
        self.assertEqual(new_counts, [1, 2], u'Новое дерево построено неправильно!')

        # связываем таблицу datasources, добавляем новые джойны

        TablesTree.update_node_joins(
            self.tree, 'auth_permission', 'datasources', 'inner', [[u'codename', u'eq', u'db'], ])

        ordered_nodes = TablesTree.get_tree_ordered_nodes([self.tree.root, ])
        self.assertEqual(len(ordered_nodes), 4, u'Количество нодов в новом дереве не 4!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission', u'datasources'],
                         u'Cбит порядок нодов в дереве после добавления datasources!')

        counts = TablesTree.get_nodes_count_by_level([self.tree.root, ])
        self.assertEqual(counts, [1, 2, 1], u'Дерево построено неправильно!')

        TablesTree.update_node_joins(
            self.tree, 'auth_group_permissions', 'auth_group', 'right',
            [[u'group_id', u'eq', u'id'], [u'id', u'lt', u'name']]
        )
        expected_structure = {
            u'childs': [
                {u'childs': [],
                 u'join_type': u'inner',
                 u'joins': [
                     {u'right': {u'column': u'id', u'table': u'auth_group'},
                      u'join': {u'type': u'right', u'value': u'eq'},
                      u'left': {u'column': u'group_id', u'table': u'auth_group_permissions'}},
                     {u'right': {u'column': u'name', u'table': u'auth_group'},
                      u'join': {u'type': u'right', u'value': u'lt'},
                      u'left': {u'column': u'id', u'table': u'auth_group_permissions'}}
                 ],
                 u'val': u'auth_group'},
                {u'childs': [
                    {u'childs': [],
                     u'join_type': u'inner',
                     u'joins': [
                         {u'right': {u'column': u'db', u'table': u'datasources'},
                          u'join': {u'type': u'inner', u'value': u'eq'},
                          u'left': {u'column': u'codename', u'table': u'auth_permission'}}
                     ],
                     u'val': u'datasources'}
                ],
                    u'join_type': u'inner',
                    u'joins': [
                        {u'right': {u'column': u'id', u'table': u'auth_permission'},
                         u'join': {u'type': u'inner', u'value': u'eq'},
                         u'left': {u'column': u'permission_id', u'table': u'auth_group_permissions'}}
                    ], u'val': u'auth_permission'}
            ],
            u'join_type': u'inner',
            u'joins': [],
            u'val': u'auth_group_permissions'
        }

        structure = TablesTree.get_tree_structure(self.tree.root)
        self.assertEqual(structure, expected_structure, u'Структура дерева построена неправильно!')
