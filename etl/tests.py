# coding: utf-8
from __future__ import unicode_literals

import json

from django.test import TestCase
from etl.services.datasource.repository import r_server
from etl.services.db.postgresql import Postgresql
from etl.services.datasource.base import TablesTree, DataSourceService
from core.models import Datasource, ConnectionChoices

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
        self.maxDiff = None

    def test_generate_join(self):
        structure = {'childs': [{'childs': [], 'joins': [
            {'right': {'column': 'billing_bank_packet_status_id', 'table': 'billing_bank_packet'},
             'join': {'type': 'inner', 'value': 'eq'},
             'left': {'column': 'id', 'table': 'billing_bank_packet_status'}}], 'join_type': 'inner',
                                 'val': 'billing_bank_packet'}], 'joins': [], 'join_type': 'inner',
                     'val': 'billing_bank_packet_status'}
        join_query = self.database.generate_join(structure)
        expected_join_query = ('"billing_bank_packet_status" INNER JOIN "billing_bank_packet" ON '
                               '("billing_bank_packet_status"."id" = "billing_bank_packet"."billing_bank_packet_status_id")')
        self.assertEqual(expected_join_query, join_query)

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
        expected_join_query = ('"billing_bank_packet_status" INNER JOIN "billing_bank_packet" ON '
                               '("billing_bank_packet_status"."id" = "billing_bank_packet"."billing_bank_packet_status_id") '
                               'INNER JOIN "billing_bank_packet_operation" ON '
                               '("billing_bank_packet_operation"."billing_bank_packet_id" = "billing_bank_packet"."id")')
        self.assertEqual(expected_join_query, join_query)

    def test_statistic(self):
        data = [('first', 5, 5), ('second', 0, 0), ]
        result = self.database.processing_statistic(data)
        self.assertEqual(result,
                         {
                             'first': {'count': 5, 'size': 5},
                             'second': None,
                         },
                         "Результат статистики неверен!")


class TablesTreeTest(TestCase):
    """
        Тестирование всех методов TablesTree
    """

    def setUp(self):

        self.tables = ['auth_group_permissions', 'auth_group', 'auth_permission', 'datasources']

        self.tables_info = {
            'auth_permission':
                {
                    'foreigns': [
                        {'on_update': 'NO ACTION', 'source': {'column': 'content_type_id', 'table': 'auth_permission'},
                         'destination': {'column': 'id', 'table': 'django_content_type'},
                         'name': 'auth_content_type_id_508cf46651277a81_fk_django_content_type_id', 'on_delete': 'NO ACTION'}],
                    'columns': [
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'content_type_id', 'is_primary': False},
                        {'is_index': True, 'is_unique': True, 'type': 'text', 'name': 'codename', 'is_primary': False},
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'id', 'is_primary': True},
                        {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'name', 'is_primary': False}],
                    'indexes': [
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_permission_417f1b1c', 'columns': ['content_type_id']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_permission_content_type_id_codename_key',
                         'columns': ['content_type_id', 'codename']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_permission_pkey', 'columns': ['id']}]
                },
            'auth_group':
                {
                    'foreigns': [],
                    'columns': [
                        {'is_index': True, 'is_unique': True, 'type': 'text', 'name': 'name', 'is_primary': False},
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'id', 'is_primary': True}],
                    'indexes': [
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_name_253ae2a6331666e8_like', 'columns': ['name']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_name_key', 'columns': ['name']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_pkey', 'columns': ['id']}]
                },
            'auth_group_permissions':
                {
                    'foreigns': [
                        {'on_update': 'NO ACTION', 'source': {'column': 'group_id', 'table': 'auth_group_permissions'},
                         'destination': {'column': 'id', 'table': 'auth_group'},
                         'name': 'auth_group_permissio_group_id_689710a9a73b7457_fk_auth_group_id', 'on_delete': 'NO ACTION'},
                        {'on_update': 'NO ACTION', 'source': {'column': 'permission_id', 'table': 'auth_group_permissions'},
                         'destination': {'column': 'id', 'table': 'auth_permission'},
                         'name': 'auth_group_permission_id_1f49ccbbdc69d2fc_fk_auth_permission_id', 'on_delete': 'NO ACTION'}],
                    'columns': [
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'id', 'is_primary': True},
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'permission_id', 'is_primary': False},
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'group_id', 'is_primary': False}],
                    'indexes': [
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_permissions_0e939a4f',
                         'columns': ['group_id']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_permissions_8373b171',
                         'columns': ['permission_id']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_permissions_group_id_permission_id_key',
                         'columns': ['group_id', 'permission_id']},
                        {'is_primary': False, 'is_unique': False, 'name': 'auth_group_permissions_pkey', 'columns': ['id']}]
                },
            'datasources':
                {
                    'foreigns': [],
                    'columns': [
                        {'is_index': True, 'is_unique': True, 'type': 'text', 'name': 'db', 'is_primary': False},
                        {'is_index': True, 'is_unique': False, 'type': 'date', 'name': 'create_date', 'is_primary': False},
                        {'is_index': False, 'is_unique': False, 'type': 'int', 'name': 'user_id', 'is_primary': False},
                        {'is_index': False, 'is_unique': False, 'type': 'int', 'name': 'conn_type', 'is_primary': False},
                        {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'login', 'is_primary': False},
                        {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'password', 'is_primary': False},
                        {'is_index': False, 'is_unique': False, 'type': 'int', 'name': 'port', 'is_primary': False},
                        {'is_index': True, 'is_unique': True, 'type': 'text', 'name': 'host', 'is_primary': False},
                        {'is_index': True, 'is_unique': True, 'type': 'int', 'name': 'id', 'is_primary': True}],
                    'indexes': [
                        {'is_primary': False, 'is_unique': False, 'name': 'datasources_3d8252a0', 'columns': ['create_date']},
                        {'is_primary': False, 'is_unique': False, 'name': 'datasources_67b3dba8', 'columns': ['host']},
                        {'is_primary': False, 'is_unique': False, 'name': 'datasources_host_303054fe224cb4d4_like',
                         'columns': ['host']},
                        {'is_primary': False, 'is_unique': False, 'name': 'datasources_host_620e1720a93098d_uniq',
                         'columns': ['host', 'db']},
                        {'is_primary': False, 'is_unique': False, 'name': 'datasources_pkey', 'columns': ['id']}]
                }
        }

    # тест построения дерева
    def test_tree_build(self):

        #       auth_group_permissions
        #         /                \
        #    auth_group      auth_permission
        #                           \!!!(without bind)
        #                       datasources

        self.tree = TablesTree('auth_group_permissions')
        remains = TablesTree.build_tree(
            [self.tree.root, ], self.tables, self.tables_info)

        self.assertEqual(remains, ['datasources'],
                         'Должна быть 1 таблица без связей datasources!')

    # тест проверки количества, порядка обхода,  нодов и  дерева
    def test_tree_nodes_order_and_count(self):

        self.tree = TablesTree('auth_group_permissions')
        TablesTree.build_tree(
            [self.tree.root, ], self.tables, self.tables_info)

        ordered_nodes = TablesTree.get_tree_ordered_nodes([self.tree.root, ])
        self.assertEqual(len(ordered_nodes), 3, 'Количество нодов в дереве не 3!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission'],
                         'Cбит порядок нодов в дереве!')

        counts = TablesTree.get_nodes_count_by_level([self.tree.root, ])
        self.assertEqual(counts, [1, 2], 'Дерево построено неправильно!')

    # тест построения структуры дерева
    def test_tree_structure(self):

        self.tree = TablesTree('auth_group_permissions')
        TablesTree.build_tree(
            [self.tree.root, ], self.tables, self.tables_info)

        structure = TablesTree.get_tree_structure(self.tree.root)

        expected_structure = {
            'childs': [
                {'childs': [],
                 'join_type': 'inner',
                 'joins': [
                     {'right': {'column': 'id', 'table': 'auth_group'},
                      'join': {'type': 'inner', 'value': 'eq'},
                      'left': {'column': 'group_id', 'table': 'auth_group_permissions'}}
                 ],
                 'val': 'auth_group'},
                {'childs': [],
                 'join_type': 'inner',
                 'joins': [
                     {'right': {'column': 'id', 'table': 'auth_permission'},
                      'join': {'type': 'inner', 'value': 'eq'},
                      'left': {'column': 'permission_id', 'table': 'auth_group_permissions'}}],
                 'val': 'auth_permission'}
            ],
            'join_type': None,
            'joins': [],
            'val': 'auth_group_permissions'
        }

        self.assertEqual(structure, expected_structure, 'Структура дерева построена неправильно!')

    # тест на построение нового дерева
    def test_build_new_tree(self):

        self.tree = TablesTree('auth_group_permissions')
        TablesTree.build_tree(
            [self.tree.root, ], self.tables, self.tables_info)

        structure = TablesTree.get_tree_structure(self.tree.root)

        new_tree = TablesTree.build_tree_by_structure(structure)

        ordered_nodes = TablesTree.get_tree_ordered_nodes([new_tree.root, ])
        self.assertEqual(len(ordered_nodes), 3, 'Количество нодов в новом дереве не 3!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission'],
                         'Cбит порядок нодов в новом дереве!')

        new_counts = TablesTree.get_nodes_count_by_level([new_tree.root, ])
        self.assertEqual(new_counts, [1, 2], 'Новое дерево построено неправильно!')

        # связываем таблицу datasources, добавляем новые джойны

        TablesTree.update_node_joins(
            self.tree, 'auth_permission', 'datasources', 'inner', [['codename', 'eq', 'db'], ])

        ordered_nodes = TablesTree.get_tree_ordered_nodes([self.tree.root, ])
        self.assertEqual(len(ordered_nodes), 4, 'Количество нодов в новом дереве не 4!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission', 'datasources'],
                         'Cбит порядок нодов в дереве после добавления datasources!')

        counts = TablesTree.get_nodes_count_by_level([self.tree.root, ])
        self.assertEqual(counts, [1, 2, 1], 'Дерево построено неправильно!')

    # тест на добавление новых джойнов таблиц
    def test_update_joins(self):

        self.tree = TablesTree('auth_group_permissions')
        TablesTree.build_tree(
            [self.tree.root, ], self.tables, self.tables_info)

        TablesTree.update_node_joins(
            self.tree, 'auth_group_permissions', 'auth_group', 'right',
            [['group_id', 'eq', 'id'], ['id', 'lt', 'name']]
        )
        expected_structure = {
            'childs': [
                {'childs': [],
                 'join_type': 'right',
                 'joins': [
                     {'right': {'column': 'id', 'table': 'auth_group'},
                      'join': {'type': 'right', 'value': 'eq'},
                      'left': {'column': 'group_id', 'table': 'auth_group_permissions'}},
                     {'right': {'column': 'name', 'table': 'auth_group'},
                      'join': {'type': 'right', 'value': 'lt'},
                      'left': {'column': 'id', 'table': 'auth_group_permissions'}}
                 ],
                 'val': 'auth_group'},
                {'childs': [],
                    'join_type': 'inner',
                    'joins': [
                        {'right': {'column': 'id', 'table': 'auth_permission'},
                         'join': {'type': 'inner', 'value': 'eq'},
                         'left': {'column': 'permission_id', 'table': 'auth_group_permissions'}}
                    ], 'val': 'auth_permission'}
            ],
            'join_type': None,
            'joins': [],
            'val': 'auth_group_permissions'
        }

        structure = TablesTree.get_tree_structure(self.tree.root)
        self.assertEqual(structure, expected_structure, 'Структура дерева построена неправильно!')


class DatasourceTest(TestCase):

    def test_set_from_dict(self):
        source = Datasource()
        data = {"db": "test", "conn_type": 1, "host": "localhost", "login": "foo", "password": "bar", "port": 5432}
        source.set_from_dict(**data)
        for k, v in data.items():
            self.assertEquals(v, source.__dict__[k], "attribute %s does not valid" % k)


class RedisKeysTest(TestCase):
    def setUp(self):
        connection = {'host': 'localhost', 'port': 5432, 'db': 'test_biplatform',
                      'login': 'biplatform', 'password': 'biplatform'}

        self.source = Datasource.objects.create(
            db='test_biplatform',
            host='localhost',
            port=5432,
            login='biplatform',
            password='biplatform',
            user_id=11,
            conn_type=ConnectionChoices.POSTGRESQL,
        )

        self.database = Postgresql(connection)
        self.cursor = self.database.connection.cursor()
        self.cursor.execute("""
            drop table if exists test_table;

            create table test_table(
              id serial NOT NULL,
              arguments text NOT NULL,
              date_created timestamp with time zone NOT NULL,
              comment character varying(1024),
              queue_id integer NOT NULL,
              CONSTRAINT test_table_pkey PRIMARY KEY (id),
              CONSTRAINT test_table_uniq UNIQUE (queue_id)
            )
            WITH (OIDS=FALSE);

            ALTER TABLE test_table
              OWNER TO biplatform;

            CREATE INDEX test_queue_id_index
              ON test_table
              USING btree
              (queue_id);

            CREATE INDEX test_date_created_index
              ON test_table
              USING btree
              (date_created);

            CREATE UNIQUE INDEX test_uniq_together_index
              ON test_table
              USING btree
              (date_created, queue_id);
        """)
        self.database.connection.commit()

    def test_redis_keys(self):
        tables = ['test_table', ]
        DataSourceService.get_columns_info(self.source, tables)

        keys = ['user_datasources:11:1:active_collections',
                'user_datasources:11:1:counter',
                'user_datasources:11:1:ddl:1',
                'user_datasources:11:1:collection:1',
                ]

        for k in keys:
            self.assertTrue(r_server.exists(k), 'Ключ {0} не создался!'.format(k))

        collections = json.loads(r_server.get('user_datasources:11:1:active_collections'))
        self.assertEqual(collections, [{"name": "test_table", "order": 1}, ],
                         'Активные коллекции сохранены неправильно!')

        self.assertEqual(r_server.get('user_datasources:11:1:counter'), '1',
                         'Счетчик коллекций сохранен неправильно!')

        expected_col1 = {"foreigns": [], "stats": None,
                         "columns": [
                             {"is_index": True, "name": "id", "is_primary": True, "is_unique": True, "type": "integer"},
                             {"is_index": False, "name": "arguments", "is_primary": False, "is_unique": False, "type": "text"},
                             {"is_index": True, "name": "date_created", "is_primary": False, "is_unique": False, "type": "timestamp"},
                             {"is_index": False, "name": "comment", "is_primary": False, "is_unique": False, "type": "text"},
                             {"is_index": True, "name": "queue_id", "is_primary": False, "is_unique": True, "type": "integer"}
                         ],
                         "indexes": [
                             {u'is_primary': False, u'is_unique': False, u'name': u'test_date_created_index', u'columns': [u'date_created']},
                             {u'is_primary': False, u'is_unique': True, u'name': u'test_uniq_together_index', u'columns': [u'date_created', u'queue_id']},
                             {u'is_primary': False, u'is_unique': False, u'name': u'test_queue_id_index', u'columns': [u'queue_id']},
                             {u'is_primary': True, u'is_unique': True, u'name': u'test_table_pkey', u'columns': [u'id']},
                             {u'is_primary': False, u'is_unique': True, u'name': u'test_table_uniq', u'columns': [u'queue_id']}]}

        collection1 = json.loads(r_server.get('user_datasources:11:1:collection:1'))
        self.assertEqual(collection1, expected_col1,
                         'Collection сохранен неправильно!')
        expected_ddl1 = {u'foreigns': [], u'stats': None,
                         u'columns': [{u'is_index': True, u'name': u'id', u'extra': u'serial',
                                       u'is_primary': True, u'is_nullable': u'NO', u'is_unique': True, u'type': u'integer'},
                                      {u'is_index': False, u'name': u'arguments', u'extra': None, u'is_primary': False,
                                       u'is_nullable': u'NO', u'is_unique': False, u'type': u'text'},
                                      {u'is_index': True, u'name': u'date_created', u'extra': None, u'is_primary': False,
                                       u'is_nullable': u'NO', u'is_unique': False, u'type': u'timestamp with time zone'},
                                      {u'is_index': False, u'name': u'comment', u'extra': None, u'is_primary': False,
                                       u'is_nullable': u'YES', u'is_unique': False, u'type': u'character varying'},
                                      {u'is_index': True, u'name': u'queue_id', u'extra': None, u'is_primary': False,
                                       u'is_nullable': u'NO', u'is_unique': True, u'type': u'integer'}],
                         u'indexes': [{u'is_primary': False, u'is_unique': False, u'name': u'test_date_created_index', u'columns': [u'date_created']},
                                      {u'is_primary': False, u'is_unique': True, u'name': u'test_uniq_together_index', u'columns': [u'date_created', u'queue_id']},
                                      {u'is_primary': False, u'is_unique': False, u'name': u'test_queue_id_index', u'columns': [u'queue_id']},
                                      {u'is_primary': True, u'is_unique': True, u'name': u'test_table_pkey', u'columns': [u'id']},
                                      {u'is_primary': False, u'is_unique': True, u'name': u'test_table_uniq', u'columns': [u'queue_id']}]
                         }
        ddl1 = json.loads(r_server.get('user_datasources:11:1:ddl:1'))
        self.assertEqual(ddl1, expected_ddl1, 'DDL сохранен неправильно!')

        self.database.connection.close()

