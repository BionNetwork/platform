# coding: utf-8


import json

from django.db import connections
from django.test import TestCase
from mock import patch

from core.models import Datasource, ConnectionChoices, DatasourceMeta, \
    DatasourceMetaKeys, Measure
from etl.constants import GENERATE_DIMENSIONS, GENERATE_MEASURES
from etl.services.datasource.base import TablesTree, DataSourceService
from etl.services.datasource.db import Postgresql
from etl.services.datasource.repository import r_server
from etl.services.queue.base import TaskService

# from etl.tasks import LoadDimensions, LoadMeasures

"""
Тестирование etl методов
Работа с базой
Загрузка данных
Проверка метаданных
"""


class BaseCoreTest(TestCase):

    fixtures = ['initial_data.json', 'queue_data.json', ]


class DatabaseTest(BaseCoreTest):

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


class TablesTreeTest(BaseCoreTest):
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
        remains = self.tree.build(self.tables, self.tables_info)

        self.assertEqual(remains, ['datasources'],
                         'Должна быть 1 таблица без связей datasources!')

    # тест проверки количества, порядка обхода,  нодов и  дерева
    def test_tree_nodes_order_and_count(self):

        self.tree = TablesTree('auth_group_permissions')
        self.tree.build(self.tables, self.tables_info)

        ordered_nodes = self.tree.ordered_nodes
        self.assertEqual(len(ordered_nodes), 3, 'Количество нодов в дереве не 3!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission'],
                         'Cбит порядок нодов в дереве!')

        counts = self.tree.nodes_count_for_levels
        self.assertEqual(counts, [1, 2], 'Дерево построено неправильно!')

    # тест построения структуры дерева
    def test_tree_structure(self):

        self.tree = TablesTree('auth_group_permissions')
        self.tree.build(self.tables, self.tables_info)

        structure = self.tree.structure

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
        self.tree.build(self.tables, self.tables_info)

        structure = self.tree.structure

        new_tree = TableTreeRepository.build_tree_by_structure(structure)

        ordered_nodes = new_tree.ordered_nodes
        self.assertEqual(len(ordered_nodes), 3, 'Количество нодов в новом дереве не 3!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission'],
                         'Cбит порядок нодов в новом дереве!')

        new_counts = new_tree.nodes_count_for_levels
        self.assertEqual(new_counts, [1, 2], 'Новое дерево построено неправильно!')

        # связываем таблицу datasources, добавляем новые джойны

        self.tree.update_node_joins(
            'auth_permission', 'datasources', 'inner', [['codename', 'eq', 'db'], ])

        ordered_nodes = self.tree.ordered_nodes
        self.assertEqual(len(ordered_nodes), 4, 'Количество нодов в новом дереве не 4!')

        ordered_nodes_vals = [x.val for x in ordered_nodes]
        self.assertEqual(ordered_nodes_vals,
                         ['auth_group_permissions', 'auth_group', 'auth_permission', 'datasources'],
                         'Cбит порядок нодов в дереве после добавления datasources!')

        counts = TablesTree.nodes_count_for_levels
        self.assertEqual(counts, [1, 2, 1], 'Дерево построено неправильно!')

    # тест на добавление новых джойнов таблиц
    def test_update_joins(self):

        self.tree = TablesTree('auth_group_permissions')
        self.tree.build(self.tables, self.tables_info)

        self.tree.update_node_joins(
            'auth_group_permissions', 'auth_group', 'right',
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

        structure = self.tree.structure
        self.assertEqual(structure, expected_structure, 'Структура дерева построена неправильно!')


class DatasourceTest(BaseCoreTest):

    def setUp(self):
        self.source = Datasource()
        self.data = {"db": "test", "conn_type": 1, "host": "localhost", "login": "foo", "password": "bar", "port": 5432}

    def test_set_from_dict(self):
        self.source.set_from_dict(**self.data)
        for k, v in list(self.data.items()):
            self.assertEquals(v, self.source.__dict__[k], "attribute %s does not valid" % k)

    def test_mock_get_columns_info(self):
        with patch('etl.services.db.factory.DatabaseService.get_columns_info') as cols_mock:
            cols_mock.return_value = [
                (('VERSION_BUNDLE', 'NODE_ID', 'varbinary(16)', 'NO', None),
                 ('VERSION_BUNDLE', 'BUNDLE_DATA', 'longblob', 'NO', None),
                 ('VERSION_BUNDLE', 's', 'varchar(60)', 'YES', None),
                 ),
                (('VERSION_BUNDLE', 'NODE_ID', 'VERSION_BUNDLE_IDX', 'f', 't'),),
                (('VERSION_BUNDLE', 'NODE_ID', 'VERSION_BUNDLE_IDX', 'UNIQUE', None, None, None, None),),
            ]
            with patch('etl.services.db.factory.DatabaseService.get_stats_info') as stats_mock:
                stats_mock.return_value = {'version_bundle': {'count': 3, 'size': 16384}}

                info = DataSourceService.get_columns_info(self.source, ['VERSION_BUNDLE', ])
                expected_info = [
                    {'dest': None, 'without_bind': False, 'db': '',
                     'cols': [
                         {'col_title': None, 'col_name': 'NODE_ID'},
                         {'col_title': None, 'col_name': 'BUNDLE_DATA'},
                         {'col_title': None, 'col_name': 's'}
                     ],
                     'is_root': True, 'host': '', 'tname': 'VERSION_BUNDLE'}
                ]

                self.assertEqual(info, expected_info)


class RedisKeysTest(BaseCoreTest):
    def setUp(self):

        r_server.flushdb()

        db_conn = connections['default']
        conn_params = db_conn.get_connection_params()

        connection = {
            'host': conn_params.get('host'),
            'port': int(conn_params.get('port')),
            'db': conn_params.get('database'),
            'login': conn_params.get('user'),
            'password': conn_params.get('password')
        }

        self.source = Datasource.objects.create(
            user_id=11,
            conn_type=ConnectionChoices.POSTGRESQL,
            **connection
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

        source_id = self.source.id

        counter_str = 'user_datasources:11:{0}:counter'.format(source_id)
        ddl_str = 'user_datasources:11:{0}:ddl:1'.format(source_id)
        collection_str = 'user_datasources:11:{0}:collection:1'.format(source_id)

        keys = [counter_str, ddl_str, collection_str, ]

        for k in keys:
            self.assertTrue(r_server.exists(k), 'Ключ {0} не создался!'.format(k))

        self.assertEqual(json.loads(r_server.get(counter_str)),
                         {"next_id": 2,
                          "data": [{"name": "test_table", "id": 1}]},
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
                             {'is_primary': False, 'is_unique': False, 'name': 'test_date_created_index', 'columns': ['date_created']},
                             {'is_primary': False, 'is_unique': True, 'name': 'test_uniq_together_index', 'columns': ['date_created', 'queue_id']},
                             {'is_primary': False, 'is_unique': False, 'name': 'test_queue_id_index', 'columns': ['queue_id']},
                             {'is_primary': True, 'is_unique': True, 'name': 'test_table_pkey', 'columns': ['id']},
                             {'is_primary': False, 'is_unique': True, 'name': 'test_table_uniq', 'columns': ['queue_id']}],
                         "date_intervals": [{'startDate': None, 'last_updated': None, 'name': 'date_created', 'endDate': None}],
                         }

        collection1 = json.loads(r_server.get(collection_str))

        for interval in collection1['date_intervals']:
            interval['last_updated'] = None

        self.assertEqual(collection1, expected_col1,
                         'Collection сохранен неправильно!')
        expected_ddl1 = {'foreigns': [], 'stats': None,
                         'columns': [{'is_index': True, 'name': 'id', 'extra': 'serial',
                                       'is_primary': True, 'is_nullable': 'NO', 'is_unique': True, 'type': 'integer'},
                                      {'is_index': False, 'name': 'arguments', 'extra': None, 'is_primary': False,
                                       'is_nullable': 'NO', 'is_unique': False, 'type': 'text'},
                                      {'is_index': True, 'name': 'date_created', 'extra': None, 'is_primary': False,
                                       'is_nullable': 'NO', 'is_unique': False, 'type': 'timestamp with time zone'},
                                      {'is_index': False, 'name': 'comment', 'extra': None, 'is_primary': False,
                                       'is_nullable': 'YES', 'is_unique': False, 'type': 'character varying'},
                                      {'is_index': True, 'name': 'queue_id', 'extra': None, 'is_primary': False,
                                       'is_nullable': 'NO', 'is_unique': True, 'type': 'integer'}],
                         'indexes': [{'is_primary': False, 'is_unique': False, 'name': 'test_date_created_index', 'columns': ['date_created']},
                                      {'is_primary': False, 'is_unique': True, 'name': 'test_uniq_together_index', 'columns': ['date_created', 'queue_id']},
                                      {'is_primary': False, 'is_unique': False, 'name': 'test_queue_id_index', 'columns': ['queue_id']},
                                      {'is_primary': True, 'is_unique': True, 'name': 'test_table_pkey', 'columns': ['id']},
                                      {'is_primary': False, 'is_unique': True, 'name': 'test_table_uniq', 'columns': ['queue_id']}],
                         'date_intervals': [{'startDate': None, 'last_updated': None, 'name': 'date_created', 'endDate': None}]
                         }
        ddl1 = json.loads(r_server.get(ddl_str))

        for interval in ddl1['date_intervals']:
            interval['last_updated'] = None

        self.assertEqual(ddl1, expected_ddl1, 'DDL сохранен неправильно!')

        self.database.connection.close()


class DimCreateTest(BaseCoreTest):

    def setUp(self):

        db_conn = connections['default']
        conn_params = db_conn.get_connection_params()

        connection_params = {
            'host': conn_params.get('host'),
            'port': int(conn_params.get('port')),
            'db': conn_params.get('database'),
            'login': conn_params.get('user'),
            'password': conn_params.get('password')
        }

        self.source = Datasource.objects.create(
            user_id=11,
            conn_type=ConnectionChoices.POSTGRESQL,
            **connection_params
        )

        self.fields_info = {
            'columns': [{
                'is_index': True,
                'is_unique': True,
                'type': 'integer',
                'name': 'id',
                'is_primary': True
            }, {
                'is_index': False,
                'is_unique': False,
                'type': 'text',
                'name': 'arguments',
                'is_primary': False
            }, {
                'is_index': False,
                'is_unique': False,
                'type': 'text',
                'name': 'comment',
                'is_primary': False
            }, {
                'is_index': False,
                'is_unique': False,
                'type': 'integer',
                'name': 'queue_id',
                'is_primary': False
            },
            ]
        }

        source_meta = DatasourceMeta.objects.create(
            datasource=self.source,
            collection_name='table1',
            fields=json.dumps(self.fields_info),
            stats=json.dumps({'date_intervals': [], }),
        )

        self.meta_data = DatasourceMetaKeys.objects.create(
            meta=source_meta,
            value=123456789,
        )

        self.db = Postgresql(connection_params)
        self.cursor = self.db.connection.cursor()
        self.cursor.execute("""
            drop table if exists sttm_datasource_123456789;

            create table sttm_datasource_123456789(
              table1__id serial NOT NULL,
              table1__arguments text NOT NULL,
              table1__comment character varying(1024),
              table1__queue_id integer NOT NULL
            )
            """)

        self.db.connection.commit()

    def test_dim_create(self):
        arguments = {
            'is_meta_stats': True,
            'checksum': 123456789,
            'user_id': 11,
            'source_id': self.source.id,
            'db_update': False,
            'meta_info': json.dumps({'table1': self.fields_info}),
        }

        task_id, channel = TaskService(GENERATE_DIMENSIONS).add_task(
            arguments=arguments)
        LoadDimensions(task_id, channel, last_task=True).load_data()


        self.cursor.execute("""
          select column_name, data_type from information_schema.columns where table_name='dimensions_123456789';
        """)

        dim_info = self.cursor.fetchall()

        task_id, channel = TaskService(GENERATE_MEASURES).add_task(
            arguments=arguments)
        LoadMeasures(task_id, channel, last_task=True).load_data()

        self.cursor.execute("""
          select column_name, data_type from information_schema.columns where table_name='measures_123456789';
        """)

        measures_info = self.cursor.fetchall()

        # прибавляем 2 колонки 'cdc_key'
        self.assertEqual(len(dim_info) + len(measures_info), 4+2)

        cut_dim_info = [(x, y) for (x, y) in dim_info if x != 'cdc_key']

        for el in cut_dim_info:
            self.assertTrue(el[1] in ['text'])

        cut_meas_info = [(x, y) for (x, y) in measures_info if x != 'cdc_key']

        for el in cut_meas_info:
            self.assertTrue(el[1] in [
                Measure.INTEGER, Measure.TIME, Measure.DATE, Measure.TIMESTAMP])

        self.db.connection.commit()

        self.db.connection.close()


class DatasourceMetaTest(BaseCoreTest):
    def setUp(self):

        db_conn = connections['default']
        conn_params = db_conn.get_connection_params()

        connection_params = {
            'host': conn_params.get('host'),
            'port': int(conn_params.get('port')),
            'db': conn_params.get('database'),
            'login': conn_params.get('user'),
            'password': conn_params.get('password')
        }

        self.key = 123456789

        self.source = Datasource.objects.create(
            user_id=11,
            conn_type=ConnectionChoices.POSTGRESQL,
            **connection_params
        )

        # self.tables = ['datasources', u'datasources_meta']

        self.cols = [{'table': 'datasources', 'col': 'db'},
                {'table': 'datasources', 'col': 'host'},
                {'table': 'datasources', 'col': 'user_id'},
                {'table': 'datasources', 'col': 'id'},
                {'table': 'datasources_meta', 'col': 'datasource_id'},
                {'table': 'datasources_meta', 'col': 'id'}
                ]

        self.meta_info = {
            'datasources': {
                'foreigns': [],
                'date_intervals': [],
                'stats': {
                    'count': 2,
                    'size': 8192
                },
                'columns': [
                    {'is_index': True, 'is_unique': False, 'type': 'text', 'name': 'db', 'is_primary': False},
                    {'is_index': True, 'is_unique': False, 'type': 'text', 'name': 'host', 'is_primary': False},
                    {'is_index': False, 'is_unique': False, 'type': 'integer', 'name': 'port', 'is_primary': False},
                    {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'login', 'is_primary': False},
                    {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'password', 'is_primary': False},
                    {'is_index': True, 'is_unique': False, 'type': 'timestamp', 'name': 'create_date', 'is_primary': False},
                    {'is_index': True, 'is_unique': False, 'type': 'integer', 'name': 'user_id', 'is_primary': False},
                    {'is_index': False, 'is_unique': False, 'type': 'integer', 'name': 'conn_type', 'is_primary': False},
                    {'is_index': True, 'is_unique': True, 'type': 'integer', 'name': 'id', 'is_primary': True}],
                'indexes': [
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_3d8252a0', 'columns': ['create_date']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_67b3dba8', 'columns': ['host']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_host_175fd78a6f0fe936_uniq', 'columns': ['db', 'host', 'user_id']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_host_303054fe224cb4d4_like', 'columns': ['host']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_pkey', 'columns': ['id']}]},
            'datasources_meta': {
                'foreigns': [
                    {'on_update': 'NO ACTION',
                     'source': {'column': 'datasource_id', 'table': 'datasources_meta'},
                     'destination': {'column': 'id', 'table': 'datasources'},
                     'name': 'datasources_me_datasource_id_2bccd05d1d1955f1_fk_datasources_id', 'on_delete': 'NO ACTION'}
                ],
                'date_intervals': [],
                'stats': {'count': 10, 'size': 24576},
                'columns': [
                    {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'collection_name', 'is_primary': False},
                    {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'fields', 'is_primary': False},
                    {'is_index': False, 'is_unique': False, 'type': 'text', 'name': 'stats', 'is_primary': False},
                    {'is_index': True, 'is_unique': False, 'type': 'timestamp', 'name': 'create_date', 'is_primary': False},
                    {'is_index': True, 'is_unique': False, 'type': 'timestamp', 'name': 'update_date', 'is_primary': False},
                    {'is_index': True, 'is_unique': False, 'type': 'integer', 'name': 'datasource_id', 'is_primary': False},
                    {'is_index': True, 'is_unique': True, 'type': 'integer', 'name': 'id', 'is_primary': True}],
                'indexes': [
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_meta_3d8252a0', 'columns': ['create_date']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_meta_3ec3fa10', 'columns': ['datasource_id']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_meta_41747ca0', 'columns': ['update_date']},
                    {'is_primary': False, 'is_unique': False, 'name': 'datasources_meta_pkey', 'columns': ['id']}]}}

        self.last_row = ('biplatform', 'localhost', 11, 4,  4,  37)

    def test_source_meta(self):

        dataset_id = 1
        DataSourceService.update_datasource_meta(
            self.key, self.source, self.cols, self.meta_info, self.last_row, dataset_id)

        dm = DatasourceMeta.objects.filter(datasource=self.source)

        ds = dm.get(collection_name='datasources')
        dsm = dm.get(collection_name='datasources_meta')

        ds_stats = {'row_key_value': [{'id': 4}],
                    'row_key': ['id'],
                    'tables_stat': {'count': 2, 'size': 8192},
                    'date_intervals': []}
        ds_fields = {'columns':
                         [
                             {'is_index': True, 'is_unique': False, 'type': 'text', 'name': 'db', 'is_primary': False},
                             {'is_index': True, 'is_unique': False, 'type': 'text', 'name': 'host', 'is_primary': False},
                             {'is_index': True, 'is_unique': False, 'type': 'integer', 'name': 'user_id', 'is_primary': False},
                             {'is_index': True, 'is_unique': True, 'type': 'integer', 'name': 'id', 'is_primary': True}
                         ]
        }

        self.assertEqual(json.loads(ds.stats), ds_stats)
        self.assertEqual(json.loads(ds.fields), ds_fields)

        dsm_stats = {'row_key_value': [{'id': 37}],
                     'row_key': ['id'],
                     'tables_stat': {'count': 10, 'size': 24576},
                     'date_intervals': []}
        dsm_fields = {'columns':
                          [
                              {'is_index': True, 'is_unique': False, 'type': 'integer', 'name': 'datasource_id', 'is_primary': False},
                              {'is_index': True, 'is_unique': True, 'type': 'integer', 'name': 'id', 'is_primary': True}
                          ]
        }
        self.assertEqual(json.loads(dsm.stats), dsm_stats)
        self.assertEqual(json.loads(dsm.fields), dsm_fields)
