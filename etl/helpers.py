# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb
import json
import operator
from itertools import groupby, izip_longest, izip
from collections import defaultdict

from django.conf import settings

from core.models import ConnectionChoices
from . import r_server
from .maps import postgresql as psql_map


def get_utf8_string(value):
    """
    Кодирование в utf-8 строки
    :param value: string
    :return: string
    """
    return unicode(value)


class Database(object):
    """
    Базовыми возможности для работы с базами данных
    Получение информации о таблице, список колонок, проверка соединения и т.д.
    """
    @staticmethod
    def get_db_info(user_id, source):

        if settings.USE_REDIS_CACHE:
            user_db_key = RedisCacheKeys.get_user_databases(user_id)
            user_datasource_key = RedisCacheKeys.get_user_datasource(user_id, source.id)

            if not r_server.exists(user_datasource_key):
                conn_info = source.get_connection_dict()
                conn = DataSourceService.get_connection(conn_info)
                tables = DataSourceService.get_tables(source, conn)

                new_db = {
                    "db": source.db,
                    "host": source.host,
                    "tables": tables
                }
                if str(source.id) not in r_server.lrange(user_db_key, 0, -1):
                    r_server.rpush(user_db_key, source.id)
                r_server.set(user_datasource_key, json.dumps(new_db))
                r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)

            return json.loads(r_server.get(user_datasource_key))

        else:
            conn_info = source.get_connection_dict()
            conn = DataSourceService.get_connection(conn_info)
            tables = DataSourceService.get_tables(source, conn)

            return {
                "db": source.db,
                "host": source.host,
                "tables": tables
            }

    @staticmethod
    def check_connection(post):
        """
        Проверка соединения источников данных
        :param data: dict
        :return: connection obj or None
        """
        conn_info = {
            'host': get_utf8_string(post.get('host')),
            'user': get_utf8_string(post.get('login')),
            'passwd': get_utf8_string(post.get('password')),
            'db': get_utf8_string(post.get('db')),
            'port': get_utf8_string(post.get('port')),
            'conn_type': get_utf8_string(post.get('conn_type')),
        }

        return DataSourceService.get_connection(conn_info)

    @staticmethod
    def get_columns_info(source, user, tables):
        conn_info = source.get_connection_dict()
        conn = DataSourceService.get_connection(conn_info)

        return DataSourceService.get_columns(source, user, tables, conn)

    @staticmethod
    def get_rows_info(source, tables, cols):
        conn_info = source.get_connection_dict()
        conn = DataSourceService.get_connection(conn_info)

        return DataSourceService.get_rows(source, conn, tables, cols)

    @staticmethod
    def get_query_result(query, conn):
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def _get_columns_query(source, tables):
        raise ValueError("Columns query is not realized")

    @classmethod
    def get_columns(cls, source, user, tables, conn):

        query = cls._get_columns_query(source, tables)

        records = Database.get_query_result(query, conn)

        result = []
        for key, group in groupby(records, lambda x: x[0]):
            result.append({
                "tname": key, 'db': source.db, 'host': source.host,
                "cols": [x[1] for x in group]
            })
        return result

    @staticmethod
    def get_rows(conn, tables, cols):
        query = """
            SELECT {0} FROM {1} LIMIT {2};
        """.format(', '.join(cols), ', '.join(tables), settings.ETL_COLLECTION_PREVIEW_LIMIT)
        records = Database.get_query_result(query, conn)
        return records


class Postgresql(Database):
    """Управление источником данных Postgres"""
    @staticmethod
    def get_connection(conn_info):
        try:
            conn_str = (u"host='{host}' dbname='{db}' user='{user}' "
                        u"password='{passwd}' port={port}").format(**conn_info)
            conn = psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            return None
        return conn

    @staticmethod
    def get_tables(source, conn):
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='public' order by table_name;
        """
        records = Postgresql.get_query_result(query, conn)
        records = map(lambda x: {'name': x[0], },
                      sorted(records, key=lambda y: y[0]))

        return records

    @staticmethod
    def _get_columns_query(source, tables):
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        # public - default scheme for postgres
        cols_query = psql_map.cols_query.format(tables_str, source.db, 'public')

        constraints_query = psql_map.constraints_query.format(tables_str)

        indexes_query = psql_map.indexes_query.format(tables_str)

        return cols_query, constraints_query, indexes_query

    @classmethod
    def get_columns(cls, source, user, tables, conn):

        max_ = 0
        active_tables = {}

        #FIXME на данный момент exist_result будет всегда пустой
        exist_result = []
        new_result = []
        root = None

        str_table = RedisCacheKeys.get_active_table(user.id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table(user.id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(user.id, source.id)

        if settings.USE_REDIS_CACHE:
            # выбранные ранее таблицы в редисе
            if not r_server.exists(str_active_tables):
                # root = tables[0]
                r_server.set(str_active_tables, '{}')
                r_server.expire(str_active_tables, settings.REDIS_EXPIRE)
            else:
                active_tables = json.loads(r_server.get(str_active_tables))
                for k, v in active_tables.items():
                    if v == 0:
                        root = k
                        break
                # max_ = max(active_tables.values())

            # exist_tables = [name for (name, order) in active_tables.items()
            #                 if r_server.exists(str_table.format(order))]
            #
            # tables = [x for x in tables if x not in exist_tables]
            #
            # for ex_t in exist_tables:
            #     ext_info = json.loads(r_server.get(str_table.format(active_tables[ex_t])))
            #     table = {"tname": ex_t, 'db': source.db, 'host': source.host,
            #              'cols': [x["name"] for x in ext_info['columns']], }
            #     exist_result.append(table)

        if not active_tables:
            columns_query, consts_query, indexes_query = cls._get_columns_query(source, tables)

            col_records = Database.get_query_result(columns_query, conn)

            if settings.USE_REDIS_CACHE:
                index_records = Database.get_query_result(indexes_query, conn)
                indexes = defaultdict(list)
                itable_name, icol_names, index_name, primary, unique = xrange(5)

                for ikey, igroup in groupby(index_records, lambda x: x[itable_name]):
                    for ig in igroup:
                        indexes[ikey].append({
                            "name": ig[index_name],
                            "columns": ig[icol_names].split(','),
                            "is_primary": ig[primary] == 't',
                            "is_unique": ig[unique] == 't',
                        })

                const_records = Database.get_query_result(consts_query, conn)
                constraints = defaultdict(list)
                (c_table_name, c_col_name, c_name, c_type,
                 c_foreign_table, c_foreign_col, c_update, c_delete) = xrange(8)

                for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
                    for ig in igroup:
                        constraints[ikey].append({
                            "c_col_name": ig[c_col_name],
                            "c_name": ig[c_name],
                            "c_type": ig[c_type],
                            "c_f_table": ig[c_foreign_table],
                            "c_f_col": ig[c_foreign_col],
                            "c_upd": ig[c_update],
                            "c_del": ig[c_delete],
                        })

                columns = defaultdict(list)
                foreigns = defaultdict(list)

                for key, group in groupby(col_records, lambda x: x[0]):

                    t_indexes = indexes[key]
                    t_consts = constraints[key]

                    for x in group:
                        is_index = is_unique = is_primary = False
                        col = x[1]

                        for i in t_indexes:
                            if col in i['columns']:
                                is_index = True
                                index_name = i['name']
                                for c in t_consts:
                                    const_type = c['c_type']
                                    if index_name == c['c_name']:
                                        if const_type == 'UNIQUE':
                                            is_unique = True
                                        elif const_type == 'PRIMARY KEY':
                                            is_unique = True
                                            is_primary = True

                        columns[key].append({"name": col, "type": psql_map.PSQL_TYPES[x[2]] or x[2],
                                             "is_index": is_index,
                                             "is_unique": is_unique, "is_primary": is_primary})

                    # находим внешние ключи
                    for c in t_consts:
                        if c['c_type'] == 'FOREIGN KEY':
                            foreigns[key].append({
                                "name": c['c_name'],
                                "source": {"table": key, "column": c["c_col_name"]},
                                "destination":
                                    {"table": c["c_f_table"], "column": c["c_f_col"]},
                                "on_delete": c["c_del"],
                                "on_update": c["c_upd"],
                            })
                # if root:
                #     tables.remove(root)
                #     tables.insert(0, root)

                # for ind, t_name in enumerate(tables, start=1):
                #     order = max_ + ind
                #     active_tables[t_name] = order
                #     r_server.set(str_table.format(order), json.dumps(
                #         {
                #             "columns": columns[key],
                #             "indexes": indexes[key],
                #             "foreigns": foreigns[key],
                #         }
                #     ))
                #     r_server.expire(str_table.format(t_name), settings.REDIS_EXPIRE)

                # # сохраняем активные таблицы
                # r_server.set(str_active_tables, json.dumps(active_tables))
                # r_server.expire(str_active_tables, settings.REDIS_EXPIRE)
                #
                # cls.set_joins(source, tables, new_result)

                for t_name in tables:
                    r_server.set(str_table_by_name.format(t_name), json.dumps(
                        {
                            "columns": columns[t_name],
                            "indexes": indexes[t_name],
                            "foreigns": foreigns[t_name],
                        }
                    ))
                    r_server.expire(str_table.format(t_name), settings.REDIS_EXPIRE)

                trees, without_bind = cls.build_trees(tuple(tables), source)

                sel_tree = cls.select_tree(trees)
                remains = without_bind[sel_tree.root.val]
                last = remains[0] if remains else None

                cls.insert_tree_to_redis(sel_tree, source)

                new_result += cls.get_final_info(sel_tree, source, last)

        return new_result

    @classmethod
    def get_final_info(cls, tree, source, last=None):
        result = []
        str_table = RedisCacheKeys.get_active_table(source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(source.user_id, source.id)
        ordered_nodes = tree.get_tree_ordered_nodes([tree.root, ])
        actives = json.loads(r_server.get(str_active_tables))
        db = source.db
        host = source.host

        for ind, node in enumerate(ordered_nodes):
            n_val = node.val
            n_info = {'tname': n_val, 'db': db, 'host': host,
                      'dest': getattr(node.parent, 'val', None),
                      'is_root': not ind
                      }
            order = actives.get(n_val)
            table_info = json.loads(r_server.get(str_table.format(order)))
            n_info['cols'] = [x['name'] for x in table_info['columns']]
            result.append(n_info)

        if last:
            table_info = json.loads(r_server.get(str_table_by_name.format(last)))
            l_info = {'tname': last, 'db': db, 'host': host,
                      'dest': n_val, 'is_last': True,
                      'cols': [x['name'] for x in table_info['columns']]
                      }
            result.append(l_info)
        return result

    @classmethod
    def insert_tree_to_redis(cls, tree, source):

        str_table = RedisCacheKeys.get_active_table(source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table(source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(source.user_id, source.id)
        str_joins = RedisCacheKeys.get_source_joins(source.user_id, source.id)
        ordered_nodes = tree.get_tree_ordered_nodes([tree.root, ])

        actives = {}
        joins_in_redis = {}

        for ind, node in enumerate(ordered_nodes, start=1):
            n_val = node.val
            r_server.set(str_table.format(ind),
                         r_server.get(str_table_by_name.format(n_val)))
            r_server.delete(str_table_by_name.format(n_val))
            actives[n_val] = ind

            joins = cls.get_node_joins_info(node)
            if joins:
                joins_in_redis[n_val] = joins

        r_server.set(str_active_tables, json.dumps(actives))
        r_server.set(str_joins, json.dumps(joins_in_redis))

    @classmethod
    def get_node_joins_info(cls, node):
        node_joins = []

        n_val = node.val
        for join in node.joins:
            t1, c1, t2, c2 = join
            if n_val == t1:
                node_joins.append({
                    "source": t1, "source_col": c1,
                    "destination": t2, "destination_col": c2,
                })
            else:
                node_joins.append({
                    "source": t2, "source_col": c2,
                    "destination": t1, "destination_col": c1,
                })
        return node_joins

    @classmethod
    def build_trees(cls, tables, source):
        str_table_by_name = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')

        trees = {}
        without_bind = {}

        for t_name in tables:
            tree = TablesTree(t_name)
            without_bind[t_name] = tree.build_tree(
                [tree.root, ], list(tables), str_table_by_name)
            trees[t_name] = tree

        # for t in trees.values():
        #     print t.root.val
        #     r_chs = [x for x in t.root.childs]
        #     print [x.val for x in r_chs]
        #     for c in r_chs:
        #         print [x.val for x in c.childs]
        #     print 80*'*'

        return trees, without_bind

    @classmethod
    def select_tree(cls, trees):
        counts = {}
        for tr_name, tree in trees.iteritems():
            counts[tr_name] = tree.get_nodes_count_by_level([tree.root])
        root_table = max(counts.iteritems(), key=operator.itemgetter(1))[0]

        return trees[root_table]

    @classmethod
    def get_joins(cls, l_t, r_t, l_info, r_info):

        l_cols = l_info['columns']
        r_cols = r_info['columns']

        joins = set()

        for l_c in l_cols:
            l_str = '{0}_{1}'.format(l_t, l_c['name'])
            for r_c in r_cols:
                r_str = '{0}_{1}'.format(r_t, r_c['name'])
                if l_c['name'] == r_str:
                    joins.add((l_t, l_c["name"], r_t, r_c["name"]))
                    break
                if l_str == r_c["name"]:
                    joins.add((l_t, l_c["name"], r_t, r_c["name"]))
                    break

        l_foreign = l_info['foreigns']
        r_foreign = r_info['foreigns']

        for f in l_foreign:
            if f['destination']['table'] == r_t:
                joins.add((
                    f['source']['table'],
                    f['source']['column'],
                    f['destination']['table'],
                    f['destination']['column'],
                ))

        for f in r_foreign:
            if f['destination']['table'] == l_t:
                joins.add((
                    f['source']['table'],
                    f['source']['column'],
                    f['destination']['table'],
                    f['destination']['column'],
                ))

        return joins


class Node(object):
    def __init__(self, t_name, parent=None, joins=set()):
        self.val = t_name
        self.parent = parent
        self.childs = []
        self.joins = joins


class TablesTree(object):
    def __init__(self, t_name):
        self.root = Node(t_name)

    def get_tree_ordered_nodes(self, nodes):
        all_nodes = []
        all_nodes += nodes
        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            all_nodes += self.get_tree_ordered_nodes(child_nodes)
        return all_nodes

    def get_nodes_count_by_level(self, nodes):
        counts = [len(nodes)]

        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            counts += self.get_nodes_count_by_level(child_nodes)
        return counts

    def build_tree(self, childs, tables, str_table_by_name):
        child_vals = [x.val for x in childs]
        tables = [x for x in tables if x not in child_vals]

        new_childs = []

        for child in childs:
            r_val = child.val
            l_info = json.loads(r_server.get(str_table_by_name.format(r_val)))

            for t_name in tables[:]:
                r_info = json.loads(r_server.get(str_table_by_name.format(t_name)))
                #todo refactor
                joins = Postgresql.get_joins(r_val, t_name, l_info, r_info)

                if joins:
                    tables.remove(t_name)
                    new_node = Node(t_name, child, joins)
                    child.childs.append(new_node)
                    new_childs.append(new_node)
        if new_childs:
            self.build_tree(new_childs, tables, str_table_by_name)

        # таблицы без связей
        return tables


class Mysql(Database):
    """Управление источником данных MySQL"""
    @staticmethod
    def get_connection(conn_info):
        try:
            conn = MySQLdb.connect(**conn_info)
        except MySQLdb.OperationalError:
            return None
        return conn

    @staticmethod
    def get_tables(source, conn):
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='{0}' order by table_name;
        """.format(source.db)

        records = Mysql.get_query_result(query, conn)
        records = map(lambda x: {'name': x[0], }, records)

        return records

    @staticmethod
    def _get_columns_query(source, tables):
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        query = """
            SELECT table_name, column_name FROM information_schema.columns
            where table_name in {0} and table_schema = '{1}';
        """.format(tables_str, source.db)
        return query

    @classmethod
    def get_columns(cls, source, user, tables, conn):
        pass


class DataSourceConnectionFactory(object):
    """Фабрика для подключения к источникам данных"""
    @staticmethod
    def factory(conn_type):
        if conn_type == ConnectionChoices.POSTGRESQL:
            return Postgresql()
        elif conn_type == ConnectionChoices.MYSQL:
            return Mysql()
        else:
            raise ValueError("Неизвестный тип подключения!")


class DataSourceService(object):
    """Сервис для источников данных"""
    @staticmethod
    def get_tables(source, conn):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_tables(source, conn)

    @staticmethod
    def get_columns(source, user, tables, conn):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_columns(source, user, tables, conn)

    @staticmethod
    def get_rows(source, conn, tables, cols):
        instance = DataSourceConnectionFactory.factory(source.conn_type)
        return instance.get_rows(conn, tables, cols)

    @staticmethod
    def get_connection(conn_info):

        instance = DataSourceConnectionFactory.factory(
            int(conn_info.get('conn_type', '')))

        del conn_info['conn_type']
        conn_info['port'] = int(conn_info['port'])

        conn = instance.get_connection(conn_info)
        if conn is None:
            raise ValueError("Сбой при подключении!")
        return conn


class RedisCacheKeys(object):
    """Ключи для редиса"""
    @staticmethod
    def get_user_databases(user_id):
        return 'user_datasources:{0}'.format(user_id)

    @staticmethod
    def get_user_datasource(user_id, datasource_id):
        return '{0}:{1}'.format(
            RedisCacheKeys.get_user_databases(user_id), datasource_id)

    @staticmethod
    def get_active_table(user_id, datasource_id, number):
        return '{0}:collection:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), number)

    @staticmethod
    def get_active_tables(user_id, datasource_id):
        return '{0}:active_collections'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_active_table_by_name(user_id, datasource_id, table):
        return '{0}:collection:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), table)

    @staticmethod
    def get_source_joins(user_id, datasource_id):
        return '{0}:joins'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))
