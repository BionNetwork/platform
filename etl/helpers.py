# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb
import json
import operator
from itertools import groupby
from collections import defaultdict

from django.conf import settings

from core.models import ConnectionChoices
from . import r_server
from .maps import postgresql as psql_map
from .maps import mysql as mysql_map


# FIXME use redis_collection

class JoinTypes(object):

    INNER, LEFT, RIGHT = ('inner', 'left', 'right')

    values = {
        INNER: "inner join",
        LEFT: "left join",
        RIGHT: "right join",
    }


class Operations(object):

    EQ, LT, GT, LTE, GTE, NEQ = ('eq', 'lt', 'gt', 'lte', 'gte', 'neq')

    values = {
        EQ: '=',
        LT: '<',
        GT: '>',
        LTE: '<=',
        GTE: '>=',
        NEQ: '<>',
    }


# типы колонок приходят типа bigint(20), убираем скобки
def lose_brackets(str_):
    return str_.split('(')[0].lower()


def get_utf8_string(value):
    """
    Кодирование в utf-8 строки
    :param value: string
    :return: string
    """
    return unicode(value)


# FIXME достаем из списка диктов по имени, смущает!
# FIXME сделать диктом actives {t_name: ind, }
def get_order_from_actives(t_name, actives):
    return [x for x in actives if x['name'] == t_name][0]['order']


def get_joins(l_t, r_t, l_info, r_info):
    """
        Функция выявляет связи между таблицами
    """

    l_cols = l_info['columns']
    r_cols = r_info['columns']

    joins = set()

    for l_c in l_cols:
        l_str = '{0}_{1}'.format(l_t, l_c['name'])
        for r_c in r_cols:
            r_str = '{0}_{1}'.format(r_t, r_c['name'])
            if l_c['name'] == r_str:
                #todo лишняя избыточность таблиц откуда и куда в каждой связи этих таблиц
                #todo joins переделать из сета в дикт
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

    dict_joins = []

    for join in joins:
        dict_joins.append({
            'left': {'table': join[0], 'column': join[1]},
            'right': {'table': join[2], 'column': join[3]},
            'operation': {"type": JoinTypes.INNER, "value": Operations.EQ},
        })

    return dict_joins


class Database(object):
    """
    Базовыми возможности для работы с базами данных
    Получение информации о таблице, список колонок, проверка соединения и т.д.
    """

    @staticmethod
    def get_query_result(query, conn):
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def _get_columns_query(source, tables):
        raise ValueError("Columns query is not realized")

    @staticmethod
    def get_rows(conn, tables, cols):
        query = """
            SELECT {0} FROM {1} LIMIT {2};
        """.format(', '.join(cols), ', '.join(tables),
                   settings.ETL_COLLECTION_PREVIEW_LIMIT)
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

    @classmethod
    def get_tables(cls, source, conn):
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='public' order by table_name;
        """
        records = cls.get_query_result(query, conn)
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
    def get_columns(cls, source, tables, conn):

        columns_query, consts_query, indexes_query = cls._get_columns_query(
            source, tables)

        col_records = Database.get_query_result(columns_query, conn)
        index_records = Database.get_query_result(indexes_query, conn)
        const_records = Database.get_query_result(consts_query, conn)

        return col_records, index_records, const_records

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
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

        table_name, col_name, col_type = xrange(3)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

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

                columns[key].append({"name": col,
                                     "type": (psql_map.PSQL_TYPES[lose_brackets(x[col_type])]
                                              or x[col_type]),
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
        return columns, indexes, foreigns


class Mysql(Database):
    """Управление источником данных MySQL"""
    @staticmethod
    def get_connection(conn_info):
        try:
            conn = MySQLdb.connect(**conn_info)
        except MySQLdb.OperationalError:
            return None
        return conn

    @classmethod
    def get_tables(cls, source, conn):
        query = """
            SELECT table_name FROM information_schema.tables
            where table_schema='{0}' order by table_name;
        """.format(source.db)

        records = cls.get_query_result(query, conn)
        records = map(lambda x: {'name': x[0], }, records)

        return records

    @staticmethod
    def _get_columns_query(source, tables):
        tables_str = '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

        cols_query = mysql_map.cols_query.format(tables_str, source.db)

        constraints_query = mysql_map.constraints_query.format(tables_str, source.db)

        indexes_query = mysql_map.indexes_query.format(tables_str, source.db)

        return cols_query, constraints_query, indexes_query

    @classmethod
    def get_columns(cls, source, tables, conn):
        columns_query, consts_query, indexes_query = cls._get_columns_query(
            source, tables)

        col_records = Database.get_query_result(columns_query, conn)
        index_records = Database.get_query_result(indexes_query, conn)
        const_records = Database.get_query_result(consts_query, conn)

        return col_records, index_records, const_records

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
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

        table_name, col_name, col_type = xrange(3)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

                for i in t_indexes:
                    if col in i['columns']:
                        is_index = True
                        for c in t_consts:
                            const_type = c['c_type']
                            if col == c['c_col_name']:
                                if const_type == 'UNIQUE':
                                    is_unique = True
                                elif const_type == 'PRIMARY KEY':
                                    is_unique = True
                                    is_primary = True

                columns[key].append({"name": col,
                                     "type": (mysql_map.MYSQL_TYPES[lose_brackets(x[col_type])]
                                              or x[col_type]),
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
        return columns, indexes, foreigns


class DatabaseService(object):
    """Сервис для источников данных"""

    @staticmethod
    def factory(conn_type):
        if conn_type == ConnectionChoices.POSTGRESQL:
            return Postgresql()
        elif conn_type == ConnectionChoices.MYSQL:
            return Mysql()
        else:
            raise ValueError("Неизвестный тип подключения!")

    @classmethod
    def get_tables(cls, source, conn):
        instance = cls.factory(source.conn_type)
        return instance.get_tables(source, conn)

    @classmethod
    def get_columns_info(cls, source, tables, conn):
        instance = cls.factory(source.conn_type)
        return instance.get_columns(source, tables, conn)

    @classmethod
    def get_rows(cls, source, conn, tables, cols):
        instance = cls.factory(source.conn_type)
        return instance.get_rows(conn, tables, cols)

    @classmethod
    def get_connection(cls, source):
        conn_info = source.get_connection_dict()
        return cls.get_connection_by_dict(conn_info)

    @classmethod
    def get_connection_by_dict(cls, conn_info):
        instance = cls.factory(
            int(conn_info.get('conn_type', '')))

        del conn_info['conn_type']
        conn_info['port'] = int(conn_info['port'])

        conn = instance.get_connection(conn_info)
        if conn is None:
            raise ValueError("Сбой при подключении!")
        return conn

    @classmethod
    def processing_records(cls, source, col_records, index_records, const_records):
        instance = cls.factory(source.conn_type)
        return instance.processing_records(col_records, index_records, const_records)


class Node(object):
    def __init__(self, t_name, parent=None, joins=[], join_type='inner'):
        self.val = t_name
        self.parent = parent
        self.childs = []
        self.joins = joins
        self.join_type = join_type

    # def get_node_joins_info(self):
    #     node_joins = defaultdict(list)
    #
    #     n_val = self.val
    #     for join in self.joins:
    #         t1, c1, t2, c2, oper = join
    #         if n_val == t1:
    #             node_joins[t2].append({
    #                 "source": t2, "source_col": c2,
    #                 "destination": t1, "destination_col": c1,
    #                 "join_val": oper,
    #                 "join_type": self.join_type,
    #             })
    #         else:
    #             node_joins[t1].append({
    #                 "source": t1, "source_col": c1,
    #                 "destination": t2, "destination_col": c2,
    #                 "join_val": oper,
    #                 "join_type": self.join_type,
    #             })
    #     return node_joins

    def get_node_joins_info(self):
        node_joins = defaultdict(list)

        n_val = self.val
        for join in self.joins:
            left = join['left']
            right = join['right']
            operation = join['operation']
            if n_val == right['table']:
                node_joins[left['table']].append({
                    "left": left,
                    "right": right,
                    "join_val": operation['value'],
                    "join_type": operation['type'],
                })
            else:
                node_joins[right['table']].append({
                    "left": right, "right": left,
                    "join_val": operation['value'],
                    "join_type": operation['type'],
                })
        return node_joins


class TablesTree(object):

    def __init__(self, t_name):
        self.root = Node(t_name)

    # def display(self):
    #     if self.root:
    #         print self.root.val, self.root.joins
    #         r_chs = [x for x in self.root.childs]
    #         print [(x.val, x.joins) for x in r_chs]
    #         for c in r_chs:
    #             print [x.val for x in c.childs]
    #         print 80*'*'
    #     else:
    #         print 'Empty Tree!!!'


class TableTreeRepository(object):

    @classmethod
    def get_tree_ordered_nodes(cls, nodes):
        all_nodes = []
        all_nodes += nodes
        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            all_nodes += cls.get_tree_ordered_nodes(child_nodes)
        return all_nodes

    @classmethod
    def get_nodes_count_by_level(cls, nodes):
        counts = [len(nodes)]

        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            counts += cls.get_nodes_count_by_level(child_nodes)
        return counts

    @classmethod
    def get_tree_structure(cls, root):
        root_info = {'val': root.val, 'childs': [], 'joins': list(root.joins),
                     'join_type': root.join_type}
        for ch in root.childs:
            root_info['childs'].append(cls.get_tree_structure(ch))
        return root_info

    @classmethod
    def build_tree(cls, childs, tables, tables_info):

        def inner_build_tree(childs, tables):
            child_vals = [x.val for x in childs]
            tables = [x for x in tables if x not in child_vals]

            new_childs = []

            for child in childs:
                new_childs += child.childs
                r_val = child.val
                l_info = tables_info[r_val]

                for t_name in tables[:]:
                    r_info = tables_info[t_name]
                    joins = get_joins(r_val, t_name, l_info, r_info)

                    if joins:
                        tables.remove(t_name)
                        new_node = Node(t_name, child, joins)
                        child.childs.append(new_node)
                        new_childs.append(new_node)

            if new_childs and tables:
                tables = inner_build_tree(new_childs, tables)

            # таблицы без связей
            return tables

        tables = inner_build_tree(childs, tables)

        return tables

    @classmethod
    def build_trees(cls, tables, source):

        trees = {}
        without_bind = {}

        for t_name in tables:
            tree = TablesTree(t_name)

            tables_info = RedisSourceService.info_for_tree_building(
                (), tables, source)

            without_bind[t_name] = cls.build_tree(
                [tree.root, ], tables, tables_info)
            trees[t_name] = tree

        return trees, without_bind

    @classmethod
    def select_tree(cls, trees):
        counts = {}
        for tr_name, tree in trees.iteritems():
            counts[tr_name] = cls.get_nodes_count_by_level([tree.root])
        root_table = max(counts.iteritems(), key=operator.itemgetter(1))[0]
        return trees[root_table]

    @classmethod
    def build_tree_by_structure(cls, structure):
        tree = TablesTree(structure['val'])

        def inner_build(root, childs):
            for ch in childs:
                new_node = Node(ch['val'], root, ch['joins'],
                                ch['join_type'])
                root.childs.append(new_node)
                inner_build(new_node, ch['childs'])

        inner_build(tree.root, structure['childs'])

        return tree

    @classmethod
    def delete_nodes_from_tree(cls, tree, source, tables):

        def inner_delete(node):
            for child in node.childs[:]:
                if child.val in tables:
                    child.parent = None
                    node.childs.remove(child)
                else:
                    inner_delete(child)

        r_val = tree.root.val
        if r_val in tables:
            RedisSourceService.tree_full_clean(source)
            tree.root = None
        else:
            inner_delete(tree.root)

    @classmethod
    def update_node_joins(cls, sel_tree, left_table,
                          right_table, join_type, joins):

        nodes = cls.get_tree_ordered_nodes([sel_tree.root, ])
        parent = [x for x in nodes if x.val == left_table][0]
        childs = [x for x in parent.childs if x.val == right_table]

        # случай, когда две таблицы не имели связей
        if not childs:
            node = Node(right_table, parent, [], join_type)
            parent.childs.append(node)
        else:
            # меняем существующие связи
            node = childs[0]
            node.joins = []

        for came_join in joins:
            parent_col, oper, child_col = came_join
            # todo опять переисбыточность!!!
            node.joins.append({
                'left': {'table': left_table, 'column': parent_col},
                'right': {'table': right_table, 'column': child_col},
                'operation': {"type": join_type, "value": oper},
            })


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

    # таблицы без связей
    @staticmethod
    def get_source_remain(user_id, datasource_id):
        return '{0}:remain'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_active_tree(user_id, datasource_id):
        return '{0}:active:tree'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))


class RedisSourceService(object):

    @classmethod
    def delete_datasource(cls, source):
        """ удаляет информацию о датасосре из редиса
        """
        user_db_key = RedisCacheKeys.get_user_databases(source.user_id)
        user_datasource_key = RedisCacheKeys.get_user_datasource(
            source.user_id, source.id)

        r_server.lrem(user_db_key, 1, source.id)
        r_server.delete(user_datasource_key)

    @classmethod
    def get_tables(cls, source, tables):
        user_db_key = RedisCacheKeys.get_user_databases(source.user_id)
        user_datasource_key = RedisCacheKeys.get_user_datasource(source.user_id, source.id)

        def inner_save_tables():
            new_db = {
                "db": source.db,
                "host": source.host,
                "tables": tables
            }
            if str(source.id) not in r_server.lrange(user_db_key, 0, -1):
                r_server.rpush(user_db_key, source.id)
            r_server.set(user_datasource_key, json.dumps(new_db))
            r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)
            return new_db

        if not r_server.exists(user_datasource_key):
            return inner_save_tables()

        return json.loads(r_server.get(user_datasource_key))

    @classmethod
    def delete_tables(cls, source, tables):
        rck = RedisCacheKeys

        str_table = rck.get_active_table(source.user_id, source.id, '{0}')
        str_table_by_name = rck.get_active_table(source.user_id, source.id, '{0}')
        str_active_tables = rck.get_active_tables(source.user_id, source.id)
        str_joins = rck.get_source_joins(source.user_id, source.id)

        actives = json.loads(r_server.get(str_active_tables))
        joins = json.loads(r_server.get(str_joins))

        # если есть, то удаляем таблицу без связей
        for t_name in tables:
            r_server.delete(str_table_by_name.format(t_name))

        # удаляем все джоины пришедших таблиц
        cls.initial_delete_joins(tables, joins)
        child_tables = cls.delete_joins(tables, joins)

        # добавляем к основным таблицам, их дочерние для дальнейшего удаления
        tables += child_tables

        r_server.set(str_joins, json.dumps(joins))

        # удаляем полную инфу пришедших таблиц
        cls.delete_tables_info(tables, actives, str_table)
        r_server.set(str_active_tables, json.dumps(actives))

    @classmethod
    def initial_delete_joins(cls, tables, joins):
        """
            удаляем связи таблиц, из таблиц, стоящих левее выбранных
        """
        for v in joins.values():
            for j in v[:]:
                if j['right']['table'] in tables:
                    v.remove(j)

    @classmethod
    def delete_joins(cls, tables, joins):
        """
            удаляем связи таблиц, плюс связи таблиц, стоящих правее выбранных!
            возвращает имена дочерних таблиц на удаление
        """
        destinations = []
        for table in tables:
            if table in joins:
                destinations += [x['right']['table'] for x in joins[table]]
                del joins[table]
                if destinations:
                    destinations += cls.delete_joins(destinations, joins)
        return destinations

    @classmethod
    def delete_tables_info(cls, tables, actives, str_table):
        names = [x['name'] for x in actives]
        for table in tables:
            if table in names:
                found = [x for x in actives if x['name'] == table][0]
                r_server.delete(str_table.format(found['order']))
                actives.remove(found)

    @classmethod
    def get_table_full_info(cls, source, table):

        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)

        active_tables = json.loads(r_server.get(str_active_tables))

        if r_server.exists(str_table_by_name.format(table)):
            return r_server.get(str_table_by_name.format(table))
        else:
            order = [x for x in active_tables if x['name'] == table][0]['order']
            return r_server.get(str_table.format(order))

    # сохраняем структуру дерева
    @classmethod
    def save_active_tree(cls, tree_structure, source):
        str_active_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)

        r_server.set(str_active_tree, json.dumps(tree_structure))

    # достаем структуру дерева из редиса
    @classmethod
    def get_active_tree_structure(cls, source):
        str_active_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)

        return json.loads(r_server.get(str_active_tree))

    @classmethod
    def insert_tree(cls, structure, ordered_nodes, source):

        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        str_joins = RedisCacheKeys.get_source_joins(
            source.user_id, source.id)

        new_actives = []
        joins_in_redis = defaultdict(list)

        pipe = r_server.pipeline()

        for ind, node in enumerate(ordered_nodes, start=1):
            n_val = node.val

            # достаем инфу либо по имени, либо по порядковому номеру
            pipe.set(str_table.format(ind),
                     RedisSourceService.get_table_full_info(source, n_val))
            # удаляем таблицы с именованными ключами
            pipe.delete(str_table_by_name.format(n_val))

            # строим новую карту активных таблиц
            new_actives.append({'name': n_val, 'order': ind})

            # добавляем инфу новых джойнов
            joins = node.get_node_joins_info()
            for k, v in joins.iteritems():
                joins_in_redis[k] += v

        pipe.set(str_active_tables, json.dumps(new_actives))
        pipe.set(str_joins, json.dumps(joins_in_redis))

        pipe.execute()

        # сохраняем само дерево
        RedisSourceService.save_active_tree(structure, source)

        # удаляем инфу о таблице без связи, если она есть
        # TODO почему-то если раскоментить, то не работает, разобраться!!!
        # RedisSourceService.delete_last_remain(source)

    @classmethod
    def tree_full_clean(cls, source):
        """ удаляет информацию о таблицах, джоинах, дереве
            из редиса
        """
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        str_joins = RedisCacheKeys.get_source_joins(
            source.user_id, source.id)
        str_remain = RedisCacheKeys.get_source_remain(
            source.user_id, source.id)
        str_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        if r_server.exists(str_active_tables):
            for item in json.loads(r_server.get(str_active_tables)):
                str_t = str_table.format(item['order'])
                if r_server.exists(str_t):
                    r_server.delete(str_t)

        r_server.delete(str_table_by_name.format(r_server.get(str_remain)))
        r_server.delete(str_remain)
        r_server.delete(str_active_tables)
        r_server.delete(str_joins)
        r_server.delete(str_tree)

    @classmethod
    def insert_remains(cls, source, remains):
        str_remain = RedisCacheKeys.get_source_remain(source.user_id, source.id)
        if remains:
            # первая таблица без связей
            last = remains[0]
            # таблица без связей
            r_server.set(str_remain, last)

            # удаляем таблицы без связей, кроме первой
            cls.delete_unneeded_remains(source, remains[1:])
        else:
            last = None
            # r_server.set(str_remain, '')
        # либо таблица без связи, либо None
        return last

    @classmethod
    def delete_unneeded_remains(cls, source, remains):
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        for t_name in remains:
            r_server.delete(str_table_by_name.format(t_name))

    @classmethod
    def delete_last_remain(cls, source):
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_remain = RedisCacheKeys.get_source_remain(
            source.user_id, source.id)
        if r_server.exists(str_remain):
            last = r_server.get(str_remain)
            r_server.delete(str_table_by_name.format(last))
            r_server.delete(str_remain)

    @classmethod
    def get_columns_for_tables_without_bind(
            cls, source, parent_table, without_bind_table):
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        str_remain = RedisCacheKeys.get_source_remain(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        err_msg = 'Истекло время хранения ключей в редисе!'

        if (not r_server.exists(str_active_tables) or
                not r_server.exists(str_remain)):
            raise Exception(err_msg)

        wo_bind_columns = json.loads(r_server.get(str_table_by_name.format(
            without_bind_table)))['columns']

        actives = json.loads(r_server.get(str_active_tables))

        parent_columns = json.loads(r_server.get(str_table.format(
            get_order_from_actives(parent_table, actives)
        )))['columns']

        return {
            without_bind_table: [x['name'] for x in wo_bind_columns],
            parent_table: [x['name'] for x in parent_columns],
            'without_bind': True,
        }

    @classmethod
    def get_columns_for_tables_with_bind(
            cls, source, parent_table, child_table):
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_joins = RedisCacheKeys.get_source_joins(
            source.user_id, source.id)

        err_msg = 'Истекло время хранения ключей!'

        if (not r_server.exists(str_active_tables) or
                not r_server.exists(str_joins)):
            raise Exception(err_msg)

        actives = json.loads(r_server.get(str_active_tables))

        parent_columns = json.loads(r_server.get(str_table.format(
            get_order_from_actives(parent_table, actives)
        )))['columns']

        child_columns = json.loads(r_server.get(str_table.format(
            get_order_from_actives(child_table, actives)
        )))['columns']

        exist_joins = json.loads(r_server.get(str_joins))
        parent_joins = exist_joins[parent_table]
        child_joins = [x for x in parent_joins if x['right']['table'] == child_table]

        return {
            child_table: [x['name'] for x in child_columns],
            parent_table: [x['name'] for x in parent_columns],
            'without_bind': False,
            'joins': child_joins,
        }

    @classmethod
    def get_final_info(cls, ordered_nodes, source, last=None):
        result = []
        str_table = RedisCacheKeys.get_active_table(source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(source.user_id, source.id)
        actives = json.loads(r_server.get(str_active_tables))
        db = source.db
        host = source.host

        for ind, node in enumerate(ordered_nodes):
            n_val = node.val
            n_info = {'tname': n_val, 'db': db, 'host': host,
                      'dest': getattr(node.parent, 'val', None),
                      'is_root': not ind, 'without_bind': False,
                      }
            order = get_order_from_actives(n_val, actives)
            table_info = json.loads(r_server.get(str_table.format(order)))
            n_info['cols'] = [x['name'] for x in table_info['columns']]
            result.append(n_info)

        if last:
            table_info = json.loads(r_server.get(str_table_by_name.format(last)))
            l_info = {'tname': last, 'db': db, 'host': host,
                      'dest': n_val, 'without_bind': True,
                      'cols': [x['name'] for x in table_info['columns']]
                      }
            result.append(l_info)
        return result

    @classmethod
    def insert_columns_info(cls, source, tables, columns, indexes, foreigns):
        active_tables = []
        user_id = source.user_id

        str_table = RedisCacheKeys.get_active_table(user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table(user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(user_id, source.id)

        pipe = r_server.pipeline()

        # выбранные ранее таблицы в редисе
        if not r_server.exists(str_active_tables):
            pipe.set(str_active_tables, '[]')
            pipe.expire(str_active_tables, settings.REDIS_EXPIRE)
        else:
            active_tables = json.loads(r_server.get(str_active_tables))

        for t_name in tables:
            pipe.set(str_table_by_name.format(t_name), json.dumps(
                {
                    "columns": columns[t_name],
                    "indexes": indexes[t_name],
                    "foreigns": foreigns[t_name],
                }
            ))
            pipe.expire(str_table.format(t_name), settings.REDIS_EXPIRE)
        pipe.execute()
        return active_tables

    @classmethod
    def info_for_tree_building(cls, ordered_nodes, tables, source):
        user_id = source.user_id
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            user_id, source.id, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            user_id, source.id, '{0}')
        str_active_tables = RedisCacheKeys.get_active_tables(
            user_id, source.id)
        actives = json.loads(r_server.get(str_active_tables))

        final_info = {}

        # инфа таблиц из существующего дерева
        for child in ordered_nodes:
            ch_val = child.val
            order = [x for x in actives if x['name'] == ch_val][0]['order']
            final_info[ch_val] = json.loads(r_server.get(str_table.format(order)))
        # инфа таблиц не из дерева
        for t_name in tables:
            if r_server.exists(str_table_by_name.format(t_name)):
                final_info[t_name] = json.loads(
                    r_server.get(str_table_by_name.format(t_name)))

        return final_info


class DataSourceService(object):
    """
        Сервис управляет сервисами БД и Редиса
    """
    @classmethod
    def delete_datasource(cls, source):
        """ удаляет информацию о датасосре
        """
        RedisSourceService.delete_datasource(source)

    @classmethod
    def tree_full_clean(cls, source):
        """ удаляет информацию о таблицах, джоинах, дереве
        """
        RedisSourceService.tree_full_clean(source)

    @staticmethod
    def get_database_info(source):
        """ Возвращает таблицы истоника данных
        """
        conn = DatabaseService.get_connection(source)
        tables = DatabaseService.get_tables(source, conn)

        if settings.USE_REDIS_CACHE:
            return RedisSourceService.get_tables(source, tables)
        else:
            return {
                "db": source.db,
                "host": source.host,
                "tables": tables
            }

    @staticmethod
    def check_connection(post):
        """ Проверяет подключение
        """
        conn_info = {
            'host': get_utf8_string(post.get('host')),
            'user': get_utf8_string(post.get('login')),
            'passwd': get_utf8_string(post.get('password')),
            'db': get_utf8_string(post.get('db')),
            'port': get_utf8_string(post.get('port')),
            'conn_type': get_utf8_string(post.get('conn_type')),
        }

        return DatabaseService.get_connection_by_dict(conn_info)

    @classmethod
    def get_columns_info(cls, source, tables):

        conn = DatabaseService.get_connection(source)
        col_records, index_records, const_records = (
            DatabaseService.get_columns_info(source, tables, conn))

        cols, indexes, foreigns = DatabaseService.processing_records(
            source, col_records, index_records, const_records)

        if settings.USE_REDIS_CACHE:
            active_tables = RedisSourceService.insert_columns_info(
                source, tables, cols, indexes, foreigns)
            # работа с деревьями
            if not active_tables:
                trees, without_bind = TableTreeRepository.build_trees(tuple(tables), source)
                sel_tree = TableTreeRepository.select_tree(trees)

                remains = without_bind[sel_tree.root.val]
            else:
                # достаем структуру дерева из редиса
                structure = RedisSourceService.get_active_tree_structure(source)
                # строим дерево
                sel_tree = TableTreeRepository.build_tree_by_structure(structure)

                ordered_nodes = TableTreeRepository.get_tree_ordered_nodes([sel_tree.root, ])

                tables_info = RedisSourceService.info_for_tree_building(
                    ordered_nodes, tables, source)

                # перестраиваем дерево
                remains = TableTreeRepository.build_tree(
                    [sel_tree.root, ], tuple(tables), tables_info)

            # таблица без связи
            last = RedisSourceService.insert_remains(source, remains)

            # сохраняем дерево
            structure = TableTreeRepository.get_tree_structure(sel_tree.root)
            ordered_nodes = TableTreeRepository.get_tree_ordered_nodes([sel_tree.root, ])
            RedisSourceService.insert_tree(structure, ordered_nodes, source)

            # возвращаем результат
            return RedisSourceService.get_final_info(ordered_nodes, source, last)

        return []

    @classmethod
    def get_rows_info(cls, source, tables, cols):

        conn = DatabaseService.get_connection(source)
        return DatabaseService.get_rows(source, conn, tables, cols)

    @classmethod
    def remove_tables_from_tree(cls, source, tables):
        # достаем структуру дерева из редиса
        structure = RedisSourceService.get_active_tree_structure(source)
        # строим дерево
        sel_tree = TableTreeRepository.build_tree_by_structure(structure)
        TableTreeRepository.delete_nodes_from_tree(sel_tree, source, tables)

        if sel_tree.root:
            RedisSourceService.delete_tables(source, tables)

            ordered_nodes = TableTreeRepository.get_tree_ordered_nodes([sel_tree.root, ])
            structure = TableTreeRepository.get_tree_structure(sel_tree.root)
            RedisSourceService.insert_tree(structure, ordered_nodes, source)

    @classmethod
    def get_columns_for_choices(cls, source, parent_table,
                                child_table, is_without_bind):
        if is_without_bind:
            data = RedisSourceService.get_columns_for_tables_without_bind(
                source, parent_table, child_table)
        else:
            data = RedisSourceService.get_columns_for_tables_with_bind(
                source, parent_table, child_table)

        return data

    @classmethod
    def save_new_joins(cls, source, left_table, right_table, join_type, joins):
        # достаем структуру дерева из редиса
        structure = RedisSourceService.get_active_tree_structure(source)
        # строим дерево
        sel_tree = TableTreeRepository.build_tree_by_structure(structure)
        TableTreeRepository.update_node_joins(
            sel_tree, left_table, right_table, join_type, joins)

        # сохраняем дерево
        ordered_nodes = TableTreeRepository.get_tree_ordered_nodes([sel_tree.root, ])
        structure = TableTreeRepository.get_tree_structure(sel_tree.root)
        RedisSourceService.insert_tree(structure, ordered_nodes, source)

        data = RedisSourceService.get_final_info(ordered_nodes, source)

        return data
