# coding: utf-8
from __future__ import unicode_literals

from . import r_server
import json
from copy import deepcopy
from django.conf import settings
from collections import defaultdict
from redis_collections import Dict as RedisDict
from core.helpers import CustomJsonEncoder


T_S = "T{0}_S{1}"


class RedisCacheKeys(object):
    """Ключи для редиса"""

    @staticmethod
    def get_user_datasource(datasource_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'source:{0}'.format(datasource_id)

    @staticmethod
    def get_card_key(card_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'card:{0}'.format(card_id)

    @staticmethod
    def get_user_collection_counter(source_key):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param user_id: int
        :param datasource_id: int
        :return: str
        """
        return u'{0}:counter'.format(source_key)

    @staticmethod
    def get_card_builder(card_key):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param card_key: str
        :return: str
        """
        return u'{0}:builder'.format(card_key)

    @classmethod
    def get_active_table(cls, card_id, source_id, table_id):
        """
        фулл инфа таблицы, которая в дереве
        """
        card_key = cls.get_card_key(card_id)
        source_key = cls.get_user_datasource(source_id)
        return u'{0}:{1}:collection:{2}'.format(
            card_key, source_key, table_id)

    @staticmethod
    def get_active_table_ddl(source_key, number):
        """
        фулл инфа таблицы, которая в дереве для ddl
        :param user_id:
        :param datasource_id:
        :param number:
        :return:
        """
        return u'{0}:ddl:{1}'.format(source_key, number)

    @staticmethod
    def get_active_tables(source_key):
        """
        список таблиц из дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'{0}:active_collections'.format(source_key)

    @staticmethod
    def get_active_table_by_name(source_key, table):
        """
        фулл инфа таблицы, которая НЕ в дереве
        :param user_id:
        :param datasource_id:
        :param table:
        :return:
        """
        return u'{0}:collection:{1}'.format(source_key, table)

    @staticmethod
    def get_source_joins(source_key):
        """
        инфа о джоинах дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'{0}:joins'.format(source_key)

    @staticmethod
    def get_source_remain(source_key):
        """
        таблица без связей
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'{0}:remain'.format(source_key)

    @staticmethod
    def get_active_tree(source_key):
        """
        Структура дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'{0}:active:tree'.format(source_key)

    @staticmethod
    def get_user_subscribers(user_id):
        """
        ключ каналов юзера для сокетов
        """
        return u'user_channels:{0}'.format(user_id)

    @staticmethod
    def get_queue(task_id):
        """
        ключ информации о ходе работы таска
        """
        return u'queue:{0}'.format(task_id)

    @classmethod
    def get_indent_key(cls, source_id):
        """
        Ключ отступа
        """
        source_key = cls.get_user_datasource(source_id)
        return u'{0}:indent'.format(source_key)

RKeys = RedisCacheKeys


class RedisSourceService(object):
    """
        Сервис по работе с редисом
    """

    @staticmethod
    def r_get(name, params=None):
        if params:
            return json.loads(r_server.get(name.format(*params)))
        else:
            return json.loads(r_server.get(name))

    @staticmethod
    def r_set(name, structure):
        r_server.set(name, json.dumps(structure))

    @staticmethod
    def r_del(name):
        r_server.delete(name)

    @staticmethod
    def r_exists(name):
        return r_server.exists(name)

    @staticmethod
    def get_user_source(source_id):
        return RKeys.get_user_datasource(source_id)

    @classmethod
    def delete_datasource(cls, source):
        """
        удаляет информацию о датасосре из редиса
        :param cls:
        :param source: Datasource
        """
        user_datasource_key = cls.get_user_source(source)

        r_server.delete(user_datasource_key)

    @classmethod
    def set_tables(cls, source_id, tables):
        """
        кладем информацию о таблицах в редис
        :param source: Datasource
        :param tables: list
        :return: list
        """
        user_datasource_key = cls.get_user_source(source_id)

        r_server.set(user_datasource_key,
                     json.dumps({'tables': tables}))
        r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)

    @classmethod
    def delete_tables_NEW(cls, card_id, tables):
        """
        удаляет информацию о таблицах

        Args:
            source(Datasource): объект Источника
            tables(list): Список названий таблиц
        """
        card_key = RKeys.get_card_key(card_id)
        str_actives = RKeys.get_card_builder(card_key)
        str_joins = RKeys.get_source_joins(card_key)

        actives = cls.get_card_builder(card_id)
        actives_data = actives['data']

        # если есть, то удаляем таблицу без связей
        for (t_name, sid) in tables:
            f_sid = str(sid)
            source_key = RKeys.get_user_datasource(sid)
            if t_name in actives_data[f_sid]['actives']:
                table_str = RedisCacheKeys.get_active_table(
                    source_key, actives_data[f_sid]['actives'][t_name])
                del actives_data[f_sid]['actives'][t_name]
            else:
                table_str = RedisCacheKeys.get_active_table(
                    source_key, t_name)
                actives_data[f_sid]['remains'].remove(t_name)
            r_server.delete(table_str)

        joins = json.loads(r_server.get(str_joins))

        # удаляем все джоины пришедших таблиц
        cls.initial_delete_joins(tables, joins)
        child_tables = cls.delete_joins_NEW(tables, joins)

        # добавляем к основным таблицам, их дочерние для дальнейшего удаления
        tables += child_tables

        r_server.set(str_joins, json.dumps(joins))
        r_server.set(str_actives, json.dumps(actives))

    @classmethod
    def initial_delete_joins(cls, tables, joins):
        """
        Удаляем связи таблиц, из таблиц, стоящих левее выбранных
        """
        for k, v in joins.items():
            for j in v[:]:
                if (j['right']['table'], j['right']['sid']) in tables:
                    v.remove(j)
            if not joins[k]:
                del joins[k]

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
    def delete_joins_NEW(cls, tables, joins):
        """
            удаляем связи таблиц, плюс связи таблиц, стоящих правее выбранных!
            возвращает имена дочерних таблиц на удаление
        """
        destinations = []
        for (table, sid) in tables:
            s_t = T_S.format(sid, table)
            if s_t in joins:
                destinations += [
                    (x['right']['table'], x['right']['sid'])
                    for x in joins[table]]
                del joins[s_t]
                if destinations:
                    destinations += cls.delete_joins(destinations, joins)
        return destinations

    @classmethod
    def delete_tables_info(cls, tables, actives, str_table, str_table_ddl):
        """
        удаляет информацию о таблицах
        :param tables: list
        :param actives: list
        :param str_table: str
        """
        names = [x['name'] for x in actives]
        for table in tables:
            if table in names:
                found = [x for x in actives if x['name'] == table][0]
                r_server.delete(str_table.format(found['id']))
                r_server.delete(str_table_ddl.format(found['id']))
                actives.remove(found)

    @classmethod
    def get_collection_name(cls, source, table):
        """
        Получение название коллекции для таблицы

        Args:
            source_key(str): Базовая часть ключа
            table(str): Название таблицы

        Returns:
            str: Название коллекции
        """

        source_key = cls.get_user_source(source)
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, table)

        active_tables = cls.get_active_table_list(source_key)

        if r_server.exists(str_table_by_name):
            return str_table_by_name
        else:
            order = [x for x in active_tables if x['name'] == table][0]['id']
            return RedisCacheKeys.get_active_table(source_key, order)

    @classmethod
    def get_table_full_info(cls, source, table):
        """
        Получение полной информации по источнику из хранилища

        Args:
            source(`Datasource`): Объект источника
            table(str): Название таблицы

        Returns:
            str: Данные по коллекции
        """
        return r_server.get(cls.get_collection_name(source, table))

    # FIXME перенесен в кэш
    @classmethod
    def set_table_info(cls, card_id, source_id, table_id, table_info):
        str_table = RKeys.get_active_table(card_id, source_id, table_id)
        return cls.r_set(str_table, table_info)

    @classmethod
    def del_table_info(cls, node_id, source_id, table_id):
        str_table = RKeys.get_active_table(
            node_id, source_id, table_id)
        return cls.r_del(str_table)

    @classmethod
    def filter_exists_tables(cls, source, tables):
        """
        Возвращает таблицы, которых нет в редисе в ранее выбранных коллекциях

        Args:
            source(Datasource): объект источника
            tables(list): список названий таблиц, которые ищутся в редисе

        Returns:
            list: Список входных таблиц, которых нет в редис
            list: Список всех таблиц, которые есть в редис
        """
        source_key = cls.get_user_source(source)
        coll_counter = cls.get_collection_counter(source_key)
        # список коллекций
        actives_names = [x['name'] for x in coll_counter['data']]
        not_exists = [t for t in tables if t not in actives_names]
        return not_exists, actives_names

    @classmethod
    def check_table_in_builder(cls, card_id, source_id, table):
        """
        Проверяет таблица в остатках или нет
        """
        builder_data = cls.get_card_builder_data(card_id)
        s_id = str(source_id)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            # таблица не должна быть в активных
            if (table in source_colls['actives'] or
                        table in source_colls['remains']):
                return True
        return False

    @classmethod
    def check_tree_exists(cls, card_id):
        """
        Проверяет существование дерева

        Args:
            card_id(int): id карточки
        Returns:
            bool: Наличие 'user_datasource:<user_id>:<source_id>:active:tree'
        """
        user_card_key = RedisCacheKeys.get_card_key(card_id)
        str_active_tree = RedisCacheKeys.get_active_tree(user_card_key)

        return r_server.exists(str_active_tree)

    @classmethod
    def save_active_tree(cls, tree_structure, source):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        :param source: Datasource
        """
        source_key = cls.get_user_source(source)
        str_active_tree = RedisCacheKeys.get_active_tree(source_key)

        cls.r_set(str_active_tree, tree_structure)

    @classmethod
    def save_tree_structure(cls, card_id, tree):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        :param source: Datasource
        """
        card_key = RKeys.get_card_key(card_id)
        str_active_tree = RKeys.get_active_tree(card_key)

        tree_structure = tree.structure
        cls.r_set(str_active_tree, tree_structure)

    @classmethod
    def put_remain_to_builder_actives(cls, card_id, node):
        """
        сохраняем карту дерева, перенос остатка в активные
        """
        builber = cls.get_card_builder(card_id)
        b_data = builber['data']

        table, sid, node_id = node.val, str(node.source_id), node.node_id

        s_remains = b_data[sid]['remains']
        s_actives = b_data[sid]['actives']

        if table not in s_remains:
            raise Exception("Table not in remains!")
        if s_remains[table] != node_id:
            raise Exception("Table ID is broken!")
        if table in s_actives:
            raise Exception("Table already in actives!")

        del s_remains[table]
        s_actives[table] = node_id

        cls.set_card_builder(card_id, builber)

    @classmethod
    def put_actives_to_builder_remains(cls, card_id, to_remain_nodes):
        """
        сохраняем карту дерева, перенос активов в остатки
        """
        builber = cls.get_card_builder(card_id)
        b_data = builber['data']

        for node in to_remain_nodes:
            table, sid, node_id = (
                node.val, str(node.source_id), node.node_id)

            s_remains = b_data[sid]['remains']
            s_actives = b_data[sid]['actives']

            del s_actives[table]
            s_remains[table] = node_id

        cls.set_card_builder(card_id, builber)

    @classmethod
    def insert_remains(cls, source, remains):
        """
        сохраняет таблицу без связей
        :param source: Datasource
        :param remains: list
        :return:
        """
        source_key = cls.get_user_source(source)
        str_remain = RedisCacheKeys.get_source_remain(source_key)
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            source_key, '{0}')
        if remains:
            # первая таблица без связей
            last = remains[0]
            # таблица без связей
            r_server.set(str_remain, last)

            # если таблицу ранее уже выбирали, ее инфа лежит в
            # счетчике коллекций и достать ее оттуда, иначе болтается
            # отдельно и доставать по имени
            if not r_server.exists(str_table_by_name.format(last)):
                actives = cls.get_active_table_list(source_key)
                order = cls.get_order_from_actives(last, actives)
                r_server.set(
                    str_table_by_name.format(last),
                    r_server.get(str_table.format(order)))

            # удаляем таблицы без связей, кроме первой
            cls.delete_unneeded_remains(source, remains[1:])
        else:
            last = None
            # r_server.set(str_remain, '')
        # либо таблица без связи, либо None
        return last

    @classmethod
    def delete_unneeded_remains(cls, source, remains):
        """
        удаляет таблицы без связей,(все кроме первой)
        :param source: Datasource
        :param remains: list
        """
        source_key = cls.get_user_source(source)
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')

        for t_name in remains:
            r_server.delete(str_table_by_name.format(t_name))

    @classmethod
    def delete_last_remain(cls, source_key):
        """
        удаляет единственную таблицу без связей
        :param source: Datasource
        """
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')
        str_remain = RedisCacheKeys.get_source_remain(
            source_key)
        if r_server.exists(str_remain):
            last = cls.get_last_remain(source_key)
            r_server.delete(str_table_by_name.format(last))
            r_server.delete(str_remain)


    # FIXME убрать метод
    @classmethod
    def get_remain_node(cls, nodes, node_id):
        """
        Получение узла
        """
        node_id = int(node_id)

        for node in nodes:
            if node.node_id == node_id:
                return node
        return

    # FIXME перенесен в кэш
    @classmethod
    def put_table_info_in_builder(cls, card_id, source_id, table, table_info):
        """
        инфу таблицы кладем в остатки билдера дерева
        """
        builder = cls.get_card_builder(card_id)
        next_id = builder['next_id']
        b_data = builder['data']

        cls.set_table_info(card_id, source_id, next_id, table_info)

        sid = str(source_id)
        if sid not in b_data:
            b_data[sid] = {
                'actives': {},
                'remains': {table: next_id, },
            }
        else:
            b_data[sid]['remains'][table] = next_id

        builder['next_id'] = next_id + 1

        # save builder's new state
        cls.set_card_builder(card_id, builder)

        return next_id

    @classmethod
    def insert_date_intervals(cls, source, tables, intervals):
        # сохраняем актуальный период дат у каждой таблицы
        # для каждой колонки типа дата

        source_key = cls.get_user_source(source)

        str_table = RedisCacheKeys.get_active_table(source_key, '{0}')
        str_table_ddl = RedisCacheKeys.get_active_table_ddl(source_key, '{0}')

        # список имеющихся коллекций
        actives = cls.get_active_table_list(source_key)
        names = [x['name'] for x in actives]

        # если старые таблицы, каким то образом не в активных коллекциях
        if [t for t in tables if t not in names]:
            raise Exception("Cтарая таблица, не в активных коллекциях!")

        pipe = r_server.pipeline()

        for t_name in tables:

            found = [x for x in actives if x['name'] == t_name][0]
            found_id = found["id"]
            # collection info
            coll_info = json.loads(
                r_server.get(str_table.format(found_id)))
            coll_info["date_intervals"] = intervals.get(t_name, [])
            pipe.set(str_table.format(found_id),
                     json.dumps(coll_info, cls=CustomJsonEncoder))
            # ddl info
            ddl_info = json.loads(
                r_server.get(str_table_ddl.format(found_id)))
            ddl_info["date_intervals"] = intervals.get(t_name, [])
            pipe.set(str_table_ddl.format(found_id),
                     json.dumps(ddl_info, cls=CustomJsonEncoder))

        pipe.execute()

    @classmethod
    def get_order_from_actives(cls, t_name, actives):
        """
        возвращает порядковый номер таблицы по имени
        :param t_name:
        :param actives:
        :return: list
        """
        processed = [x for x in actives if x['name'] == t_name]
        return processed[0]['id'] if processed else None

    @staticmethod
    def get_user_subscribers(user_id):
        """
        каналы юзера для сокетов
        :param user_id:
        :return:
        """
        subs_str = RedisCacheKeys.get_user_subscribers(user_id)
        if not r_server.exists(subs_str):
            return []

        return r_server.get(subs_str)

    @classmethod
    def set_user_subscribers(cls, user_id, channel):
        """
        добавляем канал юзера для сокетов
        """
        subs_str = RedisCacheKeys.get_user_subscribers(user_id)
        subscribes = cls.get_user_subscribers(user_id)
        if subscribes:
            channels = json.loads(cls.get_user_subscribers(user_id))
        else:
            channels = []
        channels.append(channel)
        r_server.set(subs_str, json.dumps(channels))

    @classmethod
    def delete_user_subscriber(cls, user_id, task_id):
        """
        удаляет канал из каналов для сокетов
        """
        subs_str = RedisCacheKeys.get_user_subscribers(user_id)
        subscribes = cls.get_user_subscribers(user_id)
        if subscribes:
            subscribers = json.loads(subscribes)
        else:
            subscribers = []
        for sub in subscribers:
            if sub['queue_id'] == task_id:
                subscribers.remove(sub)
                break
        r_server.set(subs_str, json.dumps(subscribers))

    @staticmethod
    def get_queue_dict(task_id):
        """
        информация о ходе работы таска
        :param task_id:
        """
        queue_str = RedisCacheKeys.get_queue(task_id)
        return RedisDict(key=queue_str, redis=r_server, pickler=json)

    @staticmethod
    def delete_queue(task_id):
        """
        информация о ходе работы таска
        :param task_id:
        """
        queue_str = RedisCacheKeys.get_queue(task_id)
        r_server.delete(queue_str)

    @classmethod
    def get_collection_counter(cls, source_key):
        """
        порядковый номер коллекции юзера

        Args:
            source_key(str): Базовая часть ключа

        Returns:
            dict:
        """
        counter_str = RedisCacheKeys.get_user_collection_counter(source_key)
        if not r_server.exists(counter_str):
            r_server.set(counter_str, json.dumps({
                'data': [],
                'next_id': 1,
            }))
        return cls.r_get(counter_str)

    @classmethod
    def get_card_builder(cls, card_id):
        """
        Строительная карта дерева
        """
        card_key = RKeys.get_card_key(card_id)
        card_builder = RKeys.get_card_builder(card_key)

        if not r_server.exists(card_builder):
            builder = {
                'data': {},
                'next_id': 1,
            }
            cls.r_set(card_builder, builder)
            return builder
        return cls.r_get(card_builder)

    @classmethod
    def set_card_builder(cls, card_id, actives):
        """
        """
        card_key = RKeys.get_card_key(card_id)
        card_builder = RKeys.get_card_builder(card_key)
        return cls.r_set(card_builder, actives)

    @classmethod
    def get_card_builder_data(cls, card_id):
        """
        """
        builder = cls.get_card_builder(card_id)
        return builder['data']

    @classmethod
    def get_active_table_list(cls, source_key):
        """
        Возвращает список коллекций юзера
        Args:
            source_key(str): Строка коллекции
        :param source_id: int
        :return:
        """
        return cls.get_collection_counter(source_key)['data']

    @classmethod
    def save_good_error_joins(
            cls, card_id, left_table, left_sid, right_table,
            right_sid, good_joins, error_joins, join_type):
        """
        Сохраняет временные ошибочные и нормальные джойны таблиц
        """
        card_key = RKeys.get_card_key(card_id)
        builder_str = RKeys.get_card_builder(card_key)
        str_joins = RKeys.get_source_joins(card_key)

        r_joins = cls.r_get(str_joins)

        if left_table in r_joins:
            # старые связи таблицы папы
            old_left_joins = r_joins[left_table]
            # меняем связи с right_table, а остальное оставляем
            r_joins[left_table] = [j for j in old_left_joins
                                   if j['right']['table'] != right_table and
                                   int(j['right']['sid']) != right_sid]
        else:
            r_joins[left_table] = []

        for j in good_joins:
            l_c, j_val, r_c = j
            r_joins[left_table].append(
                {
                    'left': {'table': left_table, 'column': l_c,
                             'sid': left_sid, },
                    'right': {'table': right_table, 'column': r_c,
                              'sid': right_sid, },
                    'join': {'type': join_type, 'value': j_val},
                }
            )

        if error_joins:
            for j in error_joins:
                l_c, j_val, r_c = j
                r_joins[left_table].append(
                    {
                        'left': {'table': left_table, 'column': l_c,
                                 'sid': left_sid, },
                        'right': {'table': right_table, 'column': r_c,
                                  'sid': right_sid, },
                        'join': {'type': join_type, 'value': j_val},
                        'error': 'types mismatch'
                    }
                )
        r_server.set(str_joins, json.dumps(r_joins))

        return {'has_error_joins': bool(error_joins), }

    @classmethod
    def get_source_joins(cls, source_key):
        # FIXME: к удалению ?
        str_joins = RedisCacheKeys.get_source_joins(source_key)
        return json.loads(r_server.get(str_joins))

    @classmethod
    def get_last_remain(cls, source_key):
        tables_remain_key = RedisCacheKeys.get_source_remain(source_key)
        # если имя таблицы кириллица, то в юникод преобразуем
        return (r_server.get(tables_remain_key).decode('utf8')
                if r_server.exists(tables_remain_key) else None)

    @classmethod
    def remove_tree(cls, card_id):
        """
        Удаляет дерево из хранилища
        """
        card_key = RedisCacheKeys.get_card_key(card_id)
        tree_key = RedisCacheKeys.get_active_tree(card_key)
        cls.r_del(tree_key)

    @classmethod
    def get_source_indentation(cls, source_id):
        """
        Достаем отступ для страницы соурса
        """
        indent_key = RKeys.get_indent_key(source_id)

        if not cls.r_exists(indent_key):
            return defaultdict(int)

        return defaultdict(int, cls.r_get(indent_key))

    @classmethod
    def set_source_indentation(cls, source_id, indents):
        """
        Сохраняем отступ для страницы соурса
        """
        indent_key = RKeys.get_indent_key(source_id)
        cls.r_set(indent_key, indents)


class CacheService(object):
    """
    Работа с кэш
    """
    def __init__(self, card_id):
        """
        Args:
            card_id(int): id карточки
        """
        self.card_id = card_id
        self.cache_keys = RedisCacheKeys

    def check_table_in_builder_remains(self, sid, table):
        """
        Проверка наличия узла в builder
        Args:
            sid(int): id источника
            table(unicode): название таблицы

        Returns:
            int: id узла
        """
        builder_data = self.card_builder_data
        s_id = str(sid)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            # таблица не должна быть в активных
            if table in source_colls['actives']:
                raise Exception("Table must be in remains, but it's in actives")
            # таблица уже в остатках
            return source_colls['remains'].get(table, None)
        return None

    @property
    def card_builder_data(self):
        """
        """
        return self.card_builder['data']

    @property
    def card_builder(self):
        """
        Строительная карта дерева
        """
        card_key = RKeys.get_card_key(self.card_id)
        card_builder = RKeys.get_card_builder(card_key)

        if not r_server.exists(card_builder):
            builder = {
                'data': {},
                'next_id': 1,
            }
            self.r_set(card_builder, builder)
            return builder
        return self.r_get(card_builder)

    @staticmethod
    def r_get(name, params=None):
        if params:
            return json.loads(r_server.get(name.format(*params)))
        else:
            return json.loads(r_server.get(name))

    @staticmethod
    def r_set(name, structure):
        r_server.set(name, json.dumps(structure, cls=CustomJsonEncoder))

    def fill_cache(self, source_id, table, info):
        """
        Заполняем кэш данными для узла
        Args:
            source_id(int): id источника
            table(unicode): Название таблицы, связанной с узлом
            info(dict): Информация об узле

        Returns:
            int: id узла
        """

        builder = self.card_builder
        next_id = builder['next_id']
        b_data = builder['data']

        if source_id not in b_data:
            b_data[source_id] = {
                'actives': {},
                'remains': {table: next_id},
            }
        else:
            b_data[source_id]['remains'][table] = next_id

        builder['next_id'] = next_id + 1

        # save builder's new state
        self.set_card_builder(builder)
        self.set_table_info(source_id, next_id, info)

        return next_id

    def set_card_builder(self, actives):
        """

        Args:
            actives:

        Returns:

        """
        card_key = RKeys.get_card_key(self.card_id)
        card_builder = RKeys.get_card_builder(card_key)
        return self.r_set(card_builder, actives)

    def get_table_info(self, source_id, table_id):
        str_table = self.cache_keys.get_active_table(self.card_id, source_id, table_id)
        return self.r_get(str_table)

    def set_table_info(self, source_id, table_id, table_info):
        """

        Args:
            source_id:
            table_id:
            table_info:

        Returns:

        """
        str_table = self.cache_keys.get_active_table(self.card_id, source_id, table_id)
        return self.r_set(str_table, table_info)

    @property
    def active_tree_structure(self):
        """
        Получение текущей структуры дерева источника

        Returns:
            unicode
        """
        card_key = RKeys.get_card_key(self.card_id)
        str_active_tree = RKeys.get_active_tree(card_key)
        return self.r_get(str_active_tree)

    def tables_info_for_metasource(self, tables):
        """
        Достает инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta

        Args:
            tables(dict): Список таблиц с привязкой к источнику
            ::
                {
                    1: ['table_1', 'table_2'],
                    2: ['table_3', 'table_4'],
                }

        Returns:
            dict: Информация о таблицах

        """
        tables_info = defaultdict(dict)
        builder_data = self.card_builder_data

        for sid, table_list in tables.iteritems():
            sid_format = str(sid)
            collections = builder_data[sid_format]

            for table in table_list:
                table_id = collections['actives'][table]
                table_info = self.get_table_info(sid, table_id)
                tables_info[sid][table] = table_info

        return tables_info

    def tree_full_clean(self):
        """ удаляет информацию о таблицах, джоинах, дереве
            из редиса
        """
        card_key = RKeys.get_card_key(self.card_id)
        str_actives = RKeys.get_card_builder(card_key)
        str_joins = RKeys.get_source_joins(card_key)
        str_active_tree = RKeys.get_active_tree(card_key)

        # delete keys in redis
        pipe = r_server.pipeline()

        actives = self.card_builder_data
        for f_sid in actives:
            sid = f_sid[1:]
            t_names = (actives[f_sid]['actives'].values() +
                       actives[f_sid]['remains'])

            for t_id in t_names:
                source_key = RKeys.get_user_datasource(sid)
                table_str = RedisCacheKeys.get_active_table(
                    self.card_id, source_key, t_id)
                pipe.delete(table_str)

        pipe.delete(str_actives)
        pipe.delete(str_joins)
        pipe.delete(str_active_tree)
        pipe.execute()

    def get_columns_for_joins(self, parent_table, parent_sid,
                              child_table, child_sid):
        """
        """
        actives = self.card_builder_data

        of_parent = actives[str(parent_sid)]
        of_child = actives[str(child_sid)]

        # определяем инфа по таблице в дереве или в остатках
        if parent_table in of_parent['actives']:
            par_table_id = of_parent['actives'][parent_table]
        else:
            par_table_id = of_parent['remains'][parent_table]

        if child_table in of_child['actives']:
            ch_table_id = of_child['actives'][child_table]
        else:
            ch_table_id = of_child['remains'][child_table]

        par_cols = self.get_table_info(
            parent_sid, par_table_id)['columns']
        ch_cols = self.get_table_info(
            child_sid, ch_table_id)['columns']

        return {
            parent_table: [x['name'] for x in par_cols],
            child_table: [x['name'] for x in ch_cols],
        }

    def info_for_tree_building(self, ordered_nodes, node):
        """
        информация по таблицам для построения дерева
        """
        final_info = {}
        b_data = self.card_builder_data

        # инфа таблиц из существующего дерева
        for child in ordered_nodes:
            node_id = child.node_id
            t_name = child.val
            sid = str(child.source_id)

            if sid not in b_data:
                raise Exception(u'Информация о таблцие не найдена!')

            collections = b_data[sid]
            # коллекция, участвует в дереве
            if t_name not in collections['actives']:
                raise Exception(u'Информация о таблцие не найдена!')

            # достаем по порядковому номеру
            final_info[int(node_id)] = (
                self.get_table_info(sid, node_id)
            )
        # информация таблицы, которую хотим забиндить
        node_id, sid = node.node_id, node.source_id

        final_info[int(node_id)] = (
            self.get_table_info(sid, node_id)
        )

        return final_info
