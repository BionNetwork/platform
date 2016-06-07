# coding: utf-8
from __future__ import unicode_literals

from . import r_server
import json
from copy import deepcopy
from django.conf import settings
from collections import defaultdict
from redis_collections import Dict as RedisDict
from core.helpers import CustomJsonEncoder


# FIXME описать
S = "S{0}"
T_S = "T{0}_S{1}"


class RedisCacheKeys(object):
    """Ключи для редиса"""

    @staticmethod
    def get_user_datasource(user_id, datasource_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'user_datasource:{0}:{1}'.format(user_id, datasource_id)

    @staticmethod
    def get_user_card_key(user_id, card_id=1):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return u'user_card:{0}:{1}'.format(user_id, card_id)

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
    def get_user_card_builder(card_key):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param card_key: str
        :return: str
        """
        return u'{0}:builder'.format(card_key)

    @staticmethod
    def get_active_table(source_key, number):
        """
        фулл инфа таблицы, которая в дереве
        :param user_id:
        :param datasource_id:
        :param number:
        :return:
        """
        return u'{0}:collection:{1}'.format(source_key, number)

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


RKeys = RedisCacheKeys


class RedisSourceService(object):
    """
        Сервис по работе с редисом
    """

    @staticmethod
    def r_get(template, params=None):
        if params:
            return json.loads(r_server.get(template.format(*params)))
        else:
            return json.loads(r_server.get(template))

    @staticmethod
    def r_set(name, structure):
        r_server.set(name, json.dumps(structure))

    @staticmethod
    def r_del(name):
        r_server.delete(name)

    @staticmethod
    def get_user_source(source):
        return RedisCacheKeys.get_user_datasource(source.user_id, source.id)

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
    def set_tables(cls, source, tables):
        """
        кладем информацию о таблицах в редис
        :param source: Datasource
        :param tables: list
        :return: list
        """
        user_datasource_key = cls.get_user_source(source)

        r_server.set(user_datasource_key,
                     json.dumps({'tables': tables}))
        r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)

    @classmethod
    def delete_tables(cls, source, tables):
        """
        удаляет информацию о таблицах

        Args:
            source(Datasource): объект Источника
            tables(list): Список названий таблиц
        """
        rck = RedisCacheKeys
        source_key = cls.get_user_source(source)
        # str_table = rck.get_active_table(source_key, '{0}')
        # str_table_ddl = rck.get_active_table_ddl(source_key, '{0}')
        str_table_by_name = rck.get_active_table(source_key, '{0}')
        str_joins = rck.get_source_joins(source_key)

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

        # FIXME раньше удалялась о таблицах, сейчас оставляем
        # counter_str = RedisCacheKeys.get_user_collection_counter(
        #     source.user_id, source.id)
        # coll_counter = json.loads(
        #     cls.get_collection_counter(source.user_id, source.id))
        # actives = coll_counter['data']
        # удаляем полную инфу пришедших таблиц
        # cls.delete_tables_info(tables, actives, str_table, str_table_ddl)
        # r_server.set(counter_str, json.dumps(coll_counter))

    @classmethod
    def delete_tables_NEW(cls, user_id, tables):
        """
        удаляет информацию о таблицах

        Args:
            source(Datasource): объект Источника
            tables(list): Список названий таблиц
        """
        card_key = RKeys.get_user_card_key(user_id)
        str_actives = RKeys.get_user_card_builder(card_key)
        str_joins = RKeys.get_source_joins(card_key)

        actives = cls.get_card_actives(card_key)
        actives_data = actives['data']

        # если есть, то удаляем таблицу без связей
        for (t_name, sid) in tables:
            f_sid = S.format(sid)
            source_key = RKeys.get_user_datasource(user_id, sid)
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
        cls.initial_delete_joins_NEW(tables, joins)
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
        for v in joins.values():
            for j in v[:]:
                if j['right']['table'] in tables:
                    v.remove(j)

    @classmethod
    def initial_delete_joins_NEW(cls, tables, joins):
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

    @classmethod
    def get_table_info(cls, table_id_or_name, user_id, source_id):
        source_key = RKeys.get_user_datasource(user_id, source_id)
        str_table = RKeys.get_active_table(source_key, table_id_or_name)
        return cls.r_get(str_table)

    @classmethod
    def set_table_info(cls, table_id_or_name, user_id, source_id, table_info):
        source_key = RKeys.get_user_datasource(user_id, source_id)
        str_table = RKeys.get_active_table(source_key, table_id_or_name)
        return cls.r_set(str_table, table_info)

    @classmethod
    def del_table_info(cls, table_id_or_name, user_id, source_id):
        source_key = RKeys.get_user_datasource(user_id, source_id)
        str_table = RKeys.get_active_table(source_key, table_id_or_name)
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
    def already_table_in_redis(cls, source, table):
        """
        Проверяет таблица в редисе или нет
        Args:
            source(Datasource): объект источника
            table(str): таблица
        Returns:
            bool
        """
        card_key = RKeys.get_user_card_key(source.user_id)
        actives = cls.get_card_actives_data(card_key)

        s_id = S.format(source.id)

        if s_id in actives:
            source_colls = actives[s_id]
            # таблица уже в дереве или в остатках
            return (table in source_colls['actives'] or
                    table in source_colls['remains'])

        return False

    @classmethod
    def check_tree_exists(cls, source):
        """
        Проверяет существование дерева

        Args:
            source(Datasource): источник

        Returns:
            bool: Наличие 'user_datasource:<user_id>:<source_id>:active:tree'
        """
        source_key = cls.get_user_source(source)
        str_active_tree = RedisCacheKeys.get_active_tree(source_key)

        return r_server.exists(str_active_tree)

    @classmethod
    def check_tree_exists_NEW(cls, card_id):
        """
        Проверяет существование дерева

        Args:
            source(Datasource): источник

        Returns:
            bool: Наличие 'user_datasource:<user_id>:<source_id>:active:tree'
        """
        user_card_key = RedisCacheKeys.get_user_card_key(card_id)
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
    def save_active_tree_NEW(cls, tree_structure, user_id):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        :param source: Datasource
        """
        card_key = RKeys.get_user_card_key(user_id)
        str_active_tree = RKeys.get_active_tree(card_key)

        cls.r_set(str_active_tree, tree_structure)

    # достаем структуру дерева из редиса
    @classmethod
    def get_active_tree_structure(cls, source):
        """
        Получение текущей структуры дерева источника
        :param source: Datasource
        :return:
        """
        source_key = cls.get_user_source(source)
        str_active_tree = RKeys.get_active_tree(source_key)

        return json.loads(r_server.get(str_active_tree))

    @classmethod
    def get_active_tree_structure_NEW(cls, user_id):
        """
        Получение текущей структуры дерева источника
        :param source: Datasource
        :return:
        """
        card_key = RKeys.get_user_card_key(user_id)
        str_active_tree = RKeys.get_active_tree(card_key)
        return cls.r_get(str_active_tree)

    @classmethod
    def insert_tree(cls, structure, ordered_nodes, source, update_joins=True):
        """
        сохраняем полную инфу о дереве
        :param structure:
        :param ordered_nodes:
        :param source:
        """

        source_key = cls.get_user_source(source)

        str_table = RedisCacheKeys.get_active_table(
            source_key, '{0}')
        str_table_ddl = RedisCacheKeys.get_active_table_ddl(
            source_key, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')
        str_joins = RedisCacheKeys.get_source_joins(source_key)
        counter_str = RedisCacheKeys.get_user_collection_counter(
            source_key)

        # список коллекций
        coll_counter = cls.get_collection_counter(source_key)

        # старый список коллекций
        actives = coll_counter['data']

        joins_in_redis = defaultdict(list)

        pipe = r_server.pipeline()

        for node in ordered_nodes:
            n_val = node.val
            order = cls.get_order_from_actives(n_val, actives)
            # если инфы о коллекции нет
            if order is None:

                # порядковый номер cчетчика коллекций пользователя
                sequence_id = coll_counter['next_sequence_id']

                # Получаем информацию либо по имени, либо по порядковому номеру
                table_info = json.loads(
                    cls.get_table_full_info(source, n_val))

                info_for_coll = deepcopy(table_info)
                info_for_ddl = deepcopy(table_info)

                for column in info_for_coll["columns"]:
                    del column["origin_type"]
                    # del column["is_nullable"]
                    del column["extra"]

                pipe.set(str_table.format(sequence_id), json.dumps(info_for_coll))

                for column in info_for_ddl["columns"]:
                    column["type"] = column["origin_type"]
                    del column["origin_type"]

                pipe.set(str_table_ddl.format(sequence_id), json.dumps(info_for_ddl))

                # удаляем таблицы с именованными ключами
                pipe.delete(str_table_by_name.format(n_val))
                try:
                    assert isinstance(coll_counter, dict)
                except AssertionError:
                    coll_counter = {'data': [], 'next_sequence_id': coll_counter}
                # добавляем новую таблциу в карту активных таблиц
                coll_counter['data'].append({'name': n_val, 'id': sequence_id})

                # увеличиваем счетчик
                coll_counter['next_sequence_id'] += 1

            # добавляем инфу новых джойнов
            if update_joins:
                joins = node.get_node_joins_info()
                for k, v in joins.iteritems():
                    joins_in_redis[k] += v

        pipe.set(counter_str, json.dumps(coll_counter))

        if update_joins:
            pipe.set(str_joins, json.dumps(joins_in_redis))

        pipe.execute()

        # сохраняем само дерево
        cls.save_active_tree(structure, source)

    @classmethod
    def insert_tree_NEW(cls, structure, ordered_nodes,
                        user_id, update_joins=True):
        """
        сохраняем полную инфу о дереве
        :param structure:
        :param ordered_nodes:
        :param source:
        """

        card_key = RKeys.get_user_card_key(user_id)
        builder_str = RKeys.get_user_card_builder(card_key)
        str_joins = RKeys.get_source_joins(card_key)

        # список коллекций
        coll_counter = cls.get_card_actives(card_key)
        actives = coll_counter['data']
        # порядковый номер cчетчика коллекций пользователя
        sequence_id = coll_counter['next_sequence_id']

        joins_in_redis = defaultdict(list)

        for node in ordered_nodes:
            n_val = node.val
            n_sid = node.source_id
            f_n_sid = S.format(n_sid)

            if f_n_sid not in actives:
                actives[f_n_sid] = {
                    'actives': {},
                    'remains': [],
                }

            s_actives = actives[f_n_sid]

            # суем в активные
            if n_val not in s_actives['actives']:
                n_table_info = cls.get_table_info(n_val, user_id, n_sid)

                cls.set_table_info(sequence_id, user_id, n_sid, n_table_info)

                cls.del_table_info(n_val, user_id, n_sid)

                # добавляем новую таблциу в карту активных таблиц
                s_actives['actives'][n_val] = sequence_id

                # увеличиваем счетчик
                sequence_id += 1

            # убираем из остатков
            if n_val in s_actives['remains']:
                s_actives['remains'].remove(n_val)

            # добавляем инфу новых джойнов
            if update_joins:
                joins = node.get_node_joins_info_NEW()
                for k, v in joins.iteritems():
                    joins_in_redis[k] += v

        coll_counter['next_sequence_id'] = sequence_id

        r_server.set(builder_str, json.dumps(coll_counter))

        if update_joins:
            r_server.set(str_joins, json.dumps(joins_in_redis))

        # сохраняем само дерево
        cls.save_active_tree_NEW(structure, user_id)

    @classmethod
    def tree_full_clean(cls, source, delete_ddl=True):
        """ удаляет информацию о таблицах, джоинах, дереве
            из редиса
        """
        source_key = cls.get_user_source(source)
        active_tables_key = RedisCacheKeys.get_active_tables(source_key)
        tables_joins_key = RedisCacheKeys.get_source_joins(source_key)
        tables_remain_key = RedisCacheKeys.get_source_remain(source_key)
        active_tree_key = RedisCacheKeys.get_active_tree(source_key)
        table_by_name_key = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')
        table_str = RedisCacheKeys.get_active_table(
                source_key, '{0}')
        table_str_ddl = RedisCacheKeys.get_active_table_ddl(
                source_key, '{0}')

        # delete keys in redis
        pipe = r_server.pipeline()
        remain = cls.get_last_remain(source_key)
        pipe.delete(table_by_name_key.format(remain))
        pipe.delete(tables_remain_key)

        # actives = cls.get_active_list(source.user_id, source.id)
        # for t in actives:
        #     table_str = RedisCacheKeys.get_active_table(
        #         user_id, source_id, t['order'])
        #     pipe.delete(table_str)

        pipe.delete(active_tables_key)
        pipe.delete(tables_joins_key)
        pipe.delete(active_tree_key)
        pipe.execute()

    @classmethod
    def tree_full_clean_NEW(cls, user_id):
        """ удаляет информацию о таблицах, джоинах, дереве
            из редиса
        """
        card_key = RKeys.get_user_card_key(user_id)
        str_actives = RKeys.get_user_card_builder(card_key)
        str_joins = RKeys.get_source_joins(card_key)
        str_active_tree = RKeys.get_active_tree(card_key)

        # delete keys in redis
        pipe = r_server.pipeline()

        actives = cls.get_card_actives_data(card_key)
        for f_sid in actives:
            sid = f_sid[1:]
            t_names = (actives[f_sid]['actives'].values() +
                       actives[f_sid]['remains'])

            for t_id in t_names:
                source_key = RKeys.get_user_datasource(user_id, sid)
                table_str = RedisCacheKeys.get_active_table(
                    source_key, t_id)
                pipe.delete(table_str)

        pipe.delete(str_actives)
        pipe.delete(str_joins)
        pipe.delete(str_active_tree)
        pipe.execute()

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
    def insert_last(cls, last, user_id, source_id):
        """
        сохраняет таблицу без связей
        :return:
        """
        card_key = RKeys.get_user_card_key(user_id)
        card_collections = cls.get_card_actives(card_key)
        actives = card_collections['data']

        f_sid = S.format(source_id)
        if f_sid not in actives:
            actives[f_sid] = {'actives': {}, 'remains': [last, ], }
        else:
            actives[f_sid]['remains'].append(last)

        cls.set_card_actives(card_key, card_collections)

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

    @classmethod
    def get_columns_for_tables_without_bind(
        cls, source, parent_table, without_bind_table):
        """
        колонки таблиц, которым хотим добавить джойны
        :param source:
        :param parent_table:
        :param without_bind_table:
        :return: :raise Exception:
        """
        source_key = cls.get_user_source(source)
        counter_str = RedisCacheKeys.get_user_collection_counter(
            source_key)
        str_remain = RedisCacheKeys.get_source_remain(
            source_key)
        str_table = RedisCacheKeys.get_active_table(
            source_key, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')

        err_msg = 'Истекло время хранения ключей в редисе!'

        if (not r_server.exists(counter_str) or
                not r_server.exists(str_remain)):
            raise Exception(err_msg)

        wo_bind_columns = json.loads(r_server.get(str_table_by_name.format(
            without_bind_table)))['columns']

        actives = cls.get_active_table_list(source_key)

        parent_columns = json.loads(r_server.get(str_table.format(
            cls.get_order_from_actives(parent_table, actives)
        )))['columns']

        return {
            without_bind_table: [x['name'] for x in wo_bind_columns],
            parent_table: [x['name'] for x in parent_columns],
        }

    @classmethod
    def get_columns_for_tables_with_bind(
        cls, source, parent_table, child_table):
        """
        колонки таблиц, у которых есть связи
        :param source:
        :param parent_table:
        :param child_table:
        :return: :raise Exception:
        """
        source_key = cls.get_user_source(source)
        counter_str = RedisCacheKeys.get_user_collection_counter(
            source_key)
        str_table = RedisCacheKeys.get_active_table(source_key, '{0}')
        str_joins = RedisCacheKeys.get_source_joins(source_key)

        err_msg = 'Истекло время хранения ключей!'

        if (not r_server.exists(counter_str) or
                not r_server.exists(str_joins)):
            raise Exception(err_msg)

        actives = cls.get_active_table_list(source_key)

        parent_columns = json.loads(r_server.get(str_table.format(
            cls.get_order_from_actives(parent_table, actives)
        )))['columns']

        child_columns = json.loads(r_server.get(str_table.format(
            cls.get_order_from_actives(child_table, actives)
        )))['columns']

        # exist_joins = json.loads(r_server.get(str_joins))
        # parent_joins = exist_joins[parent_table]
        # child_joins = [x for x in parent_joins if x['right']['table'] == child_table]

        return {
            child_table: [x['name'] for x in child_columns],
            parent_table: [x['name'] for x in parent_columns],
            # 'without_bind': False,
            # 'joins': child_joins,
        }

    @classmethod
    def get_columns_for_joins(cls, user_id, parent_table, parent_sid,
                              child_table, child_sid):
        """
        """
        card_key = RKeys.get_user_card_key(user_id)
        actives = cls.get_card_actives_data(card_key)

        of_parent = actives[S.format(parent_sid)]
        of_child = actives[S.format(child_sid)]

        # определяем инфа по таблице в дереве или в остатках
        if parent_table in of_parent['actives']:
            par_table_id = of_parent['actives'][parent_table]
        else:
            par_table_id = parent_table

        if child_table in of_child['actives']:
            ch_table_id = of_child['actives'][child_table]
        else:
            ch_table_id = child_table

        par_cols = cls.get_table_info(
            par_table_id, user_id, parent_sid)['columns']
        ch_cols = cls.get_table_info(
            ch_table_id, user_id, child_sid)['columns']

        return {
            parent_table: [x['name'] for x in par_cols],
            child_table: [x['name'] for x in ch_cols],
        }

    @classmethod
    def get_final_info(cls, ordered_nodes, source, last=None):
        """
        Информация о дереве для передачи на клиент

        Args:
            ordered_nodes(list): Список узлов
            source(`Datasource`): Источник
            last():

        Returns:
            list: Список словарей с информацией о дереве
            ::
                [
                    {
                        'db': 'XE',
                        'host': localhost,
                        'tname': 'EMPLOYEES',
                        'cols': [u'EMPLOYEE_ID', u'FIRST_NAME', ...],
                        'is_root': True,
                        'dest': None,
                        'without_bind': False,
                    }
                    ...
                ]
        """
        result = []
        source_key = cls.get_user_source(source)
        str_table = RedisCacheKeys.get_active_table(
            source_key, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')
        actives = cls.get_active_table_list(source_key)
        db = source.db
        host = source.host

        for ind, node in enumerate(ordered_nodes):
            n_val = node.val
            n_info = {'tname': n_val, 'db': db, 'host': host,
                      'dest': getattr(node.parent, 'val', None),
                      'is_root': not ind, 'without_bind': False,
                      }
            order = cls.get_order_from_actives(n_val, actives)
            table_info = json.loads(r_server.get(str_table.format(order)))
            n_info['cols'] = [{'col_name': x['name'],
                               'col_title': x.get('title', None), }
                              for x in table_info['columns']]
            result.append(n_info)

        if last:
            table_info = json.loads(r_server.get(str_table_by_name.format(last)))
            l_info = {'tname': last, 'db': db, 'host': host,
                      'dest': n_val, 'without_bind': True,
                      'cols': [{'col_name': x['name'],
                                'col_title': x.get('title', None), }
                               for x in table_info['columns']]
                      }
            result.append(l_info)
        return result

    @classmethod
    def get_final_info_NEW(cls, ordered_nodes, user_id, last=None):
        """
        Информация о дереве для передачи на клиент
        """
        result = []
        card_key = RKeys.get_user_card_key(user_id)
        actives = cls.get_card_actives_data(card_key)

        for ind, node in enumerate(ordered_nodes):
            n_val = node.val
            n_sid = node.source_id
            f_n_sid = S.format(n_sid)

            n_info = {'tname': n_val,
                      'source_id': n_sid,
                      'dest': getattr(node.parent, 'val', None),
                      'is_root': not ind, 'without_bind': False,
                      }
            table_id = actives[f_n_sid]['actives'][n_val]
            table_info = cls.get_table_info(table_id, user_id, n_sid)

            n_info['cols'] = [{'col_name': x['name'],
                               'col_title': x.get('title', None), }
                              for x in table_info['columns']]
            result.append(n_info)

        # if last:
        #     table_info = json.loads(
        #                   r_server.get(str_table_by_name.format(last)))
        #     l_info = {'tname': last,
        #               'dest': n_val, 'without_bind': True,
        #               'cols': [{'col_name': x['name'],
        #                         'col_title': x.get('title', None), }
        #                        for x in table_info['columns']]
        #               }
        #     result.append(l_info)
        return result

    @classmethod
    def extract_tree_from_storage(cls, card_id, ordered_nodes):
        """
        Информация о дереве для передачи на клиент
        """
        result = []
        card_key = RKeys.get_user_card_key(card_id)
        actives = cls.get_card_actives_data(card_key)

        for ind, node in enumerate(ordered_nodes):
            n_val = node.val
            n_sid = node.source_id
            f_n_sid = S.format(n_sid)

            n_info = {'tname': n_val,
                      'source_id': n_sid,
                      'dest': getattr(node.parent, 'val', None),
                      'is_root': not ind, 'without_bind': False,
                      }
            table_id = actives[f_n_sid]['actives'][n_val]
            table_info = cls.get_table_info(table_id, card_id, n_sid)

            n_info['cols'] = [{'col_name': x['name'],
                               'col_title': x.get('title', None), }
                              for x in table_info['columns']]
            result.append(n_info)

            # FIXME доделать остатки
            # remains = sel_tree.no_bind_tables

        return result

    @classmethod
    def insert_columns_info(cls, source, tables, columns,
                            indexes, foreigns, stats, intervals):
        """
        инфа о колонках, констраинтах, индексах в редис
        :param source:
        :param tables:
        :param columns:
        :param indexes:
        :param foreigns:
        :param stats:
        :return:
        """
        source_key = cls.get_user_source(source)
        str_table_by_name = RedisCacheKeys.get_active_table(
            source_key, '{0}')

        pipe = r_server.pipeline()

        for t_name in tables:
            pipe.set(str_table_by_name.format(t_name), json.dumps(
                {
                    "sid": source.id,
                    "columns": columns[t_name],
                    "indexes": indexes[t_name],
                    "foreigns": foreigns[t_name],
                    "stats": stats[t_name],
                    "date_intervals": intervals.get(t_name, [])
                }, cls=CustomJsonEncoder
            ))
        pipe.execute()

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
    def info_for_tree_building(cls, ordered_nodes, tables, source):
        """
        информация для построения дерева
        :param ordered_nodes:
        :param tables:
        :param source:
        :return:
        """
        source_key = cls.get_user_source(source)
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source_key, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            source_key, '{0}')

        actives = cls.get_active_table_list(source_key)

        final_info = {}

        # инфа таблиц из существующего дерева
        for child in ordered_nodes:
            ch_val = child.val
            order = [x for x in actives if x['name'] == ch_val][0]['id']
            final_info[ch_val] = cls.r_get(str_table, (order,))

        # инфу новых таблиц достаем либо из коллекций юзера(она там
        # будет,если он эту таблу выбирал ранее), либо из редиса через имя табла
        # инфа таблиц не из дерева
        for t_name in tables:

            table_in_active = [x for x in actives if x['name'] == t_name]
            if table_in_active:
                order = table_in_active[0]['id']
                final_info[t_name] = cls.r_get(str_table, (order,))
            elif r_server.exists(str_table_by_name.format(t_name)):
                final_info[t_name] = cls.r_get(str_table_by_name, (t_name,))
            else:
                raise Exception(u'Информация о таблцие не найдена!')

        return final_info

    @classmethod
    def info_for_tree_building_NEW(cls, ordered_nodes, table, source):
        """
        информация для построения дерева
        :param ordered_nodes:
        :param table:
        :param source:
        :return:
        """

        final_info = {}

        u_id = source.user_id
        card_key = RedisCacheKeys.get_user_card_key(u_id)
        actives = cls.get_card_actives_data(card_key)

        sid_name = u'{0}_{1}'

        # FIXME пока в нодах нет source_ID
        # инфа таблиц из существующего дерева
        for child in ordered_nodes:
            ch_name = child.val
            ch_sid = child.source_id
            sid_format = S.format(ch_sid)

            if sid_format in actives:
                collections = actives[sid_format]
                # коллекция, участвует в дереве
                if ch_name in collections['actives']:
                    # FIXME можно обойтись без пор номеров
                    # достаем по порядковому номеру
                    ch_source_key = RKeys.get_user_datasource(u_id, ch_sid)
                    ch_str_table = RKeys.get_active_table(
                        ch_source_key, collections['actives'][ch_name])
                    final_info[sid_name.format(ch_sid, ch_name)] = cls.r_get(ch_str_table)
                elif ch_name in collections['remains']:
                    # достаем по имени
                    ch_source_key = RKeys.get_user_datasource(u_id, ch_sid)
                    ch_str_table = RKeys.get_active_table(
                        ch_source_key, ch_name)
                    final_info[sid_name.format(ch_sid, ch_name)] = cls.r_get(ch_str_table)
                else:
                    raise Exception(u'Информация о таблцие не найдена!')
            else:
                raise Exception(u'Информация о таблцие не найдена!')

        source_id = source.id

        source_key = RKeys.get_user_datasource(u_id, source_id)
        str_table = RedisCacheKeys.get_active_table(
            source_key, '{0}')

        # либо валяется по имени
        if r_server.exists(str_table.format(table)):
            final_info[sid_name.format(source_id, table)] = (
                cls.r_get(str_table.format(table)))
        # либо обязан быть в коллекциях карточки
        else:
            collections = actives[S.format(source_id)]
            final_info[sid_name.format(source_id, table)] = (
                cls.r_get(str_table.format(collections['actives'][table])))

        return final_info

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

    @classmethod
    def tables_info_for_metasource(cls, source, tables):
        """
        Достает инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta
        :param source: Datasource
        :param columns: list список вида [{'table': 'name', 'col': 'name'}]
        """

        tables_info_for_meta = {}
        source_key = cls.get_user_source(source)
        str_table = RedisCacheKeys.get_active_table(source_key, '{0}')

        actives_list = cls.get_active_table_list(source_key)

        for table in tables:
            tables_info_for_meta[table] = json.loads(
                r_server.get(str_table.format(
                    cls.get_order_from_actives(table, actives_list)
                )))
        return tables_info_for_meta

    @classmethod
    def tables_info_for_metasource_NEW(cls, tables, user_id):
        """
        Достает инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta
        """

        tables_info = defaultdict(dict)
        card_key = RKeys.get_user_card_key(user_id)

        actives = cls.get_card_actives_data(card_key)

        for sid, table_list in tables.iteritems():
            sid_format = S.format(sid)
            collections = actives[sid_format]

            for table in table_list:
                table_id = collections['actives'][table]
                table_info = cls.get_table_info(table_id, user_id, sid)
                tables_info[sid][table] = table_info

        return tables_info

    @classmethod
    def get_ddl_tables_info(cls, source, tables):
        """
        Достает инфу о колонках, выбранных таблиц для cdc стратегии
        """
        data = {}
        source_key = cls.get_user_source(source)
        str_table = RedisCacheKeys.get_active_table_ddl(
            source_key, '{0}')

        actives_list = cls.get_active_table_list(source_key)

        for table in tables:
            data[table] = json.loads(
                r_server.get(str_table.format(
                    cls.get_order_from_actives(table, actives_list)
                )))['columns']
        return data

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
                'next_sequence_id': 1,
            }))
        return cls.r_get(counter_str)

    @classmethod
    def get_card_actives(cls, card_key):
        """
        порядковый номер коллекции юзера

        Args:
            source_key(str): Базовая часть ключа

        Returns:
            dict:
        """
        card_builder = RKeys.get_user_card_builder(card_key)
        if not r_server.exists(card_builder):
            builder = {
                'data': {},
                'next_sequence_id': 1,
            }
            cls.r_set(card_builder, builder)
            return builder
        return cls.r_get(card_builder)

    @classmethod
    def set_card_actives(cls, card_key, collections):
        """
        """
        card_builder = RKeys.get_user_card_builder(card_key)
        return cls.r_set(card_builder, collections)

    @classmethod
    def get_card_actives_data(cls, card_key):
        """
        """
        builder = cls.get_card_actives(card_key)
        return builder['data']

    @classmethod
    def get_table_name_or_id(cls, table, user_id, sid):
        """
        """
        card_key = RKeys.get_user_card_key(user_id)
        actives = cls.get_card_actives_data(card_key)
        s_actives = actives[S.format(sid)]

        if table in s_actives['actives']:
            return s_actives['actives'][table]

        elif table not in s_actives['remains']:
            raise Exception(u'Отсутствует информация по таблице!')

        return table

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
    def save_good_error_joins(cls, source, left_table, right_table,
                              good_joins, error_joins, join_type):
        """
        Сохраняет временные ошибочные и нормальные джойны таблиц
        :param source: Datasource
        :param joins: list
        :param error_joins: list
        """
        source_key = cls.get_user_source(source)
        str_joins = RedisCacheKeys.get_source_joins(source_key)
        r_joins = json.loads(r_server.get(str_joins))

        if left_table in r_joins:
            # старые связи таблицы папы
            old_left_joins = r_joins[left_table]
            # меняем связи с right_table, а остальное оставляем
            r_joins[left_table] = [j for j in old_left_joins
                                   if j['right']['table'] != right_table]
        else:
            r_joins[left_table] = []

        for j in good_joins:
            l_c, j_val, r_c = j
            r_joins[left_table].append(
                {
                    'left': {'table': left_table, 'column': l_c},
                    'right': {'table': right_table, 'column': r_c},
                    'join': {'type': join_type, 'value': j_val},
                }
            )

        if error_joins:
            for j in error_joins:
                l_c, j_val, r_c = j
                r_joins[left_table].append(
                    {
                        'left': {'table': left_table, 'column': l_c},
                        'right': {'table': right_table, 'column': r_c},
                        'join': {'type': join_type, 'value': j_val},
                        'error': 'types mismatch'
                    }
                )
        r_server.set(str_joins, json.dumps(r_joins))

        return {'has_error_joins': bool(error_joins), }

    @classmethod
    def save_good_error_joins_NEW(
            cls, user_id, left_table, left_sid, right_table,
            right_sid, good_joins, error_joins, join_type):
        """
        Сохраняет временные ошибочные и нормальные джойны таблиц
        :param source: Datasource
        :param joins: list
        :param error_joins: list
        """
        card_key = RKeys.get_user_card_key(user_id)
        builder_str = RKeys.get_user_card_builder(card_key)
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
    def get_good_error_joins(cls, source, parent_table, child_table):
        source_key = cls.get_user_source(source)
        r_joins = cls.get_source_joins(source_key)

        good_joins = []
        error_joins = []

        # если 2 таблицы выбраны без связей, то r_joins пустой,
        # если биндим последнюю таблицу без связи,то parent_table not in r_joins
        if r_joins and parent_table in r_joins:
            par_joins = r_joins[parent_table]
            good_joins = [
                j for j in par_joins if j['right']['table'] == child_table
                and 'error' not in j]

            error_joins = [
                j for j in par_joins if j['right']['table'] == child_table
                and 'error' in j and j['error'] == 'types mismatch']

        return good_joins, error_joins

    @classmethod
    def get_good_error_joins_NEW(cls, user_id, parent_table, parent_sid,
                                 child_table, child_sid):

        card_key = RKeys.get_user_card_key(user_id)
        r_joins = cls.get_source_joins(card_key)

        t_s = T_S.format(parent_table, parent_sid)

        good_joins = []
        error_joins = []

        # если 2 таблицы выбраны без связей, то r_joins пустой,
        # если биндим таблицу без связи,то parent_table not in r_joins
        if r_joins and t_s in r_joins:
            par_joins = r_joins[t_s]
            good_joins = [
                j for j in par_joins if j['right']['table'] == child_table and
                int(j['right']['sid']) == child_sid and 'error' not in j]

            error_joins = [
                j for j in par_joins if j['right']['table'] == child_table and
                int(j['right']['sid']) == child_sid and
                'error' in j and j['error'] == 'types mismatch']

        return good_joins, error_joins

    @classmethod
    def get_source_joins(cls, source_key):
        str_joins = RedisCacheKeys.get_source_joins(source_key)
        return json.loads(r_server.get(str_joins))

    @classmethod
    def get_last_remain(cls, source_key):
        tables_remain_key = RedisCacheKeys.get_source_remain(source_key)
        # если имя таблицы кириллица, то в юникод преобразуем
        return (r_server.get(tables_remain_key).decode('utf8')
                if r_server.exists(tables_remain_key) else None)


# FIXME не используется на данный момент
class RedisStorage:
    """
    Обертка над методами сохранения информации в redis
    Позволяет работать с объектами в python стиле, при этом информация сохраняется в redis
    Пока поддерживаются словари
    """
    def __init__(self, client):
        self.client = client

    def set_dict(self, redis_key, key, value):
        tasks = RedisDict(key=redis_key, redis=self.client, pickler=json)
        tasks[key] = value

    def get_dict(self, key):
        tasks = RedisDict(key=key, redis=self.client, pickler=json)
        return tasks
