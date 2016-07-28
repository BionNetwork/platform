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

    @classmethod
    def get_card_builder(cls, card_id):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param card_key: str
        :return: str
        """
        card_key = cls.get_card_key(card_id)
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

    @classmethod
    def get_active_tree(cls, card_id):
        """
        Структура дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        card_key = cls.get_card_key(card_id)
        return u'{0}:active:tree'.format(card_key)

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

    # FIXME to delete
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


# TODO можно сделать SourceCacheService и передавать ему source_id в ините
class CardCacheService(object):
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

    def get_table_id(self, source_id, table, data=None):
        """
        Проверяет таблица уже в кэше или нет!
        Возвращает id таблицы или None в случае отсутствия!
        Если вызывается в цикле, то лучше передавать data,
        во избежании лишних вводов-выводов
        """
        builder_data = data or self.card_builder_data
        s_id = str(source_id)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            # таблица не должна быть в активных
            if table in source_colls['actives']:
                return source_colls['actives'][table]
            elif table in source_colls['remains']:
                return source_colls['remains'][table]
        return None

    def check_table_in_actives(self, source_id, table):
        """
        Проверяет таблица в активных или нет!
        """
        builder_data = self.card_builder_data
        s_id = str(source_id)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            return table in source_colls['actives']

        return False

    def check_table_in_remains(self, source_id, table):
        """
        Проверяет таблица в остатках или нет!
        """
        builder_data = self.card_builder_data
        s_id = str(source_id)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            return table in source_colls['remains']

        return False

    @property
    def card_builder(self):
        """
        Строительная карта дерева
        """
        builder_key = self.cache_keys.get_card_builder(self.card_id)

        if not self.r_exists(builder_key):
            builder = {
                'data': {},
                'next_id': 1,
            }
            self.r_set(builder_key, builder)
            return builder
        return self.r_get(builder_key)

    def set_card_builder(self, actives):
        """
        Args:
            actives object has structure {
                'data': {},
                'next_id': 1,
            }
        Returns:
        """
        builder_key = self.cache_keys.get_card_builder(self.card_id)
        return self.r_set(builder_key, actives)

    @property
    def card_builder_data(self):
        """
        """
        return self.card_builder['data']

    @staticmethod
    def r_get(name, params=None):
        if params:
            return json.loads(r_server.get(name.format(*params)))
        else:
            return json.loads(r_server.get(name))

    @staticmethod
    def r_set(name, structure):
        r_server.set(name, json.dumps(structure, cls=CustomJsonEncoder))

    @staticmethod
    def r_del(name):
        r_server.delete(name)

    @staticmethod
    def r_exists(name):
        return r_server.exists(name)

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

        self.set_table_info(source_id, next_id, info)

        source_id = str(source_id)
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

        return next_id

    def get_table_info(self, source_id, table_id):
        table_key = self.cache_keys.get_active_table(
            self.card_id, source_id, table_id)
        return self.r_get(table_key)

    def set_table_info(self, source_id, table_id, table_info):
        """
        Args:
            source_id:
            table_id:
            table_info:
        Returns:
        """
        table_key = self.cache_keys.get_active_table(
            self.card_id, source_id, table_id)
        return self.r_set(table_key, table_info)

    @property
    def active_tree_structure(self):
        """
        Получение текущей структуры дерева источника
        Returns:
            unicode
        """
        tree_key = self.cache_keys.get_active_tree(self.card_id)
        return self.r_get(tree_key)

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
        cache_keys = self.cache_keys
        card_id = self.card_id

        builder_key = cache_keys.get_card_builder(card_id)
        tree_key = cache_keys.get_active_tree(card_id)

        # delete keys in redis in transaction
        pipe = r_server.pipeline()

        builder_data = self.card_builder_data

        for sid, bundle in builder_data.iteritems():
            t_names = bundle['actives'].values() + bundle['remains'].values()

            for table_id in t_names:
                table_key = cache_keys.get_active_table(
                    card_id, sid, table_id)

                pipe.delete(table_key)

        pipe.delete(builder_key)
        pipe.delete(tree_key)
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

    def check_tree_exists(self):
        """
        Проверяет существование дерева

        Args:
            card_id(int): id карточки
        Returns:
            bool: Наличие 'user_datasource:<user_id>:<source_id>:active:tree'
        """
        tree_key = self.cache_keys.get_active_tree(self.card_id)
        return self.r_exists(tree_key)

    def transfer_remain_to_actives(self, node):
        """
        сохраняем карту дерева, перенос остатка в активные
        """
        builder = self.card_builder
        b_data = builder['data']

        table, sid, node_id = node.val, str(node.source_id), node.node_id

        s_remains = b_data[sid]['remains']
        s_actives = b_data[sid]['actives']

        # FIXME проверку перенести отсюда
        if table not in s_remains:
            raise Exception("Table not in remains!")
        if s_remains[table] != node_id:
            raise Exception("Table ID is broken!")
        if table in s_actives:
            raise Exception("Table already in actives!")

        del s_remains[table]
        s_actives[table] = node_id

        self.set_card_builder(builder)

    def save_tree_structure(self, tree):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        :param source: Datasource
        """
        tree_key = self.cache_keys.get_active_tree(self.card_id)
        tree_structure = tree.structure

        self.r_set(tree_key, tree_structure)

    def remove_tree(self):
        """
        Удаляет дерево из хранилища
        """
        tree_key = self.cache_keys.get_active_tree(self.card_id)
        self.r_del(tree_key)

    def transfer_actives_to_remains(self, to_remain_nodes):
        """
        сохраняем карту дерева, перенос активов в остатки
        """
        builder = self.card_builder
        b_data = builder['data']

        for node in to_remain_nodes:
            table, sid, node_id = (
                node.val, str(node.source_id), node.node_id)

            s_remains = b_data[sid]['remains']
            s_actives = b_data[sid]['actives']

            del s_actives[table]
            s_remains[table] = node_id

        self.set_card_builder(builder)
