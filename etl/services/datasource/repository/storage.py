# coding: utf-8


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
    def source_key(datasource_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return 'source:{0}'.format(datasource_id)

    @staticmethod
    def get_card_key(card_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return 'card:{0}'.format(card_id)

    @classmethod
    def card_builder_key(cls, card_id):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param card_key: str
        :return: str
        """
        card_key = cls.get_card_key(card_id)
        return '{0}:builder'.format(card_key)

    @classmethod
    def table_key(cls, card_id, source_id, table_id):
        """
        фулл инфа таблицы, которая в дереве
        """
        card_key = cls.get_card_key(card_id)
        source_key = cls.source_key(source_id)
        return '{0}:{1}:collection:{2}'.format(
            card_key, source_key, table_id)

    @classmethod
    def tree_key(cls, card_id):
        """
        Структура дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        card_key = cls.get_card_key(card_id)
        return '{0}:active:tree'.format(card_key)

    @staticmethod
    def get_user_subscribers(user_id):
        """
        ключ каналов юзера для сокетов
        """
        return 'user_channels:{0}'.format(user_id)

    @staticmethod
    def get_queue(task_id):
        """
        ключ информации о ходе работы таска
        """
        return 'queue:{0}'.format(task_id)

    @classmethod
    def indent_key(cls, source_id):
        """
        Ключ отступа
        """
        source_key = cls.source_key(source_id)
        return '{0}:indent'.format(source_key)

RKeys = RedisCacheKeys


class RedisSourceService(object):
    """
        Сервис по работе с редисом
    """
    # FIXME r_get r_set r_del r_exists дублируютс с CardCacheService, предка впаять
    @staticmethod
    def r_get(name):
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
        return RKeys.source_key(source_id)

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
    def get_source_indentation(cls, source_id):
        """
        Достаем отступ для страницы соурса
        Returns: defaultdict(int)
        """
        indent_key = RKeys.indent_key(source_id)

        if not cls.r_exists(indent_key):
            return defaultdict(int)

        return defaultdict(int, cls.r_get(indent_key))

    @classmethod
    def set_source_indentation(cls, source_id, indents):
        """
        Сохраняем отступ для страницы соурса
        """
        indent_key = RKeys.indent_key(source_id)
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
        builder_key = self.cache_keys.card_builder_key(self.card_id)

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
        builder_key = self.cache_keys.card_builder_key(self.card_id)
        return self.r_set(builder_key, actives)

    def del_card_builder(self):
        """
        Удаляет строительную карта карточки
        """
        builder_key = self.cache_keys.card_builder_key(self.card_id)
        return self.r_del(builder_key)

    @property
    def card_builder_data(self):
        """
        """
        return self.card_builder['data']

    @staticmethod
    def r_get(name):
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
        table_key = self.cache_keys.table_key(
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
        table_key = self.cache_keys.table_key(
            self.card_id, source_id, table_id)
        return self.r_set(table_key, table_info)

    def del_table_info(self, source_id, table_id):
        """
        Удаляет из кэша инфу о таблице источника
        """
        table_key = self.cache_keys.table_key(
            self.card_id, source_id, table_id)

        self.r_del(table_key)

    def del_all_tables_info(self):
        """
        Удаляет из кэша инфу всех таблиц источника,
        участвующих в карточке
        """
        builder_data = self.card_builder_data
        for sid, bundle in builder_data.items():
            t_names = list(bundle['actives'].values()) + list(bundle['remains'].values())

            for table_id in t_names:
                self.del_table_info(sid, table_id)

    @property
    def active_tree_structure(self):
        """
        Получение текущей структуры дерева источника
        Returns:
            unicode
        """
        tree_key = self.cache_keys.tree_key(self.card_id)
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

        for sid, table_list in tables.items():
            sid_format = str(sid)
            collections = builder_data[sid_format]

            for table in table_list:
                table_id = collections['actives'][table]
                table_info = self.get_table_info(sid, table_id)
                tables_info[sid][table] = table_info

        return tables_info

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
                raise Exception('Информация о таблцие не найдена!')

            collections = b_data[sid]
            # коллекция, участвует в дереве
            if t_name not in collections['actives']:
                raise Exception('Информация о таблцие не найдена!')

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
        tree_key = self.cache_keys.tree_key(self.card_id)
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
        tree_key = self.cache_keys.tree_key(self.card_id)
        tree_structure = tree.structure

        self.r_set(tree_key, tree_structure)

    def remove_tree(self):
        """
        Удаляет дерево из хранилища
        """
        tree_key = self.cache_keys.tree_key(self.card_id)
        self.r_del(tree_key)

    def clear_card_cache(self):
        """
        Удаляет из редиса полную инфу о карточке
        """
        self.remove_tree()
        self.del_all_tables_info()
        self.del_card_builder()

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
