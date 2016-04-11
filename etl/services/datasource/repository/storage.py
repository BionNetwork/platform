# coding: utf-8
from . import r_server
import json
from copy import deepcopy
from django.conf import settings
from collections import defaultdict
from redis_collections import Dict as RedisDict, List as RedisList
from core.helpers import CustomJsonEncoder


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
        return 'user_datasource:{0}:{1}'.format(user_id, datasource_id)

    @staticmethod
    def get_user_collection_counter(user_id, datasource_id):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        :param user_id: int
        :param datasource_id: int
        :return: str
        """
        return '{0}:{1}'.format(RedisCacheKeys.get_user_datasource(
            user_id, datasource_id), 'counter')

    @staticmethod
    def get_active_table(user_id, datasource_id, number):
        """
        фулл инфа таблицы, которая в дереве
        :param user_id:
        :param datasource_id:
        :param number:
        :return:
        """
        return '{0}:collection:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), number)

    @staticmethod
    def get_active_table_ddl(user_id, datasource_id, number):
        """
        фулл инфа таблицы, которая в дереве для ddl
        :param user_id:
        :param datasource_id:
        :param number:
        :return:
        """
        return '{0}:ddl:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), number)

    @staticmethod
    def get_active_tables(user_id, datasource_id):
        """
        список таблиц из дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:active_collections'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_active_table_by_name(user_id, datasource_id, table):
        """
        фулл инфа таблицы, которая НЕ в дереве
        :param user_id:
        :param datasource_id:
        :param table:
        :return:
        """
        return '{0}:collection:{1}'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id), table)

    @staticmethod
    def get_source_joins(user_id, datasource_id):
        """
        инфа о джоинах дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:joins'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_source_remain(user_id, datasource_id):
        """
        таблица без связей
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:remain'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

    @staticmethod
    def get_active_tree(user_id, datasource_id):
        """
        структура дерева
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:active:tree'.format(
            RedisCacheKeys.get_user_datasource(user_id, datasource_id))

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


class RedisSourceService(object):
    """
        Сервис по работе с редисом
    """

    @classmethod
    def delete_datasource(cls, source):
        """
        удаляет информацию о датасосре из редиса
        :param cls:
        :param source: Datasource
        """
        user_datasource_key = RedisCacheKeys.get_user_datasource(
            source.user_id, source.id)

        r_server.delete(user_datasource_key)

    @classmethod
    def set_tables(cls, source, tables):
        """
        кладем информацию о таблицах в редис
        :param source: Datasource
        :param tables: list
        :return: list
        """
        user_datasource_key = RedisCacheKeys.get_user_datasource(
            source.user_id, source.id)

        r_server.set(user_datasource_key,
                     json.dumps({'tables': tables}))
        r_server.expire(user_datasource_key, settings.REDIS_EXPIRE)

    @classmethod
    def delete_tables(cls, source, tables):
        """
        удаляет инфу о таблицах
        :param source: Datasource
        :param tables: list
        """
        rck = RedisCacheKeys

        str_table = rck.get_active_table(source.user_id, source.id, '{0}')
        str_table_ddl = rck.get_active_table_ddl(source.user_id, source.id, '{0}')
        str_table_by_name = rck.get_active_table(source.user_id, source.id, '{0}')
        str_joins = rck.get_source_joins(source.user_id, source.id)

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

        # FIXME раньше удалялись инфы о таблах, ща оставляем
        # counter_str = RedisCacheKeys.get_user_collection_counter(
        #     source.user_id, source.id)
        # coll_counter = json.loads(
        #     cls.get_collection_counter(source.user_id, source.id))
        # actives = coll_counter['data']
        # удаляем полную инфу пришедших таблиц
        # cls.delete_tables_info(tables, actives, str_table, str_table_ddl)
        # r_server.set(counter_str, json.dumps(coll_counter))

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
    def delete_tables_info(cls, tables, actives, str_table, str_table_ddl):
        """
        удаляет инфу о таблицах
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
            source(`Datasource`): Объект источника
            table(str): Название таблицы

        Returns:
            str: Название коллекции
        """

        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')

        active_tables = cls.get_active_table_list(source.user_id, source.id)

        if r_server.exists(str_table_by_name.format(table)):
            return str_table_by_name.format(table)
        else:
            order = [x for x in active_tables if x['name'] == table][0]['id']
            return str_table.format(order)

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
    def filter_exists_tables(cls, source, tables):
        """
        Возвращает таблицы, которых нет в редисе в ранее выбранных коллекциях
        """
        coll_counter = json.loads(cls.get_collection_counter(
            source.user_id, source.id))
        # список коллекций
        try:
            assert isinstance(coll_counter, dict)
            actives_names = [x['name'] for x in coll_counter['data']]
        except AssertionError:
            actives_names = []
        not_exists = [t for t in tables if t not in actives_names]
        return not_exists, actives_names

    @staticmethod
    def check_tree_exists(user_id, source_id):
        """
        Проверяет существование дерева
        """
        str_active_tree = RedisCacheKeys.get_active_tree(user_id, source_id)

        return r_server.exists(str_active_tree)

    @classmethod
    def save_active_tree(cls, tree_structure, source):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        :param source: Datasource
        """
        str_active_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)

        r_server.set(str_active_tree, json.dumps(tree_structure))

    # достаем структуру дерева из редиса
    @classmethod
    def get_active_tree_structure(cls, source):
        """
        Получение текущей структуры дерева источника
        :param source: Datasource
        :return:
        """
        str_active_tree = RedisCacheKeys.get_active_tree(
            source.user_id, source.id)

        return json.loads(r_server.get(str_active_tree))

    @classmethod
    def insert_tree(cls, structure, ordered_nodes, source, update_joins=True):
        """
        сохраняем полную инфу о дереве
        :param structure:
        :param ordered_nodes:
        :param source:
        """

        user_id, source_id = source.user_id, source.id

        str_table = RedisCacheKeys.get_active_table(
            user_id, source.id, '{0}')
        str_table_ddl = RedisCacheKeys.get_active_table_ddl(
            user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            user_id, source_id, '{0}')
        str_joins = RedisCacheKeys.get_source_joins(
            user_id, source_id)
        counter_str = RedisCacheKeys.get_user_collection_counter(
            user_id, source_id)

        # список коллекций
        coll_counter = json.loads(cls.get_collection_counter(
                    user_id, source_id))

        # старый список коллекций
        try:
            assert isinstance(coll_counter, dict)
            actives = coll_counter['data']
        except AssertionError:
            actives = []

        joins_in_redis = defaultdict(list)

        pipe = r_server.pipeline()

        for node in ordered_nodes:
            n_val = node.val
            order = cls.get_order_from_actives(n_val, actives)
            # если инфы о коллекции нет
            if order is None:

                # порядковый номер cчетчика коллекций пользователя
                try:
                    assert isinstance(coll_counter, dict)
                    sequence_id = coll_counter['next_sequence_id']
                except AssertionError:
                    sequence_id = coll_counter

                # достаем инфу либо по имени, либо по порядковому номеру
                table_info = RedisSourceService.get_table_full_info(source, n_val)
                table_info = json.loads(table_info)

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
        RedisSourceService.save_active_tree(structure, source)

    @classmethod
    def tree_full_clean(cls, source, delete_ddl=True):
        """ удаляет информацию о таблицах, джоинах, дереве
            из редиса
        """
        user_id = source.user_id
        source_id = source.id

        active_tables_key = RedisCacheKeys.get_active_tables(
            user_id, source_id)
        tables_joins_key = RedisCacheKeys.get_source_joins(
            user_id, source_id)
        tables_remain_key = RedisCacheKeys.get_source_remain(
            user_id, source_id)
        active_tree_key = RedisCacheKeys.get_active_tree(
            user_id, source_id)
        table_by_name_key = RedisCacheKeys.get_active_table_by_name(
            user_id, source_id, '{0}')
        table_str = RedisCacheKeys.get_active_table(
                user_id, source_id, '{0}')
        table_str_ddl = RedisCacheKeys.get_active_table_ddl(
                user_id, source_id, '{0}')

        # delete keys in redis
        pipe = r_server.pipeline()
        pipe.delete(table_by_name_key.format(r_server.get(tables_remain_key)))
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
    def insert_remains(cls, source, remains):
        """
        сохраняет таблицу без связей
        :param source: Datasource
        :param remains: list
        :return:
        """
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
        """
        удаляет таблицы без связей,(все кроме первой)
        :param source: Datasource
        :param remains: list
        """
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        for t_name in remains:
            r_server.delete(str_table_by_name.format(t_name))

    @classmethod
    def delete_last_remain(cls, source):
        """
        удаляет единственную таблицу без связей
        :param source: Datasource
        """
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
        """
        колонки таблиц, которым хотим добавить джойны
        :param source:
        :param parent_table:
        :param without_bind_table:
        :return: :raise Exception:
        """

        counter_str = RedisCacheKeys.get_user_collection_counter(
            source.user_id, source.id)
        str_remain = RedisCacheKeys.get_source_remain(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')

        err_msg = 'Истекло время хранения ключей в редисе!'

        if (not r_server.exists(counter_str) or
                not r_server.exists(str_remain)):
            raise Exception(err_msg)

        wo_bind_columns = json.loads(r_server.get(str_table_by_name.format(
            without_bind_table)))['columns']

        actives = cls.get_active_table_list(source.user_id, source.id)

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
        counter_str = RedisCacheKeys.get_user_collection_counter(
            source.user_id, source.id)
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_joins = RedisCacheKeys.get_source_joins(
            source.user_id, source.id)

        err_msg = 'Истекло время хранения ключей!'

        if (not r_server.exists(counter_str) or
                not r_server.exists(str_joins)):
            raise Exception(err_msg)

        actives = cls.get_active_table_list(source.user_id, source.id)

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
        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            source.user_id, source.id, '{0}')
        actives = cls.get_active_table_list(source.user_id, source.id)
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

        str_table_by_name = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')

        pipe = r_server.pipeline()

        for t_name in tables:
            pipe.set(str_table_by_name.format(t_name), json.dumps(
                {
                    "columns": columns[t_name.lower()],
                    "indexes": indexes[t_name.lower()],
                    "foreigns": foreigns[t_name.lower()],
                    # "stats": stats[t_name.lower()],
                    "stats": [],
                    "date_intervals": intervals.get(t_name, [])
                }, cls=CustomJsonEncoder
            ))
        pipe.execute()

    @classmethod
    def insert_date_intervals(cls, source, tables, intervals):
        # сохраняем актуальный период дат у каждой таблицы
        # для каждой колонки типа дата

        u_id, s_id = source.user_id, source.id

        str_table = RedisCacheKeys.get_active_table(u_id, s_id, '{0}')
        str_table_ddl = RedisCacheKeys.get_active_table_ddl(u_id, s_id, '{0}')

        # список имеющихся коллекций
        actives = cls.get_active_table_list(u_id, s_id)
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
        инфа для построения дерева
        :param ordered_nodes:
        :param tables:
        :param source:
        :return:
        """
        user_id = source.user_id
        str_table_by_name = RedisCacheKeys.get_active_table_by_name(
            user_id, source.id, '{0}')
        str_table = RedisCacheKeys.get_active_table(
            user_id, source.id, '{0}')

        actives = cls.get_active_table_list(user_id, source.id)

        final_info = {}

        # инфа таблиц из существующего дерева
        for child in ordered_nodes:
            ch_val = child.val
            order = [x for x in actives if x['name'] == ch_val][0]['id']
            final_info[ch_val] = json.loads(r_server.get(str_table.format(order)))

        # инфу новых таблиц достаем либо из коллекций юзера(она там
        # будет,если он эту таблу выбирал ранее), либо из редиса через имя табла
        # инфа таблиц не из дерева
        for t_name in tables:

            table_in_active = [x for x in actives if x['name'] == t_name]
            if table_in_active:
                order = table_in_active[0]['id']
                final_info[t_name] = json.loads(
                    r_server.get(str_table.format(order)))
            elif r_server.exists(str_table_by_name.format(t_name)):
                final_info[t_name] = json.loads(
                    r_server.get(str_table_by_name.format(t_name)))
            else:
                raise Exception(u'Информация о таблцие не найдена!')

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

        str_table = RedisCacheKeys.get_active_table(
            source.user_id, source.id, '{0}')

        actives_list = cls.get_active_table_list(source.user_id, source.id)

        for table in tables:
            tables_info_for_meta[table] = json.loads(
                r_server.get(str_table.format(
                    RedisSourceService.get_order_from_actives(
                        table, actives_list)
                )))
        return tables_info_for_meta

    @classmethod
    def get_ddl_tables_info(cls, source, tables):
        """
        Достает инфу о колонках, выбранных таблиц для cdc стратегии
        """
        data = {}

        str_table = RedisCacheKeys.get_active_table_ddl(
            source.user_id, source.id, '{0}')

        actives_list = cls.get_active_table_list(source.user_id, source.id)

        for table in tables:
            data[table] = json.loads(
                r_server.get(str_table.format(
                    RedisSourceService.get_order_from_actives(
                        table, actives_list)
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
    def get_collection_counter(cls, user_id, source_id):
        """
        порядковый номер коллекции юзера
        :param user_id: int
        :param source_id: int
        :return: int
        """
        counter_str = RedisCacheKeys.get_user_collection_counter(user_id, source_id)
        if not r_server.exists(counter_str):
            r_server.set(counter_str, json.dumps({
                'data': [],
                'next_sequence_id': 1,
            }))
        return r_server.get(counter_str)

    @classmethod
    def get_active_table_list(cls, user_id, source_id):
        """
        Возвращает список коллекций юзера
        :param user_id: int
        :param source_id: int
        :return:
        """
        counter = json.loads(cls.get_collection_counter(user_id, source_id), )
        try:
            assert isinstance(counter, dict)
            return counter['data']
        except AssertionError:
            return []

    @classmethod
    def save_good_error_joins(cls, source, left_table, right_table,
                              good_joins, error_joins, join_type):
        """
        Сохраняет временные ошибочные и нормальные джойны таблиц
        :param source: Datasource
        :param joins: list
        :param error_joins: list
        """
        str_joins = RedisCacheKeys.get_source_joins(source.user_id, source.id)
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
    def get_good_error_joins(cls, source, parent_table, child_table):

        r_joins = cls.get_source_joins(source.user_id, source.id)

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
    def get_source_joins(cls, user_id, source_id):
        str_joins = RedisCacheKeys.get_source_joins(user_id, source_id)
        return json.loads(r_server.get(str_joins))

    @classmethod
    def get_last_remain(cls, user_id, source_id):
        tables_remain_key = RedisCacheKeys.get_source_remain(
            user_id, source_id)
        return (r_server.get(tables_remain_key)
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
