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
    def get_user_databases(user_id):
        """
        бд юзера
        :param user_id:
        :return:
        """
        return 'user_datasources:{0}'.format(user_id)

    @staticmethod
    def get_user_datasource(user_id, datasource_id):
        """
        соурс юзера
        :param user_id:
        :param datasource_id:
        :return:
        """
        return '{0}:{1}'.format(
            RedisCacheKeys.get_user_databases(user_id), datasource_id)

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
        user_db_key = RedisCacheKeys.get_user_databases(source.user_id)
        user_datasource_key = RedisCacheKeys.get_user_datasource(
            source.user_id, source.id)

        r_server.lrem(user_db_key, 1, source.id)
        r_server.delete(user_datasource_key)

    @classmethod
    def get_tables(cls, source, tables):
        """
        достает информацию о таблицах из редиса
        :param source: Datasource
        :param tables: list
        :return: list
        """
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
        """
        удаляет инфу о таблицах
        :param source: Datasource
        :param tables: list
        """
        rck = RedisCacheKeys

        str_table = rck.get_active_table(source.user_id, source.id, '{0}')
        str_table_ddl = rck.get_active_table_ddl(source.user_id, source.id, '{0}')
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
        cls.delete_tables_info(tables, actives, str_table, str_table_ddl)
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
                r_server.delete(str_table.format(found['order']))
                r_server.delete(str_table_ddl.format(found['order']))
                actives.remove(found)

    @staticmethod
    def get_collection_name(source, table):
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
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)

        active_tables = json.loads(r_server.get(str_active_tables))

        if r_server.exists(str_table_by_name.format(table)):
            return str_table_by_name.format(table)
        else:
            order = [x for x in active_tables if x['name'] == table][0]['order']
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
        str_active_tables = RedisCacheKeys.get_active_tables(
            user_id, source_id)
        str_joins = RedisCacheKeys.get_source_joins(
            user_id, source_id)

        # новый список коллекций
        new_actives = []
        # старый список коллекций
        old_actives = cls.get_active_list(user_id, source_id)

        joins_in_redis = defaultdict(list)

        pipe = r_server.pipeline()

        for node in ordered_nodes:
            n_val = node.val
            order = cls.get_order_from_actives(n_val, old_actives)
            # если инфы о коллекции нет
            if order is None:

                # cчетчик коллекций пользователя
                coll_counter = cls.get_next_user_collection_counter(
                    user_id, source_id)

                # достаем инфу либо по имени, либо по порядковому номеру
                table_info = RedisSourceService.get_table_full_info(source, n_val)
                table_info = json.loads(table_info)

                info_for_coll = deepcopy(table_info)
                info_for_ddl = deepcopy(table_info)

                for column in info_for_coll["columns"]:
                    del column["origin_type"]
                    del column["is_nullable"]
                    del column["extra"]

                pipe.set(str_table.format(coll_counter), json.dumps(info_for_coll))

                for column in info_for_ddl["columns"]:
                    column["type"] = column["origin_type"]
                    del column["origin_type"]

                pipe.set(str_table_ddl.format(coll_counter), json.dumps(info_for_ddl))

                # удаляем таблицы с именованными ключами
                pipe.delete(str_table_by_name.format(n_val))
                # добавляем новую таблциу в карту активных таблиц
                new_actives.append({'name': n_val, 'order': coll_counter})
            else:
                # старая таблица
                new_actives.append({'name': n_val, 'order': order})

            # добавляем инфу новых джойнов
            if update_joins:
                joins = node.get_node_joins_info()
                for k, v in joins.iteritems():
                    joins_in_redis[k] += v

        pipe.set(str_active_tables, json.dumps(new_actives))
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
            order = cls.get_order_from_actives(n_val, actives)
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
        user_id = source.user_id
        source_id = source.id

        str_table_by_name = RedisCacheKeys.get_active_table(
            user_id, source_id, '{0}')

        pipe = r_server.pipeline()

        for t_name in tables:
            pipe.set(str_table_by_name.format(t_name), json.dumps(
                {
                    "columns": columns[t_name.lower()],
                    "indexes": indexes[t_name.lower()],
                    "foreigns": foreigns[t_name.lower()],
                    "stats": stats[t_name.lower()],
                    "date_intervals": intervals.get(t_name, [])
                }, cls=CustomJsonEncoder
            ))
        pipe.execute()
        a = 4

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

    @classmethod
    def get_order_from_actives(cls, t_name, actives):
        """
        возвращает порядковый номер таблицы по имени
        :param t_name:
        :param actives:
        :return: list
        """
        processed = [x for x in actives if x['name'] == t_name]
        return processed[0]['order'] if processed else None

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
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        actives_list = json.loads(r_server.get(str_active_tables))

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
        str_active_tables = RedisCacheKeys.get_active_tables(
            source.user_id, source.id)
        actives_list = json.loads(r_server.get(str_active_tables))

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
        channels = RedisList(key=subs_str, redis=r_server, pickler=json)
        return channels

    @classmethod
    def delete_user_subscriber(cls, user_id, task_id):
        """
        удаляет канал из каналов для сокетов
        """
        subscribers = cls.get_user_subscribers(user_id)
        for sub in subscribers:
            if sub['queue_id'] == task_id:
                subscribers.remove(sub)
                break

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
    def get_next_user_collection_counter(cls, user_id, source_id):
        """
        порядковый номер коллекции юзера
        :param user_id: int
        :param source_id: int
        :return: int
        """
        counter = RedisCacheKeys.get_user_collection_counter(user_id, source_id)
        if not r_server.exists(counter):
            r_server.set(counter, 2)
            return 1
        else:
            next_count = r_server.get(counter)
            r_server.incr(counter)
        return next_count

    @classmethod
    def get_active_list(cls, user_id, source_id):
        """
        Возвращает список коллекций юзера
        :param user_id: int
        :param source_id: int
        :return:
        """
        str_active_tables = RedisCacheKeys.get_active_tables(
            user_id, source_id)
        if not r_server.exists(str_active_tables):
            r_server.set(str_active_tables, '[]')
            r_server.expire(str_active_tables, settings.REDIS_EXPIRE)
            return []
        else:
            return json.loads(r_server.get(str_active_tables))

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
