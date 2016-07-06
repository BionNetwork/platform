# coding: utf-8
from __future__ import unicode_literals

import json
from itertools import groupby
import operator

from django.db import transaction

from core.models import (DatasourceMeta, DatasourceMetaKeys, DatasetToMeta,
                         ConnectionChoices, Datasource)
from etl.services.datasource.repository import r_server
from etl.services.db.factory import DatabaseService, LocalDatabaseService
from etl.services.file.factory import FileService
from etl.services.datasource.repository.storage import RedisSourceService
from etl.models import TableTreeRepository as TTRepo
from core.helpers import get_utf8_string
from etl.services.middleware.base import generate_table_key


RedisSS = RedisSourceService


class DataSourceService(object):
    """
        Сервис управляет сервисами БД, Файлов и Редиса!
    """
    DB_TYPES = [
        ConnectionChoices.POSTGRESQL,
        ConnectionChoices.MYSQL,
        ConnectionChoices.MS_SQL,
        ConnectionChoices.ORACLE,
    ]
    FILE_TYPES = [
        ConnectionChoices.EXCEL,
        ConnectionChoices.CSV,
        # ConnectionChoices.TXT,
    ]

    @classmethod
    def get_source_service(cls, source):
        """
        В зависимости от типа источника перенаправляет на нужный сервис
        """
        conn_type = source.conn_type

        if conn_type in cls.DB_TYPES:
            return DatabaseService(source)
        elif conn_type in cls.FILE_TYPES:
            return FileService(source)
        else:
            raise ValueError("Неизвестный тип подключения!")

    @classmethod
    def delete_datasource(cls, source):
        """
        Redis
        Удаляет информацию об источнике

        Args:
            source(core.models.Datasource): Источник данных
        """
        RedisSourceService.delete_datasource(source)

    @classmethod
    def tree_full_clean(cls, source):
        """
        Redis
        Удаляет информацию о таблицах, джоинах, дереве

        Args:
            source(core.models.Datasource): Источник данных
        """
        RedisSourceService.tree_full_clean(source)

    @classmethod
    def get_source_tables(cls, source):
        """
        Возвращает информацию об источнике + таблицы истоника

        :type source: Datasource

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            dict: {
                db: <str>,
                host: <str>,
                source_id:
                user_id:
                .........
                tables: [{'name': <table_name_1>, 'name': <table_name_2>}]
            }

        """
        service = cls.get_source_service(source)
        tables = service.get_tables()
        # кладем список таблиц в редис
        RedisSourceService.set_tables(source.id, tables)

        # информация о источнике для фронта
        source_info = source.get_source_info()
        source_info["tables"] = tables

        return source_info

    @staticmethod
    def check_connection(post):
        """
        Database
        Проверяет подключение
        """
        conn_info = {
            'host': get_utf8_string(post.get('host')),
            'login': get_utf8_string(post.get('login')),
            'password': get_utf8_string(post.get('password')),
            'db': get_utf8_string(post.get('db')),
            'port': get_utf8_string(post.get('port')),
            'conn_type': int(get_utf8_string(post.get('conn_type')))
        }
        return DatabaseService.get_connection_by_dict(conn_info)

    @staticmethod
    def remains_nodes(card_id):
        """
        Возвращает список остатков типа TTRepo.RemainNode
        """
        builder_data = RedisSS.get_card_builder_data(card_id)

        return TTRepo.remains_nodes(builder_data)

    @classmethod
    def add_randomly_from_remains(cls, card_id, node_id):
        """
        Пытаемся связать остаток с деревом в любое место
        """

        remain_nodes = cls.remains_nodes(card_id)
        node = RedisSS.get_remain_node(remain_nodes, node_id)

        table, source_id = node.val, node.source_id

        if not RedisSS.check_tree_exists(card_id):
            sel_tree = TTRepo.build_single_root(node)
            resave = True
        else:
            # получаем дерево из редиса
            sel_tree = cls.get_tree(card_id)
            ordered_nodes = sel_tree.ordered_nodes

            tables_info = RedisSS.info_for_tree_building_NEW(
                card_id, ordered_nodes, node)

            # перестраиваем дерево
            unbinded = sel_tree.build(
                table, source_id, node_id, tables_info)
            resave = unbinded is None

        ordered_nodes = sel_tree.ordered_nodes

        # признак того, что дерево перестроилось
        if resave:
            # сохраняем дерево, если таблицы не в дереве
            RedisSS.put_remain_to_builder_actives(card_id, node)
            # save tree structure
            RedisSS.save_tree_structure(card_id, sel_tree)

        tree_nodes = TTRepo.nodes_info(ordered_nodes)

        remain_nodes = cls.remains_nodes(card_id)
        remains = TTRepo.nodes_info(remain_nodes)

        # determining unbinded tail
        tail_ = cls.extract_tail(remains, node_id) if not resave else None

        return {
            'tree_nodes': tree_nodes,
            'remains': remains,
            'tail': tail_,
        }

    @classmethod
    def from_remain_to_certain(cls, card_id, parent_id, child_id):
        """
        Добавление из остатков в определенную ноду
        """
        sel_tree = cls.get_tree(card_id)

        p_node = sel_tree.get_node(parent_id)
        if p_node is None:
            raise Exception("Incorrect parent ID!")

        remain_nodes = cls.remains_nodes(card_id)
        ch_node = RedisSS.get_remain_node(remain_nodes, child_id)

        if ch_node is None:
            raise Exception("Incorrect child ID!")

        parent_info = RedisSS.get_table_info(
            card_id, p_node.source_id, parent_id)
        child_info = RedisSS.get_table_info(
            card_id, ch_node.source_id, child_id)

        is_bind = sel_tree.try_bind_two_nodes(
            p_node, ch_node, parent_info, child_info)

        # если забиндилось
        if is_bind:
            # сохраняем дерево, если таблицы не в дереве
            RedisSS.put_remain_to_builder_actives(card_id, ch_node)
            # save tree structure
            RedisSS.save_tree_structure(card_id, sel_tree)

        tree_nodes = TTRepo.nodes_info(sel_tree.ordered_nodes)

        remain_nodes = cls.remains_nodes(card_id)
        remains = TTRepo.nodes_info(remain_nodes)

        # determining unbinded tail
        tail_ = cls.extract_tail(remains, child_id) if not is_bind else None

        return {
            'tree_nodes': tree_nodes,
            'remains': remains,
            'tail': tail_,
        }

    @classmethod
    def reparent(cls, card_id, parent_id, child_id):
        """
        Пытаемся перетащить узел дерева из одного места в другое

        Args:
            card_id(int): id карточки
            parent_id(int): id родительского узла
            child_id(int): id узла-потомка

        Returns:
            dict: Информацию об связанных/не связанных узлах дерева и остатка
        """
        sel_tree = cls.get_tree(card_id)

        p_node = sel_tree.get_node(parent_id)
        if p_node is None:
            raise Exception("Incorrect parent ID!")

        # child node must be in actives
        ch_node = sel_tree.get_node(child_id)

        if ch_node is None:
            raise Exception("Incorrect child ID!")

        parent_info = RedisSS.get_table_info(
            card_id, p_node.source_id, parent_id)
        child_info = RedisSS.get_table_info(
            card_id, ch_node.source_id, child_id)

        remain = sel_tree.reparent_node(
            p_node, ch_node, parent_info, child_info)

        # если забиндилось
        if remain is None:
            # save tree structure
            RedisSS.save_tree_structure(card_id, sel_tree)

        tree_nodes = TTRepo.nodes_info(
            sel_tree.ordered_nodes)

        remain_nodes = cls.remains_nodes(card_id)
        remains = TTRepo.nodes_info(remain_nodes)

        # determining unbinded tail
        tail_ = cls.extract_tail(tree_nodes, child_id) if remain else None

        return {
            'tree_nodes': tree_nodes,
            'remains': remains,
            'tail': tail_,
        }

    @classmethod
    def send_nodes_to_remains(cls, card_id, node_id):
        """
        Перенос нодов дерева в остатки
        """
        sel_tree = cls.get_tree(card_id)
        node = sel_tree.get_node(node_id)

        # is root
        if node.parent is None:
            RedisSS.put_actives_to_builder_remains(
                card_id, sel_tree.ordered_nodes)
            RedisSS.remove_tree(card_id)
            tree_nodes = []

        else:
            node_to_remove = sel_tree.remove_sub_tree(node_id)

            to_remain_nodes = sel_tree._get_tree_ordered_nodes(
                [node_to_remove, ])
            # сохраняем дерево, если таблицы не в дереве
            RedisSS.put_actives_to_builder_remains(
                card_id, to_remain_nodes)

            # save tree structure
            RedisSS.save_tree_structure(card_id, sel_tree)

            tree_nodes = TTRepo.nodes_info(
                sel_tree.ordered_nodes)

        remain_nodes = cls.remains_nodes(card_id)
        remains = TTRepo.nodes_info(remain_nodes)

        return {
            'tree_nodes': tree_nodes,
            'remains': remains,
        }

    @classmethod
    def check_table_in_builder(cls, card_id, source_id, table):
        """
        Проверяем узел приходящий уже есть в наличие билдера
        """
        return RedisSS.check_table_in_builder(card_id, source_id, table)

    @classmethod
    def get_tree(cls, card_id):
        """
        Получаем дерево
        """
        # Получаем структуру из Redis и строем дерево
        structure = RedisSS.get_active_tree_structure(card_id)
        return TTRepo.build_tree_by_structure(structure)

    @classmethod
    def extract_tail(cls, nodes_info, node_id):
        """
        Из остатков или активов определяет последнюю несвязанную ноду
        remains - список диктов [{}, {}, ...]
        """
        for node in nodes_info:
            if str(node["node_id"]) == str(node_id):
                nodes_info.remove(node)
                return node

    @classmethod
    def get_tree_api(cls, card_id):
        """
        Строит дерево, если его нет, иначе перестраивает
        """
        # дерева еще нет или все в остатках
        if not RedisSS.check_tree_exists(card_id):
            tree_nodes = []
            remain_nodes = cls.remains_nodes(card_id)
            remains = TTRepo.nodes_info(remain_nodes)

            return {
                'tree_nodes': tree_nodes,
                'remains': remains,
            }
        # иначе достраиваем дерево, если можем, если не можем вернем остаток
        sel_tree = cls.get_tree(card_id)

        ordered_nodes = sel_tree.ordered_nodes
        tree_nodes = TTRepo.nodes_info(ordered_nodes)

        remain_nodes = cls.remains_nodes(card_id)
        remains = TTRepo.nodes_info(remain_nodes)

        return {
                'tree_nodes': tree_nodes,
                'remains': remains,
            }

    @classmethod
    def cache_columns(cls, card_id, source_id, table):
        """
        Пришедшую таблицу кладем в строительную карту дерева
        """
        node_id = RedisSS.check_table_in_builder_remains(card_id, source_id, table)
        if node_id:
            return node_id

        source = Datasource.objects.get(id=source_id)
        service = cls.get_source_service(source)

        indents = DataSourceService.extract_source_indentation(source_id)

        columns, indexes, foreigns, statistics, date_intervals = (
            service.get_columns_info([table, ], indents))

        info = {
            "value": table,
            "sid": source_id,
            "columns": columns[table],
            "indexes": indexes[table],
            "foreigns": foreigns[table],
            "stats": statistics[table],
            "date_intervals": date_intervals.get(table, [])
        }

        # кладем инфу таблицы в остатки билдера дерева
        return RedisSS.put_table_info_in_builder(
            card_id, source_id, table, info)

    @classmethod
    def get_rows_info(cls, source, cols):
        """
        Получение списка значений указанных колонок и таблиц
        в выбранном источнике данных
        Args:
            source(core.models.Datasource): Источник
            cols(list): Описать

        Returns:
            list Описать
        """
        structure = RedisSourceService.get_active_tree_structure(
            source.user_id)
        service = cls.get_source_service(source)
        return service.get_rows(cols, structure)

    @classmethod
    def remove_tables_from_tree_NEW(cls, card_id, tables):
        """
        Redis
        удаление таблиц из дерева

        Args:
            source(core.models.Datasource): Источник
            tables(): Описать
        """
        # получаем дерево
        sel_tree = cls.get_tree(card_id)

        r_val = sel_tree.root.val
        source_id = sel_tree.root.source_id

        if (r_val, source_id) in tables:
            RedisSS.tree_full_clean_NEW(card_id)
            sel_tree.root = None
        else:
            sel_tree.delete_nodes_NEW(tables)

        if sel_tree.root:
            RedisSS.delete_tables_NEW(card_id, tables)

            ordered_nodes = sel_tree.ordered_nodes
            structure = sel_tree.structure
            RedisSS.save_tree_builder(structure, ordered_nodes,
                                    card_id, update_joins=False)

    @classmethod
    def get_columns_and_joins(cls, card_id, parent_id, child_id):
        """
        """
        parent = DataSourceService.get_node(card_id, parent_id)
        child = DataSourceService.get_node(card_id, child_id)
        parent_sid, parent_table = parent.source_id, parent.val
        child_sid, child_table = child.source_id, child.val

        columns = RedisSS.get_columns_for_joins(
            card_id, parent_table, parent_sid, child_table, child_sid)

        tree = cls.get_tree(card_id)

        join_type, cols_info = TTRepo.get_joins(tree, parent_id, child_id)

        return {
            'columns': columns,
            'join_type': join_type,
            'joins': cols_info
        }

    @classmethod
    def check_new_joins(cls, card_id, parent, child, joins):
        """
        Redis
        Проверяет пришедшие джойны на совпадение типов
        Args:
            card_id(int): id карточки
            parent(Node): родительский узел
            child(RemainNode): дочерний узел
            joins(): Информация о связях
        Returns:
            Описать
        """
        # FIXME: Описать
        joins_set = set()
        for j in joins:
            joins_set.add(tuple(j))

        cols_types = cls.get_columns_types(card_id, parent, child)

        # список джойнов с неверными типами
        error_joins = []
        good_joins = []

        for j in joins_set:
            l_c, _, r_c = j
            if (cols_types[u'{0}.{1}.{2}'.format(parent.val, l_c, parent.source_id)] !=
                    cols_types[u'{0}.{1}.{2}'.format(
                        child.val, r_c, child.source_id)]):
                error_joins.append(j)
            else:
                good_joins.append(j)

        return good_joins, error_joins, joins_set

    @classmethod
    def save_new_joins(cls, card_id, parent_node, child_node, join_type, joins):
        """
        Redis
        Cохранение новых джойнов

        Args:
            source(core.models.Datasource): Источник
            left_table(): Описать
            right_table(): Описать
            join_type(): Описать
            joins(): Описать

        Returns:
            Описать
        """

        good_joins, error_joins, joins_set = cls.check_new_joins(
            card_id, parent=parent_node, child=child_node, joins=joins,
        )

        if not error_joins:
            # Получаем дерево
            sel_tree = cls.get_tree(card_id)

            sel_tree.update_node_joins(parent=parent_node, child=child_node,
                                       join_type=join_type, joins=joins_set)
            RedisSS.save_tree_structure(card_id, sel_tree)

            if cls.check_node_id_in_remains(card_id, child_node.node_id):
                RedisSS.put_remain_to_builder_actives(card_id, child_node)

        return {}

    @staticmethod
    def get_collections_names(source, tables):
        """
        Redis
        Получение списка имен коллекций

        Args:
            source(core.models.Datasource): Источник
            tables(list): Список названий таблиц

        Return:
            (list): Описать
        """
        # FIXME: Описать
        return [RedisSourceService.get_collection_name(source, table)
                for table in tables]

    @classmethod
    def get_columns_types(cls, card_id, parent, child):
        """
        Redis
        Получение типов колонок таблиц
        Args:
            card_id(int): id карточки
            parent(Node): родительский узел
            child(RemainNode): дочерний узел
        Returns:
            dict: Информация о типах столбцов род и дочерней таблицы
        """
        # FIXME: Описать
        cols_types = {}

        for node in [parent, child]:
            # node_id = RedisSS.get_node_id(t_name, card_id, sid)
            t_cols = RedisSS.get_table_info(card_id, node.source_id, node.node_id)['columns']

            for col in t_cols:
                cols_types[u'{0}.{1}.{2}'.format(
                    node.val, col['name'], node.source_id)] = col['type']

        return cols_types

    @classmethod
    def retitle_table_column(cls, source, table, column, title):
        """
        Redis
        Переименовываем заголовка(title) колонки для схемы куба

        Args:
            source(core.models.Datasource): Источник
            table(): Описать
            column(): Описать
            title(): Описать

        Returns:
            Описать
        """
        table_info = json.loads(
                RedisSourceService.get_table_full_info(source, table))

        for col in table_info['columns']:
            if col['name'] == column:
                col['title'] = title
                break

        collection_name = RedisSourceService.get_collection_name(source, table)

        r_server.set(collection_name, json.dumps(table_info))

    # FIXME Удалить
    @classmethod
    def check_table_exists_query(cls, local_instance, table_name, db):
        # FIXME: Описать
        service = LocalDatabaseService()
        return service.check_table_exists_query(
            local_instance, table_name, db)

    @classmethod
    def check_existing_table(cls, table_name):
        """
        проверка существования таблицы с именем при создании
        :param table_name:
        :return:
        """
        # FIXME: Описать
        from django.db import connection
        return table_name in connection.introspection.table_names()

    @classmethod
    def get_source_connection(cls, source):
        """
        Получить объект соединения источника данных
        :type source: Datasource
        """
        # FIXME: Описать
        service = cls.get_source_service(source)
        return service.datasource.connection

    @classmethod
    def get_local_instance(cls):
        """
        возвращает инстанс локального хранилища данных

        Returns:

        """
        return LocalDatabaseService()

    @classmethod
    def tables_info_for_metasource(cls, source, tables):
        """
        Redis
        Получение инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta

        Args:
            source(core.models.Datasource): Источник данных
            tables(list): список вида [{'table': <table_name>, 'col': <col_name>}]

        Returns:
            Описать
        """

        # FIXME: Описать
        return RedisSourceService.tables_info_for_metasource(
            source, tables)

    @staticmethod
    def update_collections_stats(collections_names, last_key):
        """
        Redis
        Описать
        """
        # FIXME: Перенести в RedisSourceService

        pipe = r_server.pipeline()

        for collection in collections_names:
            info_str = r_server.get(collection)
            if info_str:
                table_info = json.loads(info_str)
                if table_info['stats']:
                    table_info['stats'].update({
                        'last_row': {
                            'cdc_key': last_key,
                        }
                    })

                    pipe.set(collection, json.dumps(table_info))
            pipe.execute()

    @staticmethod
    def update_datasource_meta(key, source, cols,
                               tables_info_for_meta, last_row, dataset_id):
        """
        Создание DatasourceMeta для Datasource

        Args:
            dataset_id(int): data set identifier
            key(str): Ключ
            source(Datasource): Источник данных
            cols(list): Список колонок
            last_row(str or None): Последняя запись
            tables_info_for_meta: Данные о таблицах

        Returns:
            DatasourceMeta: Объект мета-данных

        """
        res = dict()
        for table, col_group in groupby(cols, lambda x: x['table']):
            with transaction.atomic():
                try:
                    source_meta = DatasourceMeta.objects.get(
                        datasource_id=source.id,
                        collection_name=table,
                    )
                except DatasourceMeta.DoesNotExist:
                    source_meta = DatasourceMeta(
                        datasource_id=source.id,
                        collection_name=table,
                    )
                stats = {
                    'tables_stat': {},
                    'row_key': [],
                    'row_key_value': []
                }
                fields = {'columns': [], }

                table_info = tables_info_for_meta[table]
                stats['date_intervals'] = table_info['date_intervals']

                stats['tables_stat'] = table_info['stats']
                t_cols = table_info['columns']

                for sel_col in col_group:
                    for col in t_cols:
                        # cols info
                        if sel_col['col'] == col['name']:
                            fields['columns'].append(col)

                            # primary keys
                            if hasattr(col, 'is_primary') and col['is_primary']:
                                stats['row_key'].append(col['name'])

                if last_row and stats['row_key']:
                    # корневая таблица
                    mapped = filter(
                        lambda x: x[0]['table'] == table, zip(cols, last_row))

                    for (k, v) in mapped:
                        if k['col'] in stats['row_key']:
                            stats['row_key_value'].append({k['col']: v})

                source_meta.fields = json.dumps(fields)
                source_meta.stats = json.dumps(stats)
                source_meta.save()
                DatasourceMetaKeys.objects.get_or_create(
                    meta=source_meta,
                    value=key,
                )
                # связываем Dataset и мета информацию
                DatasetToMeta.objects.get_or_create(
                    meta_id=source_meta.id,
                    dataset_id=dataset_id,
                )
                res.update({
                    table: source_meta.id
                })
        return res

    @staticmethod
    def split_file_sub_tree(sub_tree):
        """
        """
        childs = sub_tree['childs']
        sub_tree['childs'] = []
        sub_tree['type'] = 'file'
        items = [sub_tree, ]

        while childs:
            new_childs = []
            for child in childs:
                items.append({'val': child['val'], 'childs': [],
                              'sid': child['sid'], 'type': 'file',
                              'joins': child['joins'],
                              })
                new_childs.extend(child['childs'])
            childs = new_childs

        return items

    @staticmethod
    def extract_columns(sub_trees, columns):
        """
        Достает список диктов с инфой о таблице/листе и ее колонке
        """
        for tree in sub_trees:
            tables = columns[str(tree['sid'])]
            t_name = tree['val']
            columns_info = [{"table": t_name, "col": x} for x in tables[t_name]]
            childs = tree['childs']

            while childs:
                new_childs = []
                for child in childs:
                    t_name = child['val']
                    columns_info += [
                        {"table": t_name, "col": x} for x in tables[t_name]]
                    new_childs.extend(child['childs'])
                childs = new_childs

            tree['columns'] = columns_info

    @classmethod
    def split_nodes_by_source_types(cls, sub_trees):
        """
        Если связки типа файл, то делим дальше, до примитива,
        если связка типа бд, то связку таблиц оставляем, как единую сущность
        """

        new_sub_trees = []

        for sub_tree in sub_trees:
            sid = sub_tree['sid']
            source = Datasource.objects.get(id=sid)
            source_service = DataSourceService.get_source_service(source)

            if isinstance(source_service, DatabaseService):
                sub_tree['type'] = 'db'
                new_sub_trees += [sub_tree, ]

            elif isinstance(source_service, FileService):
                new_sub_trees += cls.split_file_sub_tree(sub_tree)

        return new_sub_trees

    @classmethod
    def prepare_sub_trees(
            cls, tree_structure, columns, card_id, meta_tables_info):
        """
        Подготавливает список отдельных сущностей для закачки в монго
        """
        subs_by_sid = TTRepo.split_nodes_by_sources(
            tree_structure)
        subs_by_type = cls.split_nodes_by_source_types(
            subs_by_sid)

        cls.extract_columns(subs_by_type, columns)

        cls.create_hash_names(subs_by_type, card_id)

        cls.build_columns_info(subs_by_type, meta_tables_info)

        return subs_by_type

    @staticmethod
    def create_hash_names(items, card_id):
        """
        Для каждого набора, учитывая таблы/листы и колонки выщитывает хэш
        """
        for item in items:
            sorted_cols = sorted(
                item['columns'], key=operator.itemgetter('table', 'col'))
            cols_str = reduce(operator.add,
                              [u"{0}-{1}".format(x['table'], x['col'])
                               for x in sorted_cols], u'')
            item['collection_hash'] = generate_table_key(
                card_id, item['sid'], cols_str)

    @staticmethod
    def build_columns_info(items, meta_tables_info):
        """
        1) Образует списки ['table__column', ]
        2) Образует списки [{'table__column': {type: , length: }, ]
        Добавляет эти списки каждому поддереву
        """
        for item in items:

            sid = str(item['sid'])
            joined_columns = []
            columns_types = {}

            for col_info in item["columns"]:
                t = col_info['table']
                c = col_info['col']
                col_joined = u"{0}__{1}".format(t, c)
                joined_columns.append(col_joined)

                table_columns = meta_tables_info[sid][t]['columns']
                for t_col in table_columns:
                    if t_col["name"] == c:
                        columns_types[col_joined] = {
                            'type': t_col['type'],
                            'max_length': t_col['max_length'],
                        }
                        break

            item['joined_columns'] = joined_columns
            item['columns_types'] = columns_types

    @staticmethod
    def prepare_relations(sub_trees):
        """
        Строит список связей таблиц из Postgres
        """

        tables_hash_map = {}
        relations = []

        for sub in sub_trees:
            sid = sub['sid']
            hash_ = sub['collection_hash']

            for column in sub['joined_columns']:
                name = u"{0}__{1}".format(sid, column)
                tables_hash_map[name] = hash_

        # голова дерева без связей
        main = sub_trees[0]
        relations.append({
            "table_hash": main['collection_hash']
        })

        for sub in sub_trees[1:]:

            join = sub["joins"][0]
            left_table = join["left"]
            left_sid = left_table['sid']

            hash_str = "{0}__{1}__{2}".format(
                left_sid, left_table["table"], left_table["column"])

            table_hash = sub['collection_hash']

            rel = {
                "table_hash": table_hash,
                "type": join["join"]["type"],
                "conditions": [],
            }
            # хэш таблицы, с котрой он связан
            parent_hash = tables_hash_map[hash_str]

            # условия соединений таблиц
            for join in sub["joins"]:

                left_table = join["left"]
                right_table = join["right"]

                rel["conditions"].append({
                    "l": '"sttm__{0}"."{1}__{2}"'.format(
                        parent_hash, left_table["table"], left_table["column"]),
                    "r": '"sttm__{0}"."{1}__{2}"'.format(
                        table_hash, right_table["table"], right_table["column"]),
                    "operation": join["join"]["value"],
                })

            relations.append(rel)

        return relations

    @classmethod
    def get_node(cls, card_id, node_id):
        """
        Получение данных по узлу

        Arguments:
            card_id(int): id карточки
            node_id(int): id узла

        Returns:
            Node
        """
        node = None

        if not cls.check_node_id_in_remains(card_id, node_id):
            sel_tree = cls.get_tree(card_id)
            node = sel_tree.get_node(node_id)
        else:
            builder_data = RedisSS.get_card_builder_data(card_id)
            for remain in TTRepo.remains_nodes(builder_data):
                if int(remain.node_id) == int(node_id):
                    node = remain
                    break
        if node is None:
            raise Exception("Bull shit!")
        return node

    @classmethod
    def get_node_info(cls, card_id, node_id):
        node = cls.get_node(card_id, node_id)
        table, source_id = node.val, node.source_id
        table_info = RedisSS.get_table_info(card_id, source_id, node.node_id)

        return dict(
            node_id=node.node_id,
            parent_id=getattr(node.parent, 'node_id', None),
            sid=source_id,
            val=table,
            cols=[{'col_name': x['name'], 'col_title': x.get('title', None),}
                  for x in table_info['columns']
                 ])

    @classmethod
    def check_node_id_in_remains(cls, card_id, node_id):
        """
        Проверяет есть ли данный id в билдере карты в остатках
        """
        node_id = int(node_id)
        b_data = RedisSS.get_card_builder_data(card_id)

        for sid in b_data:
            s_data = b_data[sid]
            # проверка в остатках
            for remain, remain_id in s_data['remains'].iteritems():
                if int(remain_id) == node_id:
                    return True
        return False

    @classmethod
    def check_node_id_in_builder(cls, card_id, node_id, in_remain=True):
        """
        Проверяет есть ли данный id в билдере карты в активных или остатках
        """
        node_id = int(node_id)
        b_data = RedisSS.get_card_builder_data(card_id)

        for sid in b_data:
            s_data = b_data[sid]
            # проверка в остатках
            if in_remain:
                for remain, remain_id in s_data['remains'].iteritems():
                    if int(remain_id) == node_id:
                        return True
            # проверка в активных
            else:
                for active, active_id in s_data['actives'].iteritems():
                    if int(active_id) == node_id:
                        return True
        return False

    @classmethod
    def extract_source_indentation(cls, source_id):
        indent = RedisSS.get_source_indentation(source_id)
        return indent

    @classmethod
    def insert_source_indentation(cls, source_id, sheet, indent):
        """
        Сохраняем отступ для страницы соурса
        """
        indents = RedisSS.get_source_indentation(source_id)
        indents[sheet] = indent
        RedisSS.set_source_indentation(source_id, indents)
