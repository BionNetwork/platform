# coding: utf-8

import json
from itertools import groupby

from django.db import transaction
from django.conf import settings

from core.models import (DatasourceMeta, DatasourceMetaKeys, DatasetToMeta,
                         ConnectionChoices, Datasource)
from etl.services.datasource.repository import r_server
from etl.services.db.factory import DatabaseService, LocalDatabaseService
from etl.services.file.factory import FileService
from etl.services.datasource.repository.storage import RedisSourceService
from etl.models import TableTreeRepository
from core.helpers import get_utf8_string


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
        ConnectionChoices.TXT,
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
        RedisSourceService.set_tables(source, tables)

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

    @classmethod
    def get_columns_info(cls, source, tables):

        """
        Получение информации по колонкам

        Args:
            source(`Datasource`): источник
            tables(list): Список имен таблиц

        Returns:
            list: Список словарей с информацией о дереве. Подробрый формат
            ответа см. RedisSourceService.get_final_info
        """

        service = cls.get_source_service(source)

        # если информация о таблице уже есть в редисе, то просто получаем их,
        # иначе получаем новые таблицы
        #  и спрашиваем информацию по ним у источника
        new_tables, old_tables = RedisSourceService.filter_exists_tables(
            source, tables)

        if new_tables:
            columns, indexes, foreigns, statistics, date_intervals = (
                service.get_columns_info(new_tables))

            RedisSourceService.insert_columns_info(
                source, new_tables, columns, indexes,
                foreigns, statistics, date_intervals)

        # FIXME для старых таблиц, которые уже лежат в редисе,
        # FIXME нужно ли актуализировать интервалы дат каждый раз

        # if old_tables:
        #     # берем колонки старых таблиц
        #     all_columns = service.fetch_tables_columns(tables)
        #     old_tables_intervals = service.get_date_intervals(all_columns)
        #     # актуализируем интервалы дат (min, max) для таблиц с датами
        #     RedisSourceService.insert_date_intervals(
        #         source, old_tables, old_tables_intervals)

        # существование дерева
        tree_exists = RedisSourceService.check_tree_exists(source)

        # работа с деревьями
        if not tree_exists:
            tables_info = RedisSourceService.info_for_tree_building(
                (), tables, source)

            trees, without_bind = TableTreeRepository.build_trees(
                tuple(tables), tables_info)
            sel_tree = TableTreeRepository.select_tree(trees)

            remains = without_bind[sel_tree.root.val]
        else:
            # достаем структуру дерева из редиса
            structure = RedisSourceService.get_active_tree_structure(source)
            # строим дерево
            sel_tree = TableTreeRepository.build_tree_by_structure(structure)

            ordered_nodes = sel_tree.ordered_nodes

            tables_info = RedisSourceService.info_for_tree_building(
                ordered_nodes, tables, source)

            # перестраиваем дерево
            sel_tree.build(tuple(tables), tables_info)
            remains = sel_tree.no_bind_tables

        # таблица без связи
        last = RedisSourceService.insert_remains(source, remains)

        # сохраняем дерево
        structure = sel_tree.structure
        ordered_nodes = sel_tree.ordered_nodes
        RedisSourceService.insert_tree(structure, ordered_nodes, source)

        return RedisSourceService.get_final_info(ordered_nodes, source, last)

    @classmethod
    def get_tree_info(cls, source, table):
        """
        """
        cls.cache_columns(source, table)
        sel_tree, last = cls.get_tree(source, table)

        # таблица без связи
        if last is not None:
            RedisSourceService.insert_last(last, source.user_id, source.id)

        # сохраняем дерево
        structure = sel_tree.structure
        ordered_nodes = sel_tree.ordered_nodes
        RedisSS.insert_tree_NEW(structure, ordered_nodes, source.user_id)

        return RedisSourceService.get_final_info_NEW(
            ordered_nodes, source.user_id, last)

    @classmethod
    def get_tree(cls, source, table):
        """
        Строит дерево, если его нет, иначе перестраивает
        """
        u_id = source.user_id

        tree_exists = RedisSS.check_tree_exists_NEW(u_id)

        # дерева еще нет
        if not tree_exists:
            RedisSS.info_for_tree_building_NEW(
                (), table, source)
            sel_tree = TableTreeRepository.build_single_root(table, source.id)
            # остатков нет
            last = None

        # иначе достраиваем дерево, если можем, если не можем вернем остаток
        else:
            # достаем структуру дерева из редиса
            structure = RedisSourceService.get_active_tree_structure_NEW(u_id)

            print 'stru', structure
            # строим дерево
            sel_tree = TableTreeRepository.build_tree_by_structure(structure)
            ordered_nodes = sel_tree.ordered_nodes

            print 'nodes', ordered_nodes

            tables_info = RedisSS.info_for_tree_building_NEW(
                ordered_nodes, table, source)

            print 'tables_info', tables_info

            # перестраиваем дерево
            sel_tree.build_NEW(table, tables_info, source.id)
            print 'last', sel_tree.no_bind_tables
            last = sel_tree.no_bind_tables

        return sel_tree, last

    @classmethod
    def cache_columns(cls, source, table):

        service = cls.get_source_service(source)

        # если информация о таблице уже есть в редисе,
        # то просто получаем их,
        # иначе спрашиваем информацию по нему у источника
        already = RedisSS.already_table_in_redis(source, table)

        if not already:
            columns, indexes, foreigns, statistics, date_intervals = (
                service.get_columns_info([table, ]))

            # кладем в редис по имени просто
            RedisSS.insert_columns_info(
                source, [table, ], columns, indexes,
                foreigns, statistics, date_intervals)

    @classmethod
    def get_rows_info(cls, source, cols):
        """
        Получение списка значений указанных колонок и таблиц в выбранном источнике данных


        Args:
            source(core.models.Datasource): Источник
            cols(list): Описать

        Returns:
            list Описать
        """
        structure = RedisSourceService.get_active_tree_structure(source)
        service = cls.get_source_service(source)
        return service.get_rows(cols, structure)

    @classmethod
    def remove_tables_from_tree(cls, source, tables):
        """
        Redis
        удаление таблиц из дерева

        Args:
            source(core.models.Datasource): Источник
            tables(): Описать
        """
        # достаем структуру дерева из редиса
        structure = RedisSourceService.get_active_tree_structure(source)
        # строим дерево
        sel_tree = TableTreeRepository.build_tree_by_structure(structure)

        r_val = sel_tree.root.val
        if r_val in tables:
            RedisSourceService.tree_full_clean(source)
            sel_tree.root = None
        else:
            sel_tree.delete_nodes(tables)

        if sel_tree.root:
            RedisSourceService.delete_tables(source, tables)

            ordered_nodes = sel_tree.ordered_nodes
            structure = sel_tree.structure
            RedisSourceService.insert_tree(structure, ordered_nodes, source, update_joins=False)

    @classmethod
    def check_is_binding_remain(cls, source, child_table):
        """
        Проверяем является ли таблица child_table остаточной таблицей,
        то есть последней в дереве без связей
        """
        source_key = RedisSourceService.get_user_source(source)
        remain = RedisSourceService.get_last_remain(source_key)
        return remain == child_table

    @classmethod
    def get_columns_and_joins_for_join_window(
            cls, source, parent_table, child_table, has_warning):
        """
        Redis
        список колонок и джойнов таблиц для окна связей таблиц

        Args:
            source(core.models.Datasource): Источник
            parent_table(): Описать
            child_table(): Описать
            has_warning(): Описать

        Returns:
            dict: Описать
        """
        # FIXME: Описать
        is_binding_remain = cls.check_is_binding_remain(
            source, child_table)

        # если связываем проблемных и один из них последний(remain)
        if has_warning and is_binding_remain:
            columns = RedisSourceService.get_columns_for_tables_without_bind(
                source, parent_table, child_table)
            good_joins, error_joins = RedisSourceService.get_good_error_joins(
                source, parent_table, child_table)

        # если связываем непроблемных или таблицы в дереве,
        # имеющие неправильные связи
        else:
            columns = RedisSourceService.get_columns_for_tables_with_bind(
                source, parent_table, child_table)
            good_joins, error_joins = RedisSourceService.get_good_error_joins(
                source, parent_table, child_table)

        result = {'columns': columns,
                  'good_joins': good_joins,
                  'error_joins': error_joins,
                  }
        return result

    @classmethod
    def check_new_joins(cls, source, left_table, right_table, joins):
        # избавление от дублей
        """
        Redis
        Проверяет пришедшие джойны на совпадение типов

        Args:
            source(core.models.Datasource): Источник
            left_table(): Описать
            right_table(): Описать
            joins(): Описать

        Returns:
            Описать
        """

        # FIXME: Описать
        joins_set = set()
        for j in joins:
            joins_set.add(tuple(j))

        cols_types = cls.get_columns_types(source, [left_table, right_table])

        # список джойнов с неверными типами
        error_joins = list()
        good_joins = list()

        for j in joins_set:
            l_c, j_val, r_c = j
            if (cols_types[u'{0}.{1}'.format(left_table, l_c)] !=
                    cols_types[u'{0}.{1}'.format(right_table, r_c)]):
                error_joins.append(j)
            else:
                good_joins.append(j)

        return good_joins, error_joins, joins_set

    @classmethod
    def save_new_joins(cls, source, left_table, right_table, join_type, joins):
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

        source_key = RedisSourceService.get_user_source(source)

        # FIXME: Описать
        # joins_set избавляет от дублей
        good_joins, error_joins, joins_set = cls.check_new_joins(
            source, left_table, right_table, joins)

        data = RedisSourceService.save_good_error_joins(
            source, left_table, right_table,
            good_joins, error_joins, join_type)

        if not error_joins:
            # достаем структуру дерева из редиса
            structure = RedisSourceService.get_active_tree_structure(source)
            # строим дерево
            sel_tree = TableTreeRepository.build_tree_by_structure(structure)

            sel_tree.update_node_joins(
                left_table, right_table, join_type, joins_set)

            # сохраняем дерево
            ordered_nodes = sel_tree.ordered_nodes
            structure = sel_tree.structure
            RedisSourceService.insert_tree(
                structure, ordered_nodes, source, update_joins=False)

            # если совсем нет ошибок ни у кого, то на клиенте перерисуем дерево,
            # на всякий пожарный
            data['draw_table'] = RedisSourceService.get_final_info(
                ordered_nodes, source)

            # работа с последней таблицей
            remain = RedisSourceService.get_last_remain(source_key)
            if remain == right_table:
                # удаляем инфу о таблице без связи, если она есть
                RedisSourceService.delete_last_remain(source_key)

        return data

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
    def get_columns_types(cls, source, tables):
        """
        Redis
        Получение типов колонок таблиц

        Args:
            source(core.models.Datasource): Источник
            tables(): Описать

        Returns:
            Описать
        """
        # FIXME: Описать
        cols_types = {}

        for table in tables:
            t_cols = json.loads(
                RedisSourceService.get_table_full_info(source, table))['columns']
            for col in t_cols:
                cols_types[u'{0}.{1}'.format(table, col['name'])] = col['type']

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
                            if col['is_primary']:
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
