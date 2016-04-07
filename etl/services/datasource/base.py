# coding: utf-8
from core.models import (DatasourceMeta, DatasourceMetaKeys, DatasetToMeta,
                         DatasourcesJournal)
from etl.services.datasource.repository import r_server
from etl.services.db.factory import DatabaseService
from etl.services.datasource.repository.storage import RedisSourceService
from etl.models import TablesTree, TableTreeRepository
from core.helpers import get_utf8_string
from django.conf import settings
from itertools import groupby
from django.db import transaction
import json


class DataSourceService(object):
    """
    Сервис управляет сервисами БД и Редиса
    """

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

    @staticmethod
    def get_database_info(source):
        """
        Возвращает таблицы истоника данных.
        Фильтрация таблиц по факту создания раннее триггеров

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            dict: {
                db: <str>,
                host: <str>,
                tables: [{'name': <table_name_1>, 'name': <table_name_2>}]
            }


        """
        # FIXME: Описать ответ
        tables = DatabaseService.get_tables(source)

        trigger_tables = DatasourcesJournal.objects.filter(
            trigger__datasource=source).values_list('name', flat=True)

        # фильтруем, не показываем таблицы триггеров
        tables = filter(lambda x: x['name'] not in trigger_tables, tables)

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
            'conn_type': get_utf8_string(post.get('conn_type')),
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

        if not settings.USE_REDIS_CACHE:
                return []

        new_tables, old_tables = RedisSourceService.filter_exists_tables(
            source, tables)

        if new_tables:
            col_records, index_records, const_records = (
                DatabaseService.get_columns_info(source, new_tables))

            stat_records = DatabaseService.get_stats_info(source, new_tables)

            new_tables_intervals = DatabaseService.get_date_intervals(
                source, col_records)

            cols, indexes, foreigns = DatabaseService.processing_records(
                source, col_records, index_records, const_records)

            RedisSourceService.insert_columns_info(
                source, new_tables, cols, indexes,
                foreigns, stat_records, new_tables_intervals)

        if old_tables:
            # берем колонки старых таблиц
            all_columns = DatabaseService.fetch_tables_columns(source, tables)
            old_tables_intervals = DatabaseService.get_date_intervals(source, all_columns)
            # актуализируем интервалы дат (min, max) для таблиц с датами
            RedisSourceService.insert_date_intervals(
                source, old_tables, old_tables_intervals)

        # существование дерева
        tree_exists = RedisSourceService.check_tree_exists(
            source.user_id, source.id)

        # работа с деревьями
        if not tree_exists:
            trees, without_bind = TableTreeRepository.build_trees(tuple(tables), source)
            sel_tree = TablesTree.select_tree(trees)

            remains = without_bind[sel_tree.root.val]
        else:
            # достаем структуру дерева из редиса
            structure = RedisSourceService.get_active_tree_structure(source)
            # строим дерево
            sel_tree = TablesTree.build_tree_by_structure(structure)

            ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])

            tables_info = RedisSourceService.info_for_tree_building(
                ordered_nodes, tables, source)

            # перестраиваем дерево
            remains = TablesTree.build_tree(
                [sel_tree.root, ], tuple(tables), tables_info)

        # таблица без связи
        last = RedisSourceService.insert_remains(source, remains)

        # сохраняем дерево
        structure = TablesTree.get_tree_structure(sel_tree.root)
        ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])
        RedisSourceService.insert_tree(structure, ordered_nodes, source)

        return RedisSourceService.get_final_info(ordered_nodes, source, last)

    @classmethod
    def get_rows_info(cls, source, cols):
        """
        redis
        Получение списка значений указанных колонок и таблиц в выбранном источнике данных


        Args:
            source(core.models.Datasource): Источник
            cols(list): Описать

        Returns:
            list Описать
        """
        structure = RedisSourceService.get_active_tree_structure(source)
        return DatabaseService.get_rows(source, cols, structure)

    @classmethod
    def remove_tables_from_tree(cls, source, tables):
        """
        Redis
        удаление таблиц из дерева

        Args:
            source(core.models.Datasource): Источник
            tables(): Описать
        """
        # FIXME: Описать аргумент tables
        # достаем структуру дерева из редиса
        structure = RedisSourceService.get_active_tree_structure(source)
        # строим дерево
        sel_tree = TablesTree.build_tree_by_structure(structure)
        TableTreeRepository.delete_nodes_from_tree(sel_tree, source, tables)

        if sel_tree.root:
            RedisSourceService.delete_tables(source, tables)

            ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])
            structure = TablesTree.get_tree_structure(sel_tree.root)
            RedisSourceService.insert_tree(structure, ordered_nodes, source, update_joins=False)

    @classmethod
    def check_is_binding_remain(cls, source, child_table):
        # FIXME: Описать
        remain = RedisSourceService.get_last_remain(
            source.user_id, source.id)
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
            if (cols_types['{0}.{1}'.format(left_table, l_c)] !=
                    cols_types['{0}.{1}'.format(right_table, r_c)]):
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
            sel_tree = TablesTree.build_tree_by_structure(structure)

            TablesTree.update_node_joins(
                sel_tree, left_table, right_table, join_type, joins_set)

            # сохраняем дерево
            ordered_nodes = TablesTree.get_tree_ordered_nodes([sel_tree.root, ])
            structure = TablesTree.get_tree_structure(sel_tree.root)
            RedisSourceService.insert_tree(
                structure, ordered_nodes, source, update_joins=False)

            # если совсем нет ошибок ни у кого, то на клиенте перерисуем дерево,
            # на всякий пожарный
            data['draw_table'] = RedisSourceService.get_final_info(
                ordered_nodes, source)

            # работа с последней таблицей
            remain = RedisSourceService.get_last_remain(
                source.user_id, source.id)
            if remain == right_table:
                # удаляем инфу о таблице без связи, если она есть
                RedisSourceService.delete_last_remain(source)

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
                cols_types['{0}.{1}'.format(table, col['name'])] = col['type']

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

    # fixme: не используется?
    @classmethod
    def get_separator(cls, source):
        """
        Database
        Args:
            source(core.models.Datasource): Источник

        Returns:
            Описать
        """
        return DatabaseService.get_separator(source)

    @classmethod
    def get_table_create_query(cls, table_name, cols_str):
        """
        local db
        Запрос на создание таблицы

        Args:
            table_name(unicode): Название таблицы
            cols_str(unicode):

        Returns:
            str: Строка запроса
        """
        return DatabaseService.get_table_create_query(table_name, cols_str)

    @classmethod
    def check_table_exists_query(cls, local_instance, table_name, db):
        # FIXME: Описать
        return DatabaseService.check_table_exists_query(
            local_instance, table_name, db)

    @classmethod
    def get_page_select_query(cls, table_name, cols):
        """
        Формирование строки запроса на получение данных (с дальнейшей пагинацией)

        Args:
            table_name(unicode): Название таблицы
            cols(list): Список получаемых колонок

        Returns:
            str: Строка запроса
        """

        return DatabaseService.get_page_select_query(table_name, cols)

    @classmethod
    def get_table_insert_query(cls, source_table_name, cols_num):
        return DatabaseService.get_table_insert_query(
            source_table_name, cols_num)

    @classmethod
    def get_source_rows_query(cls, source, structure, cols):
        """
        source db
        Получение предзапроса данных указанных
        колонок и таблиц для селери задачи
        :param source:
        :param structure:
        :param cols:
        :return:
        """
        # FIXME: Описать
        rows_query = DatabaseService.get_rows_query(source, cols, structure)
        return rows_query

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
        return DatabaseService.get_connection(source)

    @classmethod
    def get_local_instance(cls):
        """
        возвращает инстанс локального хранилища данных

        Returns:

        """
        return DatabaseService.get_local_instance()

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

    @classmethod
    def get_structure_rows_number(cls, source, structure,  cols):
        """
        Source db
        Возвращает примерное кол-во строк в запросе селекта для планирования

        Args:
            source(core.models.Datasource): Источник данных
            structure():
            cols():

        Returns:
            Описать
        """

        # FIXME: Описать
        return DatabaseService.get_structure_rows_number(
            source, structure,  cols)

    @classmethod
    def get_remote_table_create_query(cls, source):
        """
        Source db
        Возвращает запрос на создание таблицы в БД клиента

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            str: Строка запроса
        """

        return DatabaseService.get_remote_table_create_query(source)

    @classmethod
    def get_remote_triggers_create_query(cls, source):
        """
        Source db
        Возвращает запрос на создание триггеров в БД клиента

        Args:
            source(core.models.Datasource): Источник данных

        Returns:
            str: Строка запроса
        """
        # FIXME: Описать
        return DatabaseService.get_remote_triggers_create_query(source)

    @staticmethod
    def reload_datasource_trigger_query(params):
        """
        Local db
        Запрос на создание триггеров в БД локально для размерностей и мер

        Args:
            params(dict): Параметры, необходимые для запроса

        Returns:
            str: Строка запроса
        """
        # FIXME: Описать "Параметры, необходимые для запроса"

        return DatabaseService.reload_datasource_trigger_query(params)

    @staticmethod
    def get_date_table_names(col_type):
        """
        Local db
        Получение запроса на создание колонок таблицы дат

        Args:
            col_type(dict): Соответсвие название поля и типа

        Returns:
            list: Список строк с названием и типом колонок для таблицы дат
        """
        return DatabaseService.get_date_table_names(col_type)

    @staticmethod
    def get_table_create_col_names(fields, ref_key):
        """
        Список строк запроса для создания колонок
        таблицы sttm_, мер и размерностей

        Args:
            fields(): Информация о колонках таблицы
            ref_key(str): идентификатор для создания внешнего ключа

        Returns:
            list: Список строк с названием и типом колонок
            для таблицы мер и размерности
        """
        return DatabaseService.get_table_create_col_names(fields, ref_key)

    @staticmethod
    def cdc_key_delete_query(table_name):
        """
        Local db
        Запрос на удаление записей по cdc-ключу

        Args:
            table_name(unicode): Название таблицы

        Returns:
            str: Строка запроса
        """
        return DatabaseService.cdc_key_delete_query(table_name)

    @staticmethod
    def get_fetchall_result(connection, source, query, *args, **kwargs):
        """
        возвращает результат fetchall преобразованного запроса с аргументами
        """
        return DatabaseService.get_fetchall_result(
            connection, source, query, *args, **kwargs)
