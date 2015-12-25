# coding: utf-8
from core.models import DatasourceMeta, DatasourceMetaKeys
from etl.services.db.factory import DatabaseService
from etl.services.datasource.repository.storage import RedisSourceService
from etl.models import TablesTree, TableTreeRepository
from core.helpers import get_utf8_string
from django.conf import settings
from itertools import groupby

import json


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
        :type source: Datasource
        """
        tables = DatabaseService.get_tables(source)

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
            ответа см. `RedisSourceService.get_final_info`
        """
        col_records, index_records, const_records = (
            DatabaseService.get_columns_info(source, tables))

        stat_records = DatabaseService.get_stats_info(source, tables)

        cols, indexes, foreigns = DatabaseService.processing_records(
            source, col_records, index_records, const_records)

        if not settings.USE_REDIS_CACHE:
            return []

        RedisSourceService.insert_columns_info(
            source, tables, cols, indexes, foreigns, stat_records)

        # выбранные ранее таблицы в редисе
        active_tables = RedisSourceService.get_active_list(
            source.user_id, source.id)

        # работа с деревьями
        if not active_tables:
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
        Получение списка значений указанных колонок и таблиц в выбранном источнике данных

        :param source: Datasource
        :param cols: list
        :return: list
        """
        structure = RedisSourceService.get_active_tree_structure(source)
        return DatabaseService.get_rows(source, cols, structure)

    @classmethod
    def remove_tables_from_tree(cls, source, tables):
        """
        удаление таблиц из дерева
        :param source:
        :param tables:
        """
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
        remain = RedisSourceService.get_last_remain(
            source.user_id, source.id)
        return remain == child_table

    @classmethod
    def get_columns_and_joins_for_join_window(
        cls, source, parent_table, child_table, has_warning):
        """
        список колонок и джойнов таблиц для окнв связей таблиц
        :param source:
        :param parent_table:
        :param child_table:
        :param has_warning:
        :return:
        """

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
        проверяет пришедшие джойны на совпадение типов
        :param source:
        :param left_table:
        :param right_table:
        :param joins:
        :return:
        """
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
        сохранение новых джойнов
        :param source:
        :param left_table:
        :param right_table:
        :param join_type:
        :param joins:
        :return:
        """
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

    @classmethod
    def get_columns_types(cls, source, tables):
        """
        типы колонок таблиц
        :param source:
        :param tables:
        :return:
        """
        types_dict = {}

        for table in tables:
            t_cols = json.loads(
                RedisSourceService.get_table_full_info(source, table))['columns']
            for col in t_cols:
                types_dict['{0}.{1}'.format(table, col['name'])] = col['type']

        return types_dict

    # fixme: не используется
    @classmethod
    def get_separator(cls, source):
        return DatabaseService.get_separator(source)

    @classmethod
    def get_table_create_query(cls, local_instance, key_str, cols_str):
        return DatabaseService.get_table_create_query(
            local_instance, key_str, cols_str)

    @classmethod
    def get_table_insert_query(cls, local_instance, source_table_name):
        return DatabaseService.get_table_insert_query(
            local_instance, source_table_name)

    @classmethod
    def get_rows_query_for_loading_task(cls, source, structure, cols):
        """
        Получение предзапроса данных указанных
        колонок и таблиц для селери задачи
        :param source:
        :param structure:
        :param cols:
        :return:
        """

        rows_query = DatabaseService.get_rows_query(source, cols, structure)
        return rows_query

    @classmethod
    def check_existing_table(cls, table_name):
        """
        проверка существования таблицы с именем при создании
        :param table_name:
        :return:
        """
        from django.db import connection
        return table_name in connection.introspection.table_names()

    @classmethod
    def get_source_connection(cls, source):
        """
        Получить объект соединения источника данных
        :type source: Datasource
        """
        return DatabaseService.get_connection(source)

    @classmethod
    def get_local_instance(cls):
        """
        возвращает инстанс локального хранилища данных(Postgresql)
        :rtype : object Postgresql()
        :return:
        """
        return DatabaseService.get_local_instance()

    @classmethod
    def tables_info_for_metasource(cls, source, tables):
        """
        Достает инфу о колонках, выбранных таблиц,
        для хранения в DatasourceMeta
        :param source: Datasource
        :param columns: list список вида [{'table': 'name', 'col': 'name'}]
        """
        tables_info_for_meta = RedisSourceService.tables_info_for_metasource(
            source, tables)
        return tables_info_for_meta

    @staticmethod
    def update_datasource_meta(key, source, cols,
                               tables_info_for_meta, last_row):
        """
        Создание DatasourceMeta для Datasource

        Args:
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
            res.update({
                table: source_meta.id
            })
        return res

    @classmethod
    def get_structure_rows_number(cls, source, structure,  cols):
        """
        возвращает примерное кол-во строк в запросе селекта для планирования
        :param source:
        :param structure:
        :param cols:
        :return:
        """
        return DatabaseService.get_structure_rows_number(
            source, structure,  cols)

    @classmethod
    def get_remote_table_create_query(cls, source):
        """
        возвращает запрос на создание таблицы в БД клиента
        """
        return DatabaseService.get_remote_table_create_query(source)

    @classmethod
    def get_remote_triggers_create_query(cls, source):
        """
        возвращает запрос на создание триггеров в БД клиента
        """
        return DatabaseService.get_remote_triggers_create_query(source)
