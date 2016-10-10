# coding: utf-8

from functools import reduce

from core.helpers import get_utf8_string
from core.models import (
    ConnectionChoices, Datasource, Columns, ColumnTypeChoices as ColTC,
    Dataset,
)
from etl.helpers import HashEncoder
from etl.models import TableTreeRepository as TTRepo
from etl.services.clickhouse.helpers import FILTER_QUERIES
from etl.services.datasource.db.factory import (
    DatabaseService, LocalDatabaseService)
from etl.services.datasource.file.factory import FileService
from etl.services.datasource.repository.storage import (
    SourceCacheService, CubeCacheService)
from etl.services.exceptions import SourceUpdateException


class DataSourceService(object):
    """
        Сервис управляет источниками, сервисами БД, Файлов и Редиса!
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

    def __init__(self, source_id):
        """
        Args:
            cube_id(int): id карточки
        """
        self.source_id = source_id
        self.service = self.get_source_service()
        self.cache = SourceCacheService(source_id)

    def get_source_service(self):

        source = Datasource.objects.get(id=self.source_id)
        conn_type = source.conn_type

        if conn_type in self.DB_TYPES:
            return DatabaseService(source)
        elif conn_type in self.FILE_TYPES:
            return FileService(source)
        else:
            raise ValueError("Неизвестный тип подключения!")

    @classmethod
    def get_local_instance(cls):
        """
        Возвращает инстанс локального хранилища данных
        """
        return LocalDatabaseService()

    # FIXME DO
    def delete_datasource(self):
        """
        Удаляет информацию об источнике
        """
        pass

    def get_source_tables(self):
        """
        Список таблиц или листов
        """
        return self.service.get_tables()

    # FIXME check method (especially indents)
    def update_file(self, file):
        """
        Изменение источника,
        предположительно для замены файлов пользователя
        """
        source_id = self.source_id
        source = Datasource.objects.get(id=source_id)

        if source.is_file:
            new_file = file

            if new_file is None:
                raise Exception("What to change if file is empty?")

            columns = Columns.objects.filter(source__source_id=source_id)

            copy = source.source_temp_copy(new_file)

            service = self.service
            service.source = copy

            new_tables = service.get_tables()
            saved_tables = columns.values_list(
                'original_table', flat=True).distinct()

            tables_range = [t for t in saved_tables if t not in new_tables]
            if tables_range:
                raise SourceUpdateException(
                    "Tables {0} removed in new file!".format(tables_range))

            indents = self.get_indentation()
            new_columns = service.fetch_tables_columns(new_tables, indents)

            new_columns = [(t, c['name'])
                           for t in new_columns for c in new_columns[t]]

            saved_columns = columns.filter(
                original_table__in=saved_tables).values_list(
                'original_table', 'original_name')

            columns_range = [tupl for tupl in saved_columns
                             if tupl not in new_columns]

            if columns_range:
                raise SourceUpdateException(
                    "Columns {0} removed in new file!".format(columns_range))
            # если все нормально, то
            # отметим заменяемый файл как старый
            source.mark_file_name_as_old()
            # заменяем файл
            source.file.save(new_file.name, new_file)
            # удаляем временную копию
            copy.remove_temp_file()
        return source

    # FIXME NO USAGE
    def validate_file(self, source):
        """
        Проверка файла на валидность
        """
        if source.is_file:
            source.create_validation_file()
            return self.service.validate()

        return True, None

    # TODO remove to DBServices
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

    def get_indentation(self):
        """
        Достает отступы из хранилища
        Returns: defaultdict(lambda: {'indent': 0, 'header': True})
        """
        return self.cache.get_source_indentation()

    def set_indentation(self, sheet, indent, header):
        """
        Сохраняем отступ для страницы соурса
        """
        indents = self.cache.get_source_indentation()

        indents[sheet]['indent'] = int(indent)
        indents[sheet]['header'] = bool(header)

        self.cache.set_source_indentation(indents)

    def get_source_cubes_relation(self):
        """
        связь источника и предкуба
        предкуб - это инфа в кэше до создания куба(дерево, билдер)
        """
        return self.cache.get_source_cubes()

    def set_source_cubes_relation(self, cube_id):
        """
        связь источника и предкуба
        предкуб - это инфа в кэше до создания куба(дерево, билдер)
        """
        cubes = self.get_source_cubes_relation()
        cubes.append(int(cube_id))
        self.cache.set_source_cubes(cubes)

    def get_source_columns(self, table_name):
        """
        Колонки источника
        """
        service = self.service
        if isinstance(service, DatabaseService):
            return service.fetch_tables_columns([table_name, ])

        indents = self.get_indentation()
        return service.fetch_tables_columns([table_name], indents)

    def get_source_rows(self, table_name):
        """
        Данные источника
        """
        service = self.service
        if isinstance(service, DatabaseService):
            return service.get_source_table_rows(
                table_name, limit=1000, offset=0)

        indents = self.get_indentation()
        return service.get_source_table_rows(table_name, indents=indents)

    def validate_column(self, table, column, col_type):
        """
        Проверка колонки на соответствующий тип typ
        """
        service = self.service

        if isinstance(service, DatabaseService):
            # TODO realize for DBs
            return service.validate_column(table, column, col_type)

        indents = self.get_indentation()
        validation_result = service.validate_column(
            table, column, col_type, indents)
        return validation_result

    def get_columns_info(self, tables):
        """
        Информация о колонках таблицы или страницы
        """
        service = self.service

        if isinstance(service, DatabaseService):
            return service.get_columns_info(tables)

        indents = self.get_indentation()
        return service.get_columns_info(tables, indents)


class DataCubeService(object):
    """
        Сервис управляет кубом!
    """
    # FIXME типы обдумать, особенно даты
    DIMENSION_TYPES = ["text", "date", "time", "timestamp", ]
    MEASURE_TYPES = ["integer", "boolean", ]
    EXCLUDE_TYPES = ["binary", ]

    def __init__(self, cube_id):
        """
        Args:
            cube_id(int): id карточки
        """
        self.cache = CubeCacheService(cube_id)

    def remains_nodes(self):
        """
        Возвращает из остатков список нодов типа etl.models.RemainNode
        """
        builder_data = self.cache.cube_builder_data
        remains = TTRepo.remains_nodes(builder_data)
        return remains

    def get_remain_node(self, node_id):
        """
        Возвращает по node_id из остатков ноду типа etl.models.RemainNode
        """
        builder_data = self.cache.cube_builder_data
        node = TTRepo.get_remain_node(builder_data, node_id)
        return node

    def add_randomly_from_remains(self, node_id):
        """
        Пытаемся связать остаток с деревом в любое место
        """
        cache = self.cache

        node = self.get_remain_node(node_id)

        table, source_id = node.val, node.source_id

        if not cache.check_tree_exists():
            sel_tree = TTRepo.build_single_root(node)
            resave = True
        else:
            # получаем дерево из редиса
            sel_tree = self.get_tree()
            ordered_nodes = sel_tree.ordered_nodes

            tables_info = cache.info_for_tree_building(
                ordered_nodes, node)

            # перестраиваем дерево
            unbinded = sel_tree.build(
                table, source_id, node_id, tables_info)
            resave = unbinded is None

        ordered_nodes = sel_tree.ordered_nodes

        # признак того, что дерево перестроилось
        if resave:
            # сохраняем дерево, если таблицы не в дереве
            cache.transfer_remain_to_actives(node)
            # save tree structure
            cache.save_tree_structure(sel_tree)

        tree_nodes = TTRepo.nodes_info(ordered_nodes)

        remains = self.remains_nodes()
        remain_nodes = TTRepo.nodes_info(remains)

        # determining unbinded tail
        tail_ = (TTRepo.extract_tail(remain_nodes, node_id)
                 if not resave else None)

        return {
            'tree_nodes': tree_nodes,
            'remains': remain_nodes,
            'tail': tail_,
        }

    def from_remain_to_certain(self, parent_id, child_id):
        """
        Добавление из остатков в определенную ноду
        """
        sel_tree = self.get_tree()
        cache = self.cache

        p_node = sel_tree.get_node(parent_id)
        if p_node is None:
            raise Exception("Incorrect parent ID!")

        ch_node = self.get_remain_node(child_id)

        if ch_node is None:
            raise Exception("Incorrect child ID!")

        parent_info = cache.get_table_info(
            p_node.source_id, parent_id)
        child_info = cache.get_table_info(
            ch_node.source_id, child_id)

        is_bind = sel_tree.try_bind_two_nodes(
            p_node, ch_node, parent_info, child_info)

        # если забиндилось
        if is_bind:
            # сохраняем дерево, если таблицы не в дереве
            cache.transfer_remain_to_actives(ch_node)
            # save tree structure
            cache.save_tree_structure(sel_tree)

        tree_nodes = TTRepo.nodes_info(sel_tree.ordered_nodes)

        remains = self.remains_nodes()
        remain_nodes = TTRepo.nodes_info(remains)

        # determining unbinded tail
        tail_ = (TTRepo.extract_tail(remain_nodes, child_id)
                 if not is_bind else None)

        return {
            'tree_nodes': tree_nodes,
            'remains': remain_nodes,
            'tail': tail_,
        }

    def reparent(self, parent_id, child_id):
        """
        Пытаемся перетащить узел дерева из одного места в другое
        Args:
            parent_id(int): id родительского узла
            child_id(int): id узла-потомка
        Returns:
            dict: Информацию об связанных/не связанных узлах дерева и остатка
        """
        sel_tree = self.get_tree()
        cache = self.cache

        p_node = sel_tree.get_node(parent_id)
        if p_node is None:
            raise Exception("Incorrect parent ID!")

        # child node must be in actives
        ch_node = sel_tree.get_node(child_id)

        if ch_node is None:
            raise Exception("Incorrect child ID!")

        parent_info = cache.get_table_info(
            p_node.source_id, parent_id)
        child_info = cache.get_table_info(
            ch_node.source_id, child_id)

        remain = sel_tree.reparent_node(
            p_node, ch_node, parent_info, child_info)

        # если забиндилось
        if remain is None:
            # save tree structure
            cache.save_tree_structure(sel_tree)

        tree_nodes = TTRepo.nodes_info(
            sel_tree.ordered_nodes)

        remain_nodes = self.remains_nodes()
        remains = TTRepo.nodes_info(remain_nodes)

        # determining unbinded tail
        tail_ = (TTRepo.extract_tail(tree_nodes, child_id) if remain else None)

        return {
            'tree_nodes': tree_nodes,
            'remains': remains,
            'tail': tail_,
        }

    def send_nodes_to_remains(self, node_id):
        """
        Перенос нодов дерева в остатки
        """
        cache = self.cache
        sel_tree = self.get_tree()
        node = sel_tree.get_node(node_id)

        # is root
        if node.parent is None:
            cache.transfer_actives_to_remains(sel_tree.ordered_nodes)
            cache.remove_tree()
            tree_nodes = []

        else:
            node_to_remove = sel_tree.remove_sub_tree(node_id)

            to_remain_nodes = sel_tree._get_tree_ordered_nodes(
                [node_to_remove, ])
            # сохраняем дерево, если таблицы не в дереве
            cache.transfer_actives_to_remains(to_remain_nodes)

            # save tree structure
            cache.save_tree_structure(sel_tree)

            tree_nodes = TTRepo.nodes_info(
                sel_tree.ordered_nodes)

        remain_nodes = self.remains_nodes()
        remains = TTRepo.nodes_info(remain_nodes)

        return {
            'tree_nodes': tree_nodes,
            'remains': remains,
        }

    def purge_nodes_from_remains(self, source_id, nodes):
        """
        Удаление из кэша инфы о нодах
        """
        builder = self.cache.cube_builder
        sid = str(source_id)
        remains = builder['data'][sid]['remains']

        for table_name, node_id in nodes.items():
            del remains[table_name]
            self.cache.del_table_info(sid, node_id)

        self.cache.set_cube_builder(builder)

    def get_tree(self):
        """
        Получаем дерево
        """
        # Получаем структуру из Redis и строем дерево
        structure = self.cache.active_tree_structure
        return TTRepo.build_tree_by_structure(structure)

    def get_tree_api(self):
        """
        Строит дерево, если его нет, иначе перестраивает
        """
        # дерева еще нет или все в остатках
        if not self.cache.check_tree_exists():
            tree_nodes = []
            remain_nodes = self.remains_nodes()
            remains = TTRepo.nodes_info(remain_nodes)

            return {
                'tree_nodes': tree_nodes,
                'remains': remains,
            }
        # иначе достраиваем дерево, если можем, если не можем вернем остаток
        sel_tree = self.get_tree()

        ordered_nodes = sel_tree.ordered_nodes
        tree_nodes = TTRepo.nodes_info(ordered_nodes)

        remain_nodes = self.remains_nodes()
        remains = TTRepo.nodes_info(remain_nodes)

        return {
                'tree_nodes': tree_nodes,
                'remains': remains,
            }

    def cache_columns(self, source_id, table):
        """
        Пришедшую таблицу кладем в строительную карту дерева!
        Если юзер указал заранее другой тип колонки, то заменяем на этот тип
        """
        cache = self.cache
        node_id = cache.get_table_id(source_id, table)

        if node_id is not None:
            return node_id

        source_worker = DataSourceService(source_id)

        all_columns, indexes, foreigns, statistics, date_intervals = (
            source_worker.get_columns_info([table, ]))

        # заменяем типы колонок, указанные заранее
        all_table_columns = all_columns[table]
        table_settings = cache.get_cube_so_settings(source_id)['tables'][table]

        # те колонки, которые выбраны для параметров,
        # для них указан свой тип! проставляем соответствующий тип
        # так же проставим дефолтные значения для пустых ячеек источника
        for column in all_table_columns:
            for col_name in table_settings:
                if column['name'] == col_name:
                    setted_type = table_settings[col_name]['type']
                    if setted_type is not None:
                        column['type'] = setted_type

                    setted_default = table_settings[col_name]['default']
                    if setted_default is not None:
                        column['default'] = setted_default

        info = {
            "value": table,
            "sid": source_id,
            "columns": all_table_columns,
            "indexes": indexes[table],
            "foreigns": foreigns[table],
            "stats": statistics[table],
            "date_intervals": date_intervals.get(table, [])
        }

        # кладем информацию во временное хранилище о связи источника и предкуба
        # предкуб - это инфа в кэше до создания куба(дерево, билдер)
        # source_worker.set_source_cubes_relation(self.cache.cube_id)

        # кладем информацию во временное хранилище о таблице источника
        return cache.fill_cache(source_id, table, info)

    def get_columns_and_joins(self, parent_id, child_id):
        """
        """
        parent = self.get_node(parent_id)
        child = self.get_node(child_id)
        parent_sid, parent_table = parent.source_id, parent.val
        child_sid, child_table = child.source_id, child.val

        columns = self.cache.get_columns_for_joins(
            parent_table, parent_sid, child_table, child_sid)

        tree = self.get_tree()

        join_type, cols_info = TTRepo.get_joins(tree, parent_id, child_id)

        return {
            'columns': columns,
            'join_type': join_type,
            'joins': cols_info
        }

    def check_new_joins(self, parent, child, joins):
        """
        Redis
        Проверяет пришедшие джойны на совпадение типов
        Args:
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

        cols_types = self.get_columns_types(parent, child)

        # список джойнов с неверными типами
        error_joins = []
        good_joins = []

        for j in joins_set:
            l_c, _, r_c = j
            if (cols_types['{0}.{1}.{2}'.format(parent.val, l_c, parent.source_id)] !=
                    cols_types['{0}.{1}.{2}'.format(
                        child.val, r_c, child.source_id)]):
                error_joins.append(j)
            else:
                good_joins.append(j)

        return good_joins, error_joins, joins_set

    # TODO ответ о результате отсылать
    def save_new_joins(self, parent_node, child_node, join_type, joins):
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
        good_joins, error_joins, joins_set = self.check_new_joins(
            parent=parent_node, child=child_node, joins=joins,
        )

        if not error_joins:
            # Получаем дерево
            sel_tree = self.get_tree()
            cache = self.cache

            sel_tree.update_node_joins(parent=parent_node, child=child_node,
                                       join_type=join_type, joins=joins_set)
            cache.save_tree_structure(sel_tree)

            if self.check_node_id_in_remains(child_node.node_id):
                cache.transfer_remain_to_actives(child_node)

        return {}

    def get_columns_types(self, parent, child):
        """
        Redis
        Получение типов колонок таблиц
        Args:
            parent(Node): родительский узел
            child(RemainNode): дочерний узел
        Returns:
            dict: Информация о типах столбцов род и дочерней таблицы
        """
        # FIXME: Описать
        cols_types = {}
        cache = self.cache

        for node in [parent, child]:
            t_cols = cache.get_table_info(
                node.source_id, node.node_id)['columns']

            for col in t_cols:
                cols_types['{0}.{1}.{2}'.format(
                    node.val, col['name'], node.source_id)] = col['type']

        return cols_types

    # FIXME not used
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

    # FIXME not used
    def split_nodes_by_source_types(self, sub_trees):
        """
        Если связки типа файл, то делим дальше, до примитива,
        если связка типа бд, то связку таблиц оставляем, как единую сущность
        """
        new_sub_trees = []

        for sub_tree in sub_trees:
            sid = sub_tree['sid']
            worker = DataSourceService(sid)
            service = worker.service

            if isinstance(service, DatabaseService):
                sub_tree['type'] = 'db'
                new_sub_trees += [sub_tree, ]

            elif isinstance(service, FileService):
                new_sub_trees += self.split_file_sub_tree(sub_tree)

        return new_sub_trees

    def prepare_sub_trees(self, sources_info):
        """
        Подготавливает список отдельных сущностей для закачки в монго
        columns_info = {
            '5':
                {
                    "Таблица1": ['name', 'gender', 'age'],
                    "Таблица2": ['name']
                },
            '3':
                {
                    'shops': ['name']
                }
        }
        """
        # разделение дерева по нодам
        tree_structure = self.cache.active_tree_structure
        tree = TTRepo.build_tree_by_structure(tree_structure)
        sub_trees = tree.nodes_structures

        # инфа о колонках для каджой ноды, инфа о датах
        self.prepare_sub_tree_info(sub_trees, sources_info)

        # хэш для каждой ноды
        self.create_sub_tree_hash_names(sub_trees)

        return sub_trees

    def prepare_sub_tree_info(self, sub_trees, sources_info):
        """
        порядок колонок выставляем как в источнике,
        достаем информацию о типе, длине колонке,
        достаем информацию о датах
        """
        cache = self.cache

        for sub_tree in sub_trees:

            sid = sub_tree['sid']
            table_id = sub_tree['node_id']
            table = sub_tree['val']

            # отступы
            worker = DataSourceService(sid)
            sub_tree['indents'] = worker.get_indentation()

            table_info = cache.get_table_info(sid, table_id)

            # инфа о датах
            # sub_tree['date_intervals'] = table_info['date_intervals']

            # инфа о колонках
            columns = list()
            come_columns = sources_info[str(sid)][table]
            for col_ind, col_info in enumerate(table_info['columns']):
                if col_info['name'] in come_columns:
                    col_info['order'] = col_ind
                    columns.append(col_info)

            sub_tree['columns'] = columns

    def create_sub_tree_hash_names(self, sub_trees):
        """
        Для каждого набора, учитывая таблицы/листы и колонки высчитывает хэш
        """
        for sub_tree in sub_trees:

            table_hash = abs(HashEncoder.encode(sub_tree['val']))
            full_hash = "{0}_{1}_{2}".format(
                self.cache.cube_id, sub_tree['sid'], table_hash)

            sub_tree['collection_hash'] = full_hash

            for column in sub_tree['columns']:
                column['hash'] = "{0}_{1}".format(
                    full_hash, abs(HashEncoder.encode(column['name'])))

    def prepare_relations(self, sub_trees):
        """
        Из Foreign таблиц в последствии строятся вьюхи,
        Для них строим список связей Foreign таблиц из Postgres,
        и достаем для каждой вьюхи список колонок,
        делим колонки на меры и размерности

        Возвращает
        [{u'columns': [u'"view_1_2_915339346089779332"."id"',
                           u'"view_1_2_915339346089779332"."name"'],
             u'dimension_columns': [u'"view_1_2_915339346089779332"."name"'],
             u'measure_columns': [u'"view_1_2_915339346089779332"."id"'],
             u'view_name': u'view_1_2_915339346089779332'
            },
            {u'columns': [u'"view_1_2_4284056851867979717"."id"',
                          u'"view_1_2_4284056851867979717"."group_id"',
                        u'"view_1_2_4284056851867979717"."permission_id"'],
            u'conditions': [{u'l': u'"view_1_2_915339346089779332"."id"',
                            u'operation': u'eq',
                            u'r': u'"view_1_2_4284056851867979717"."group_id"'}],
            u'dimension_columns': [],
            u'measure_columns': [u'"view_1_2_4284056851867979717"."id"',
                                 u'"view_1_2_4284056851867979717"."group_id"',
                                 u'"view_1_2_4284056851867979717"."permission_id"'],
            u'type': u'inner',
            u'view_name': u'view_1_2_4284056851867979717'}]
        """

        def dim_meas_columns(tree, types):
            """
            Возвращает список колонок вьюхи либо для мер, либо для размерностей
            в зависимости от типа
            """
            return [
                VIEW_COL_SEL.format(
                    VIEW_PREFIX, tree['collection_hash'], column['name'], column['click_column'])
                for column in tree['columns'] if column['type'] in types]

        def extract_columns(tree):
            """
            Возвращает список всех колонок вьюхи
            """
            return [
                VIEW_COL_SEL.format(
                    VIEW_PREFIX, tree['collection_hash'], column['name'], column['click_column'])
                for column in tree['columns']]

        # префикс для названия вьюхи, осздаваемая на foreign table
        VIEW_PREFIX = "view_"
        # название вьюхи
        VIEW_NAME = "view_{0}"
        # вспомогательный ключ для определения хэша таблицы для ее колонки
        HASH_STR = "{0}_{1}_{2}"
        # строка названия вьюхи и ее колонки для селекта
        VIEW_COL_SEL = '"{0}{1}"."{2}" as "{3}"'
        # строка названия вьюхи и ее колонки для джойнов
        VIEW_COL_JOIN = '"{0}{1}"."{2}"'

        tables_hash_map = {}
        relations = []

        for sub in sub_trees:
            sid = sub['sid']
            table = sub['val']
            hash_ = sub['collection_hash']

            for column in sub['columns']:
                name = HASH_STR.format(sid, table, column['name'])
                tables_hash_map[name] = hash_

        # голова дерева без связей
        main = sub_trees[0]
        main_hash = main['collection_hash']

        relations.append({
            "view_name": VIEW_NAME.format(main_hash),
            "dimension_columns": dim_meas_columns(main, self.DIMENSION_TYPES),
            "measure_columns": dim_meas_columns(main, self.MEASURE_TYPES),
            "columns": extract_columns(main),
        })

        for sub in sub_trees[1:]:
            table = sub['val']
            join = sub["joins"][0]
            # ищем другую таблицу-родителя, с которой связана table
            parent_info = (join["left"] if join['left']['table'] != table
                           else join['right'])

            parent_hash_str = HASH_STR.format(
                parent_info['sid'], parent_info["table"], parent_info["column"])

            child_hash = sub['collection_hash']

            rel = {
                "view_name": VIEW_NAME.format(child_hash),
                "type": join["join"]["type"],
                "dimension_columns": dim_meas_columns(sub, self.DIMENSION_TYPES),
                "measure_columns": dim_meas_columns(sub, self.MEASURE_TYPES),
                "columns": extract_columns(sub),
                "conditions": [],
            }
            # хэш таблицы, с котрой он связан
            parent_hash = tables_hash_map[parent_hash_str]

            # условия соединений таблиц
            for join in sub["joins"]:

                parent, child = (
                    (join["left"], join["right"])
                    if join['left']['table'] != table
                    else (join['right'], join["left"]))

                rel["conditions"].append({
                    "l": VIEW_COL_JOIN.format(
                        VIEW_PREFIX, parent_hash, parent["column"]),
                    "r": VIEW_COL_JOIN.format(
                        VIEW_PREFIX, child_hash, child["column"]),
                    "operation": join["join"]["value"],
                })

            relations.append(rel)

        return relations

    def get_node(self, node_id):
        """
        Получение данных по узлу
        Arguments:
            node_id(int): id узла
        Returns:
            Node
        """
        node = None

        if not self.check_node_id_in_remains(node_id):
            sel_tree = self.get_tree()
            node = sel_tree.get_node(node_id)
        else:
            remains = self.remains_nodes()
            for remain in remains:
                if int(remain.node_id) == int(node_id):
                    node = remain
                    break
        if node is None:
            raise Exception("Bull shit!")
        return node

    def get_node_info(self, node_id):
        node = self.get_node(node_id)
        table, source_id = node.val, node.source_id
        table_info = self.cache.get_table_info(source_id, node.node_id)

        return dict(
            node_id=node.node_id,
            parent_id=getattr(node.parent, 'node_id', None),
            sid=source_id,
            val=table,
            cols=[{'col_name': x['name'], 'col_title': x.get('title', None),}
                  for x in table_info['columns']
                  ])

    def check_node_id_in_remains(self, node_id):
        """
        Проверяет есть ли данный id в билдере карты в остатках
        """
        node_id = int(node_id)
        b_data = self.cache.cube_builder_data

        for sid in b_data:
            s_data = b_data[sid]
            # проверка в остатках
            for remain, remain_id in s_data['remains'].items():
                if int(remain_id) == node_id:
                    return True
        return False

    def check_node_id_in_builder(self, node_id, in_remain=True):
        """
        Проверяет есть ли данный id в билдере карты в активных или остатках
        """
        node_id = int(node_id)
        b_data = self.cache.cube_builder_data

        for sid in b_data:
            s_data = b_data[sid]
            # проверка в остатках
            if in_remain:
                for remain, remain_id in s_data['remains'].items():
                    if int(remain_id) == node_id:
                        return True
            # проверка в активных
            else:
                for active, active_id in s_data['actives'].items():
                    if int(active_id) == node_id:
                        return True
        return False

    def check_sids_exist(self, sids):
        """
        Проверяем наличие source id в кэше
        """
        data = self.cache.cube_builder_data

        cached_sids = [str(i) for i in list(data.keys())]
        sids = [str(i) for i in sids]
        uncached = [x for x in sids if x not in cached_sids]

        return uncached

    # FIXME cache_keys тут не место, логику перенести в CubeCacheService
    def check_cached_data(self, sources_info):
        """
        Проверяем наличие таблиц, ключей и колонок в кэше
        """
        cache = self.cache
        data = cache.cube_builder_data

        uncached_tables = []
        uncached_keys = []
        uncached_columns = []

        for sid, info in sources_info.items():
            for table, columns in info.items():
                table_id = cache.get_table_id(sid, table, data=data)
                # таблицы нет в билдере
                if table_id is None:
                    uncached_tables.append((sid, table))
                else:
                    table_info_key = cache.cache_keys.table_key(sid, table_id)

                    # нет информации о таблице
                    if not cache.r_exists(table_info_key):
                        uncached_keys.append((sid, table))
                    else:
                        table_info = cache.get_table_info(sid, table_id)
                        column_names = [x["name"] for x in table_info["columns"]]
                        # нет информации о колонках
                        range_ = [c for c in columns if c not in column_names]
                        if range_:
                            uncached_columns.append((sid, table, range_))

        return uncached_tables, uncached_keys, uncached_columns

    def check_tables_with_tree_structure(self, columns_info):
        """
        Проверяем наличие всех таблиц из дерева в пришедших на загрузку
        columns_info = {
            8':
                {
                    "mrk_reference": ["pubmedid", "creation_date"],
                },
            '1':
                {
                    "Лист1": [
                        "name2", "пол", "auth_group_id", "Date", "Floata", ],
                },
        }
        Возвращает список таблиц, которые есть в дереве, но не пришедшие
        к нам перед загрузкой
        """
        cache = self.cache

        tree_structure = cache.active_tree_structure
        tree = TTRepo.build_tree_by_structure(tree_structure)
        tree_nodes = tree.ordered_nodes

        tree_table_names = [
            (int(node.source_id), node.val) for node in tree_nodes]
        came_table_names = reduce(
            list.__add__,
            [[(int(sid), t) for t in list(tables.keys())]
             for sid, tables in columns_info.items()], [])

        range_ = [table_tupl for table_tupl in tree_table_names if
                  table_tupl not in came_table_names]
        return range_

    def get_cube_columns(self):
        """
        Список колонок для фильтров куба
        """
        cube_id = self.cache.cube_id

        filter_types = ColTC.filter_types()
        measure_types = ColTC.measure_types()

        local_service = DataSourceService.get_local_instance()

        context = Dataset.objects.get(key=cube_id).context
        click_table = context['warehouse']

        columns = Columns.objects.filter(source__dataset__key=cube_id)

        filters = columns.filter(type__in=filter_types).values(
            'original_name', 'name', 'original_table',
            'type', 'source_id', 'dataset__key')

        measures = columns.filter(type__in=measure_types).values(
            'original_name', 'name', 'original_table',
            'type', 'source_id', 'dataset__key')

        for col in filters:
            type_name = ColTC.values[int(col['type'])]
            q = FILTER_QUERIES[type_name]
            q = q.format(column_name=col['name'], table_name=click_table)
            col['query'] = q
            col['type'] = type_name

        for col in measures:
            col['type'] = ColTC.values[int(col['type'])]

        filters2 = []
        measures2 = []

        for column in filters:
            json_resp = local_service.fetchall(column['query'])
            # values = reduce(
            #     list.__add__, [k.values() for k in json_resp['data']], [])
            values = [k[0] for k in json_resp]

            filters2.append({
                'cube_id': column['dataset__key'],
                'source_id': column['source_id'],
                'table_name': column['original_table'],
                'column_name': column['original_name'],
                'click_column_name': column['name'],
                'column_type': column['type'],
                'values': values,
            })

        for column in measures:
            measures2.append({
                'cube_id': column['dataset__key'],
                'source_id': column['source_id'],
                'table_name': column['original_table'],
                'column_name': column['original_name'],
                'click_column_name': column['name'],
                'column_type': column['type'],
            })

        return {'filters': filters2, 'measures': measures2}

    def validate_column(self, source_id, table, column, param, type):
        """
        Проверка колонки на соответствующий тип typ
        """
        source_worker = DataSourceService(source_id)
        result = source_worker.validate_column(table, column, type)

        if not result['errors']:
            # save valid type of column for source for current cube
            self.cache.set_cube_so_column_type(
                source_id, table, column, param, type)

        return result

    def set_column_default(self, source_id, table, column, default):
        """
        Установка дефолтного значения для пустых значений колонки,
        либо удаление таких строк
        """
        self.cache.set_cube_so_column_default(source_id, table, column, default)

    def purge_cube_source_cache(self, source_id):
        """
        Удаление источника из кэша предкуба
        """
        sid = str(source_id)
        b_data = self.cache.cube_builder_data

        if sid in b_data:
            actives = b_data[sid]['actives']
            remains = b_data[sid]['remains']

            for node_id in actives.values():
                if self.check_node_id_in_builder(node_id, in_remain=False):
                    self.send_nodes_to_remains(node_id)

            all_remains = dict()
            all_remains.update(actives)
            all_remains.update(remains)

            self.purge_nodes_from_remains(sid, all_remains)

        self.cache.del_cube_so_settings(sid)

    def delete_source(self, source_id):
        """
        Удаление источника из кэша недокуба или же из куба загруженного
        """
        # загруженные кубы
        # cubes = Dataset.objects.filter(
        #     columns__source=90).distinct().values_list('key', flat=True)
        # loaded_cubes = [int(key) for key in cubes]
        # source_worker = DataSourceService(source_id)
        # # предкубы, в которых участвует данный источник
        # cached_cubes = source_worker.get_source_cubes_relation()

        self.purge_cube_source_cache(source_id)
