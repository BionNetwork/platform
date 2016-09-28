# coding: utf-8


from . import r_server
import json
from collections import defaultdict
from core.helpers import CustomJsonEncoder


# coding for redis client
UTF = "utf-8"


class SourceCacheKeys(object):
    """Ключи кэша источника"""

    def __init__(self, source_id):
        self.source_id = source_id

    @property
    def source_key(self):
        """
        Источник данных
        """
        return 'source:{0}'.format(self.source_id)

    def indents_key(self):
        """
        Ключ отступа
        """
        return '{0}:indents'.format(self.source_key)


class CubeCacheKeys(object):
    """Ключи кэша куба"""

    def __init__(self, cube_id):
        self.cube_id = cube_id

    @staticmethod
    def source_key(source_id):
        """
        Источник данных
        """
        return 'source:{0}'.format(source_id)

    @property
    def cube_key(self):
        """
        Карточка
        """
        return 'cube:{0}'.format(self.cube_id)

    def cube_builder_key(self):
        """
        Счетчик для коллекций пользователя (автоинкрементное значение)
        """
        return '{0}:builder'.format(self.cube_key)

    def table_key(self, source_id, table_id):
        """
        Полная информация таблицы, которая в дереве
        """
        return '{0}:{1}:collection:{2}'.format(
            self.cube_key, self.source_key(source_id), table_id)

    def tree_key(self):
        """
        Структура дерева
        """
        return '{0}:active:tree'.format(self.cube_key)

    def cube_so_settings_key(self, source_id):
        """
        Конфиг источника куба
        """
        return '{0}:{1}:settings'.format(
            self.cube_key, self.source_key(source_id))


class CacheService(object):
    """
    Базовый класс для работы с кэшом
    """
    @staticmethod
    def r_get(name):
        return json.loads(r_server.get(name).decode(UTF))

    @staticmethod
    def r_set(name, structure):
        r_server.set(name, json.dumps(structure, cls=CustomJsonEncoder))

    @staticmethod
    def r_del(name):
        r_server.delete(name)

    @staticmethod
    def r_exists(name):
        return r_server.exists(name)


class SourceCacheService(CacheService):
    """
    Работа с кэшом  источника
    """
    def __init__(self, source_id):
        """
        Args:
            cube_id(int): id карточки
        """
        self.source_id = source_id
        self.cache_keys = SourceCacheKeys(source_id)

    def get_source_indentation(self):
        """
        Получаем отступ для страницы источника
        Returns: defaultdict(int)
        """
        indents_key = self.cache_keys.indents_key()

        func = lambda: {'indent': 0, 'header': True}

        if not self.r_exists(indents_key):
            # first is indent, second is header need
            return defaultdict(func)

        return defaultdict(func, self.r_get(indents_key))

    def set_source_indentation(self, indents):
        """
        Сохраняем отступ для страницы соурса
        """
        indents_key = self.cache_keys.indents_key()
        self.r_set(indents_key, indents)


class CubeCacheService(CacheService):
    """
    Работа с кэшом куба
    """
    def __init__(self, cube_id):
        """
        Args:
            cube_id(int): id карточки
        """
        self.cube_id=cube_id
        self.cache_keys = CubeCacheKeys(cube_id=cube_id)

    def get_table_id(self, source_id, table, data=None):
        """
        Проверяет таблица уже в кэше или нет!
        Возвращает id таблицы или None в случае отсутствия!
        Если вызывается в цикле, то лучше передавать data,
        во избежании лишних вводов-выводов
        """
        builder_data = data or self.cube_builder_data
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
        builder_data = self.cube_builder_data
        s_id = str(source_id)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            return table in source_colls['actives']

        return False

    def check_table_in_remains(self, source_id, table):
        """
        Проверяет таблица в остатках или нет!
        """
        builder_data = self.cube_builder_data
        s_id = str(source_id)

        if s_id in builder_data:
            source_colls = builder_data[s_id]
            return table in source_colls['remains']

        return False

    @property
    def cube_builder(self):
        """
        Строительная карта дерева
        """
        builder_key = self.cache_keys.cube_builder_key()

        if not self.r_exists(builder_key):
            builder = {
                'data': {},
                'next_id': 1,
            }
            self.r_set(builder_key, builder)
            return builder
        return self.r_get(builder_key)

    def set_cube_builder(self, actives):
        """
        Args:
            actives object has structure {
                'data': {},
                'next_id': 1,
            }
        Returns:
        """
        builder_key = self.cache_keys.cube_builder_key()
        return self.r_set(builder_key, actives)

    def del_cube_builder(self):
        """
        Удаляет строительную карта карточки
        """
        builder_key = self.cache_keys.cube_builder_key()
        return self.r_del(builder_key)

    @property
    def cube_builder_data(self):
        """
        """
        return self.cube_builder['data']

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
        builder = self.cube_builder
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
        self.set_cube_builder(builder)

        return next_id

    def get_table_info(self, source_id, table_id):
        table_key = self.cache_keys.table_key(source_id, table_id)
        return self.r_get(table_key)

    def set_table_info(self, source_id, table_id, table_info):
        """
        Args:
            source_id:
            table_id:
            table_info:
        Returns:
        """
        table_key = self.cache_keys.table_key(source_id, table_id)
        return self.r_set(table_key, table_info)

    def del_table_info(self, source_id, table_id):
        """
        Удаляет из кэша инфу о таблице источника
        """
        table_key = self.cache_keys.table_key(source_id, table_id)
        self.r_del(table_key)

    def del_all_tables_info(self):
        """
        Удаляет из кэша инфу всех таблиц источника,
        участвующих в карточке
        """
        builder_data = self.cube_builder_data
        for sid, bundle in builder_data.items():
            t_names = (list(bundle['actives'].values()) +
                       list(bundle['remains'].values()))

            for table_id in t_names:
                self.del_table_info(sid, table_id)

    @property
    def active_tree_structure(self):
        """
        Получение текущей структуры дерева источника
        Returns:
            unicode
        """
        tree_key = self.cache_keys.tree_key()
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
        builder_data = self.cube_builder_data

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
        actives = self.cube_builder_data

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
        b_data = self.cube_builder_data

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
        """
        tree_key = self.cache_keys.tree_key()
        return self.r_exists(tree_key)

    def transfer_remain_to_actives(self, node):
        """
        сохраняем карту дерева, перенос остатка в активные
        """
        builder = self.cube_builder
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

        self.set_cube_builder(builder)

    def save_tree_structure(self, tree):
        """
        сохраняем структуру дерева
        :param tree_structure: string
        """
        tree_key = self.cache_keys.tree_key()
        tree_structure = tree.structure

        self.r_set(tree_key, tree_structure)

    def remove_tree(self):
        """
        Удаляет дерево из хранилища
        """
        tree_key = self.cache_keys.tree_key()
        self.r_del(tree_key)

    def clear_cube_cache(self):
        """
        Удаляет из редиса полную инфу о карточке
        """
        self.remove_tree()
        self.del_all_tables_info()
        self.del_cube_builder()

    def transfer_actives_to_remains(self, to_remain_nodes):
        """
        сохраняем карту дерева, перенос активов в остатки
        """
        builder = self.cube_builder
        b_data = builder['data']

        for node in to_remain_nodes:
            table, sid, node_id = (
                node.val, str(node.source_id), node.node_id)

            s_remains = b_data[sid]['remains']
            s_actives = b_data[sid]['actives']

            del s_actives[table]
            s_remains[table] = node_id

        self.set_cube_builder(builder)

    def get_cube_so_settings(self, source_id):
        """
        Конфиг соурса для определенного куба,
        содержит инфу об отступе, заголовке страницы,
        типе колонки и ее дефолтном значении
        """
        settings_key = self.cache_keys.cube_so_settings_key(source_id)

        func = lambda: {
            'columns': defaultdict(
                lambda: {
                    'type': None,
                    'default': None
                }),
        }

        if not self.r_exists(settings_key):
            return defaultdict(func)

        return defaultdict(func, self.r_get(settings_key))

    def set_cube_so_settings(self, source_id, settings):
        """
        Сохранение конфига соурса куба
        """
        settings_key = self.cache_keys.cube_so_settings_key(source_id)
        self.r_set(settings_key, settings)

    def set_cube_so_column_type(self, source_id, table, column, type):
        """
        Сохранение типа колонки, чтобы в этом типе посадить ее в хранилище
        """
        settings = self.get_cube_so_settings(source_id)
        settings[table][column] = type
        self.set_cube_so_settings(source_id, settings)
