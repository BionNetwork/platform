# coding: utf-8
from __future__ import unicode_literals

import operator
from collections import defaultdict

from functools import reduce

from etl.services.db.interfaces import JoinTypes, Operations


class Node(object):
    """
        Узел дерева таблиц
    """
    def __init__(self, t_name, source_id, parent=None, joins=None,
                 node_id=None, join_type='inner'):
        self.val = t_name
        self.source_id = source_id
        self.parent = parent
        self.childs = []
        self.joins = joins or []
        self.node_id = int(node_id) if node_id else None
        self.join_type = join_type

    def __str__(self):
        return u'Node: %s, parent=%s, sid=%s' % (
            self.val, self.parent, self.source_id)

    def get_node_joins_info(self):
        """
        связи узла
        :return: defaultdict
        """
        node_joins = defaultdict(list)

        n_val = self.val
        for join in self.joins:
            left = join['left']
            right = join['right']
            operation = join['join']
            if n_val == right['table']:
                node_joins[left['table']].append({
                    "left": left, "right": right,
                    "join": operation
                })
            else:
                node_joins[right['table']].append({
                    "left": right, "right": left,
                    "join": operation,
                })
        return node_joins

    def get_node_joins_info_NEW(self):
        """
        связи узла
        :return: defaultdict
        """
        T_S = "T{0}_S{1}"
        node_joins = defaultdict(list)

        n_val = self.val
        for join in self.joins:
            left = join['left']
            right = join['right']
            operation = join['join']
            if n_val == right['table']:
                node_joins[T_S.format(left['table'], left['sid'])].append({
                    "left": left, "right": right,
                    "join": operation
                })
            else:
                node_joins[T_S.format(right['table'], right['sid'])].append({
                    "left": right, "right": left,
                    "join": operation,
                })
        return node_joins

    def api_info(self):
        """
        Публичная информация об узле
        """
        return dict(
            val=self.val,
            sid=self.source_id,
            parent_id=getattr(self.parent, 'node_id', None),
            # without_bind=False,
            node_id=self.node_id,
        )


class RemainNode(Node):

    def __init__(self, t_name, source_id, node_id=None):
        super(RemainNode, self).__init__(
            t_name=t_name, source_id=source_id, node_id=node_id
        )

    def api_info(self):
        """
        Публичная информация об узле
        """
        return dict(
            val=self.val,
            sid=self.source_id,
            parent_id=getattr(self.parent, 'node_id', None),
            # without_bind=True,
            node_id=self.node_id,
        )


class TablesTree(object):
    """
        Дерево Таблиц
    """

    def __init__(self, t_name, source_id, node_id=None):
        self.root = Node(t_name, source_id, node_id=node_id)

    def display(self):
        if self.root:
            print(self.root.val, self.root.joins)
            r_chs = [x for x in self.root.childs]
            print([(x.val, x.joins) for x in r_chs])
            for c in r_chs:
                print([x.val for x in c.childs])
            print(80*'*')
        else:
            print('Empty Tree!!!')

    @property
    def ordered_nodes(self):
        root = self.root
        return self._get_tree_ordered_nodes([root, ])

    @classmethod
    def _get_tree_ordered_nodes(cls, nodes):
        """
        узлы дерева по порядку от корня вниз слева направо
        :param nodes: list
        :return: list
        """
        all_nodes = []
        all_nodes += nodes
        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            all_nodes += cls._get_tree_ordered_nodes(child_nodes)
        return all_nodes

    @property
    def nodes_count_for_levels(self):
        root = self.root
        return self._get_nodes_count_for_levels([root, ])

    @classmethod
    def _get_nodes_count_for_levels(cls, nodes):
        """
        Cписок количества нодов на каждом уровне дерева
        :param nodes: list
        :return: list
        Пример [1, 3, 2, ...]
        """
        counts = [len(nodes)]

        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            counts += cls._get_nodes_count_for_levels(child_nodes)
        return counts

    @property
    def structure(self):
        root = self.root
        return self._get_tree_structure(root)

    @classmethod
    def _get_tree_structure(cls, root):
        """
        структура дерева
        :param root: Node
        :return: dict
        """
        root_info = {'val': root.val, 'childs': [], 'joins': list(root.joins),
                     'sid': root.source_id, 'node_id': root.node_id, }
        root_info['join_type'] = (
            None if not root_info['joins'] else root.join_type)

        for ch in root.childs:
            root_info['childs'].append(cls._get_tree_structure(ch))
        return root_info

    def build(self, table, source_id,  node_id, tables_info):
        remain = self._build(
            [self.root, ], table, source_id,  node_id, tables_info)

        return remain

    def contains(self, table, source_id):

        nodes = self.ordered_nodes
        for node in nodes:
            if node.val == table and int(source_id) == int(node.source_id):
                return True
        return False

    def contains_node_id(self, node_id):
        node_id = int(node_id)
        nodes = self.ordered_nodes
        for node in nodes:
            if int(node.node_id) == node_id:
                return True
        return False

    def get_node(self, node_id):
        """
        Node's info by id
        """
        node_id = int(node_id)
        nodes = self.ordered_nodes
        for node in nodes:
            if node.node_id == node_id:
                return node
        return None

    @classmethod
    def _build(cls, children, table, source_id, node_id, tables_info):
        """
        строит дерево таблиц, возвращает таблицы без связей
        """
        new_children = []

        for child in children:
            new_children += child.childs
            l_nid = child.node_id
            l_info = tables_info[int(l_nid)]

            r_info = tables_info[int(node_id)]
            joins = cls.get_joins(l_info, r_info)

            if joins:
                new_node = Node(table, source_id, parent=child,
                                joins=joins, node_id=node_id)
                child.childs.append(new_node)
                new_children.append(new_node)

                table = None
                break

        if new_children and table is not None:
            table = cls._build(
                new_children, table, source_id, node_id, tables_info)

        # таблицы без связей
        return table

    @classmethod
    def try_bind_two_nodes(cls, parent_node, child_node,
                           parent_info, child_info):
        """
        Пытается связать к дереву 1 новый узел

        Returns:
            bool: True, если удалось связать два узла, иначе False
        """

        joins = cls.get_joins(parent_info, child_info)

        if joins:
            new_node = Node(child_node.val, child_node.source_id, parent=parent_node,
                            joins=joins, node_id=child_node.node_id)
            parent_node.childs.append(new_node)
            return True

        # признак, что таблица без связей
        return False

    @classmethod
    def reparent_node(cls, parent_node, child_node, parent_info, child_info):
        """
        Пытается связать к дереву 1 новый узел
        """
        joins = cls.get_joins(parent_info, child_info)

        if joins:
            old_parent = child_node.parent
            old_parent.childs.remove(child_node)
            child_node.joins = joins
            child_node.parent = parent_node
            parent_node.childs.append(child_node)
            return

        # признак, что таблица без связей
        return True

    def build_by_structure(self, children):
        self._build_by_structure(self.root, children)

    @classmethod
    def _build_by_structure(cls, root, children):
        """
        строит дерево по структуре дерева
        :param structure: dict
        :return: TablesTree
        """

        for ch in children:
            new_node = Node(ch['val'], ch['sid'], parent=root,
                            joins=ch['joins'], node_id=ch['node_id'],
                            join_type=ch['join_type'],)
            root.childs.append(new_node)
            cls._build_by_structure(new_node, ch['childs'])

    def update_node_joins(self, parent, child, join_type, joins):
        """
        добавляет/меняет связи между таблицами
        :param sel_tree: TablesTree
        :param left_table: str
        :param right_table: str
        :param join_type: str
        :param joins: list
        """
        # nodes = self.ordered_nodes
        # parent = [x for x in nodes if x.val == parent.val and
        #           int(x.source_id) == int(parent.source_id)][0]
        childs = [x for x in parent.childs if x.val == child.val and
                  int(x.source_id) == int(child.source_id)]

        # случай, когда две таблицы не имели связей
        if not childs:
            node = Node(
                child.val, child.source_id, parent, [], child.node_id, join_type)
            parent.childs.append(node)
        else:
            # меняем существующие связи
            node = childs[0]
            node.joins = []
            node.join_type = join_type

        for came_join in joins:
            parent_col, oper, child_col = came_join
            node.joins.append({
                'left': {'table': parent.val, 'column': parent_col,
                         'sid': parent.source_id, },
                'right': {'table': child.val, 'column': child_col,
                          'sid': child.source_id, },
                'join': {"type": join_type, "value": oper},
            })

    @staticmethod
    def get_joins(l_info, r_info):
        """
        Функция выявляет связи между таблицами
        :param l_info:
        :param r_info:
        :return: list
        """
        l_t = l_info['value']
        r_t = r_info['value']

        l_sid = l_info['sid']
        r_sid = r_info['sid']

        l_cols = l_info['columns']
        r_cols = r_info['columns']

        joins = set()
        # избавляет от дублей
        unique_set = set()

        for l_c in l_cols:
            # имя колонки
            l_name = l_c['name']
            # имя колонки с таблицей
            l_t_c_name = "{0}_{1}".format(l_t, l_name)
            # имя колонки с _id
            l_c_id = "{0}_id".format(l_name)
            l_set = {l_name, l_t_c_name, l_c_id, }

            for r_c in r_cols:
                # имя колонки
                r_name = r_c['name']
                # имя колонки с таблицей
                r_t_c_name = "{0}_{1}".format(r_t, r_name)
                # имя колонки с _id
                r_c_id = "{0}_id".format(r_name)
                r_set = {r_name, r_t_c_name, r_c_id, }

                inter = l_set.intersection(r_set)

                if (inter and l_c['type'] == r_c['type'] and
                        # когда сравниваем 2 колонки 'id', то это исключаем
                        inter != {"id", "id_id"}):

                    j_tuple = (l_t, l_name, l_sid, r_t, r_name, r_sid)
                    sort_j_tuple = tuple(sorted(j_tuple))
                    if sort_j_tuple not in unique_set:
                        joins.add(j_tuple)
                        unique_set.add(sort_j_tuple)
                        break

        l_foreign = l_info['foreigns']
        r_foreign = r_info['foreigns']

        for f in l_foreign:
            if f['destination']['table'] == r_t:
                j_tuple = (
                    f['source']['table'],
                    f['source']['column'],
                    l_sid,
                    f['destination']['table'],
                    f['destination']['column'],
                    r_sid,
                )
                sort_j_tuple = tuple(sorted(j_tuple))
                if sort_j_tuple not in unique_set:
                    joins.add(j_tuple)
                    unique_set.add(sort_j_tuple)
                    break

        for f in r_foreign:
            if f['destination']['table'] == l_t:
                j_tuple = (
                    f['source']['table'],
                    f['source']['column'],
                    r_sid,
                    f['destination']['table'],
                    f['destination']['column'],
                    l_sid,
                )
                sort_j_tuple = tuple(sorted(j_tuple))
                if sort_j_tuple not in unique_set:
                    joins.add(j_tuple)
                    unique_set.add(sort_j_tuple)
                    break

        dict_joins = []

        for join in joins:
            dict_joins.append({
                'left': {'table': join[0], 'column': join[1],
                         'sid': join[2], },
                'right': {'table': join[3], 'column': join[4],
                          'sid': join[5], },
                'join': {"type": JoinTypes.INNER, "value": Operations.EQ},
            })

        return dict_joins

    def delete_nodes(self, tables):
        root = self.root
        self._delete_nodes_from_tree(root, tables)

    @classmethod
    def _delete_nodes_from_tree(cls, node, tables):
        """
        удаляет узлы дерева
        :param node: Node
        :param tables: list
        """

        for child in node.childs[:]:
            if child.val in tables:
                child.parent = None
                node.childs.remove(child)
            else:
                cls._delete_nodes_from_tree(child, tables)

    def delete_nodes_NEW(self, tables):
        root = self.root
        self._delete_nodes_from_tree_NEW(root, tables)

    @classmethod
    def _delete_nodes_from_tree_NEW(cls, node, tables):
        """
        удаляет узлы дерева
        :param node: Node
        :param tables: list
        """
        for child in node.childs[:]:
            if (child.val, child.source_id) in tables:
                child.parent = None
                node.childs.remove(child)
            else:
                cls._delete_nodes_from_tree_NEW(child, tables)

    def remove_sub_tree(self, node_id):
        """
        Удаление поддерева по id,
        возвращает корень этого поддерева
        """
        node_id = int(node_id)
        node = self.get_node(node_id)
        parent = node.parent

        if parent is not None:
            for child in parent.childs:
                if child.node_id == node_id:
                    parent.childs.remove(child)
                    break

        node.parent = None

        return node


class TableTreeRepository(object):
    """
        Обработчик деревьев TablesTree
    """

    @staticmethod
    def build_trees(tables, tables_info):
        """
        Построение всевозможных деревьев

        Args:
            tables(tuple): Список таблиц
            tables_info(dict): Информация от таблицах

        Returns:

        """
        trees = {}
        without_bind = {}

        for t_name in tables:
            tree = TablesTree(t_name, 1)
            tree.build(tables, tables_info)
            trees[t_name] = tree
            without_bind[t_name] = tree.no_bind_tables

        return trees, without_bind

    @staticmethod
    def build_single_root(node):
        """
        Строит дерево из 1 элемента
        """
        return TablesTree(node.val, node.source_id, node.node_id)

    @staticmethod
    def build_tree_by_structure(structure):
        sel_tree = TablesTree(structure['val'], structure['sid'],
                              node_id=structure['node_id'])
        sel_tree.build_by_structure(structure['childs'])
        return sel_tree

    @staticmethod
    def select_tree(trees):
        """
        возвращает из списка деревьев лучшее дерево по насыщенности
        детей сверху вниз
        :param trees: list
        :return: list
        """
        counts = {}
        for tr_name, tree in trees.iteritems():
            counts[tr_name] = tree.nodes_count_for_levels
        root_table = max(counts.iteritems(), key=operator.itemgetter(1))[0]
        return trees[root_table]

    @classmethod
    def split_nodes_by_sources(cls, structure):
        """
        Разделяет мультисоурсное дерево по группам джойнов одного источника
        """
        remains = [structure, ]
        result = []

        while remains:
            child = remains[0]
            rems = cls.process_sub_tree(child)

            result.append(child)

            remains.pop(0)
            remains.extend(rems)

        return result

    @classmethod
    def process_sub_tree(cls, child):
        """
        Обрезает до поддерева 1 источника
        """
        sid = child['sid']
        differents = []
        childs = child['childs']

        while childs:
            new_childs = []
            for ch in childs[:]:
                if ch['sid'] == sid:
                    new_childs.extend(ch['childs'])
                    for c in ch['childs'][:]:
                        if c['sid'] != sid:
                            ch['childs'].remove(c)
                else:
                    differents.append(ch)
                    childs.remove(ch)

            childs = new_childs

        return differents

    @classmethod
    def nodes_info(cls, nodes):
        """
        Информация о дереве для передачи на клиент
        """
        return [node.api_info() for node in nodes]

    @classmethod
    def remains_nodes(cls, builder_data):
        """
        Остатки в RemainNode
        """
        remains = []
        for sid in builder_data:
            for remain, remain_id in builder_data[sid]['remains'].iteritems():
                remains.append(
                    RemainNode(remain, sid, remain_id)
                )
        return remains

    @classmethod
    def get_joins(cls, tree, parent_id, child_id):
        """
        Получение информацию по связям узлов
        """
        parent = tree.get_node(parent_id)
        if parent is None:
            raise Exception("No such parent in tree!")

        child = None
        child_id = int(child_id)
        for ch in parent.childs:
            if int(ch.node_id) == child_id:
                child = ch
                break
        # элемент, которого связываем нет в дереве
        if child is None:
            return None, []

        return cls.construct_join_info(parent, child)

    @classmethod
    def construct_join_info(cls, parent, child):
        """
        Собираем к нужному виду информацию о связе для пары узлов
        """
        joins = child.joins
        join_type = joins[0]['join']['type']
        joins_info = []
        for el in joins:
            d = {
                'left': (el['left']['column']
                         if el['left']['table'] == parent.val
                         else el['right']['column']),
                'right': (el['left']['column']
                          if el['left']['table'] == child.val
                          else el['right']['column']),
                'join': el['join']['value']
            }
            joins_info.append(d)

        return join_type, joins_info
