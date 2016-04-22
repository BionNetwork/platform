# coding: utf-8
from collections import defaultdict
from etl.services.db.interfaces import JoinTypes, Operations
from etl.services.datasource.repository.storage import RedisSourceService
import operator


class Node(object):
    """
        Узел дерева таблиц
    """
    def __init__(self, t_name, source_id, parent=None, joins=None, join_type='inner'):
        self.val = t_name
        self.source_id = source_id
        self.parent = parent
        self.childs = []
        self.joins = joins or []
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


class TablesTree(object):
    """
        Дерево Таблиц
    """

    def __init__(self, t_name, source_id):
        self.root = Node(t_name, source_id)
        self.no_bind_tables = None

    def display(self):
        if self.root:
            print self.root.val, self.root.joins
            r_chs = [x for x in self.root.childs]
            print [(x.val, x.joins) for x in r_chs]
            for c in r_chs:
                print [x.val for x in c.childs]
            print 80*'*'
        else:
            print 'Empty Tree!!!'

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
                     'source_id': root.source_id, }

        root_info['join_type'] = (
            None if not root_info['joins'] else root.join_type)

        for ch in root.childs:
            root_info['childs'].append(cls._get_tree_structure(ch))
        return root_info

    def build(self, tables, tables_info):
        self.no_bind_tables = self._build([self.root], tables, tables_info)

    @classmethod
    def _build(cls, children, tables, tables_info):
        """
        строит дерево таблиц, возвращает таблицы без связей

        Args:
            children(list of Node):
            tables
            tables_info

        Returns:
            list: Список не связанных таблиц
        """

        child_vals = [x.val for x in children]
        tables = [x for x in tables if x not in child_vals]

        new_children = []

        for child in children:
            new_children += child.childs
            l_val = child.val
            l_info = tables_info[l_val]

            for t_name in tables[:]:
                r_info = tables_info[t_name]
                joins = cls.get_joins(l_val, t_name, l_info, r_info)

                if joins:
                    tables.remove(t_name)
                    new_node = Node(t_name, 1, child, joins)
                    child.childs.append(new_node)
                    new_children.append(new_node)

        if new_children and tables:
            tables = cls._build(new_children, tables, tables_info)

        # таблицы без связей
        return tables

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
            new_node = Node(ch['val'], root, ch['joins'],
                            ch['join_type'])
            root.childs.append(new_node)
            cls._build_by_structure(new_node, ch['childs'])

    def update_node_joins(self, left_table,
                          right_table, join_type, joins):
        """
        добавляет/меняет связи между таблицами
        :param sel_tree: TablesTree
        :param left_table: str
        :param right_table: str
        :param join_type: str
        :param joins: list
        """
        nodes = self.ordered_nodes
        parent = [x for x in nodes if x.val == left_table][0]
        childs = [x for x in parent.childs if x.val == right_table]

        # случай, когда две таблицы не имели связей
        if not childs:
            node = Node(right_table, parent, [], join_type)
            parent.childs.append(node)
        else:
            # меняем существующие связи
            node = childs[0]
            node.joins = []
            node.join_type = join_type

        for came_join in joins:
            parent_col, oper, child_col = came_join
            node.joins.append({
                'left': {'table': left_table, 'column': parent_col},
                'right': {'table': right_table, 'column': child_col},
                'join': {"type": join_type, "value": oper},
            })

    @staticmethod
    def get_joins(l_t, r_t, l_info, r_info):
        """
        Функция выявляет связи между таблицами
        :param l_t:
        :param r_t:
        :param l_info:
        :param r_info:
        :return: list
        """
        l_cols = l_info['columns']
        r_cols = r_info['columns']

        joins = set()
        # избавляет от дублей
        unique_set = set()

        for l_c in l_cols:
            l_str = u'{0}_{1}'.format(l_t, l_c['name'])
            for r_c in r_cols:
                r_str = u'{0}_{1}'.format(r_t, r_c['name'])
                if l_c['name'] == r_str and l_c['type'] == r_c['type']:
                    j_tuple = (l_t, l_c["name"], r_t, r_c["name"])
                    sort_j_tuple = tuple(sorted(j_tuple))
                    if sort_j_tuple not in unique_set:
                        joins.add(j_tuple)
                        unique_set.add(sort_j_tuple)
                        break
                if l_str == r_c["name"] and l_c['type'] == r_c['type']:
                    j_tuple = (l_t, l_c["name"], r_t, r_c["name"])
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
                    f['destination']['table'],
                    f['destination']['column'],
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
                    f['destination']['table'],
                    f['destination']['column'],
                )
                sort_j_tuple = tuple(sorted(j_tuple))
                if sort_j_tuple not in unique_set:
                    joins.add(j_tuple)
                    unique_set.add(sort_j_tuple)
                    break

        dict_joins = []

        for join in joins:
            dict_joins.append({
                'left': {'table': join[0], 'column': join[1]},
                'right': {'table': join[2], 'column': join[3]},
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
    def build_single_root(table, source_id):
        """
        Строит дерево из 1 элемента
        """
        tree = TablesTree(table, source_id)
        return tree

    @staticmethod
    def build_tree_by_structure(structure):
        sel_tree = TablesTree(structure['val'], structure['source_id'])
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
