# coding: utf-8
from collections import defaultdict
from etl.services.db.interfaces import JoinTypes, Operations
from etl.services.datasource.repository.storage import RedisSourceService
import operator


class Node(object):
    """
        Узел дерева таблиц
    """
    def __init__(self, t_name, parent=None, joins=[], join_type='inner'):
        self.val = t_name
        self.parent = parent
        self.childs = []
        self.joins = joins
        self.join_type = join_type

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

    def __init__(self, t_name):
        self.root = Node(t_name)

    # def display(self):
    #     if self.root:
    #         print self.root.val, self.root.joins
    #         r_chs = [x for x in self.root.childs]
    #         print [(x.val, x.joins) for x in r_chs]
    #         for c in r_chs:
    #             print [x.val for x in c.childs]
    #         print 80*'*'
    #     else:
    #         print 'Empty Tree!!!'

    @classmethod
    def get_tree_ordered_nodes(cls, nodes):
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
            all_nodes += cls.get_tree_ordered_nodes(child_nodes)
        return all_nodes

    @classmethod
    def get_nodes_count_by_level(cls, nodes):
        """
        список количества нодов на каждом уровне дерева
        :param nodes: list
        :return: list
        """
        counts = [len(nodes)]

        child_nodes = reduce(
            list.__add__, [x.childs for x in nodes], [])
        if child_nodes:
            counts += cls.get_nodes_count_by_level(child_nodes)
        return counts

    @classmethod
    def get_tree_structure(cls, root):
        """
        структура дерева
        :param root: Node
        :return: dict
        """
        root_info = {'val': root.val, 'childs': [], 'joins': list(root.joins), }

        root_info['join_type'] = (
            None if not root_info['joins'] else root.join_type)

        for ch in root.childs:
            root_info['childs'].append(cls.get_tree_structure(ch))
        return root_info

    @classmethod
    def build_tree(cls, childs, tables, tables_info):
        """
        строит дерево таблиц, возвращает таблицы без свяезй
        :param childs:
        :param tables:
        :param tables_info:
        :return: list
        """

        def inner_build_tree(childs, tables):
            child_vals = [x.val for x in childs]
            tables = [x for x in tables if x not in child_vals]

            new_childs = []

            for child in childs:
                new_childs += child.childs
                r_val = child.val
                l_info = tables_info[r_val]

                for t_name in tables[:]:
                    r_info = tables_info[t_name]
                    joins = cls.get_joins(r_val, t_name, l_info, r_info)

                    if joins:
                        tables.remove(t_name)
                        new_node = Node(t_name, child, joins)
                        child.childs.append(new_node)
                        new_childs.append(new_node)

            if new_childs and tables:
                tables = inner_build_tree(new_childs, tables)

            # таблицы без связей
            return tables

        tables = inner_build_tree(childs, tables)

        return tables

    @classmethod
    def select_tree(cls, trees):
        """
        возвращает из списка деревьев лучшее дерево по насыщенности
        детей сверху вниз
        :param trees: list
        :return: list
        """
        counts = {}
        for tr_name, tree in trees.iteritems():
            counts[tr_name] = cls.get_nodes_count_by_level([tree.root])
        root_table = max(counts.iteritems(), key=operator.itemgetter(1))[0]
        return trees[root_table]

    @classmethod
    def build_tree_by_structure(cls, structure):
        """
        строит дерево по структуре дерева
        :param structure: dict
        :return: TablesTree
        """
        tree = TablesTree(structure['val'])

        def inner_build(root, childs):
            for ch in childs:
                new_node = Node(ch['val'], root, ch['joins'],
                                ch['join_type'])
                root.childs.append(new_node)
                inner_build(new_node, ch['childs'])

        inner_build(tree.root, structure['childs'])

        return tree

    @classmethod
    def update_node_joins(cls, sel_tree, left_table,
                          right_table, join_type, joins):
        """
        добавляет/меняет связи между таблицами
        :param sel_tree: TablesTree
        :param left_table: str
        :param right_table: str
        :param join_type: str
        :param joins: list
        """
        nodes = cls.get_tree_ordered_nodes([sel_tree.root, ])
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

    @classmethod
    def get_joins(cls, l_t, r_t, l_info, r_info):
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
            l_str = '{0}_{1}'.format(l_t, l_c['name'])
            for r_c in r_cols:
                r_str = '{0}_{1}'.format(r_t, r_c['name'])
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


class TableTreeRepository(object):
    """
        Обработчик деревьев TablesTree
    """

    @classmethod
    def build_trees(cls, tables, source):
        """
        строит всевозможные деревья
        :param tables: list
        :param source: Datasource
        :return:
        """
        trees = {}
        without_bind = {}

        tables_info = RedisSourceService.info_for_tree_building(
            (), tables, source)

        for t_name in tables:
            tree = TablesTree(t_name)

            without_bind[t_name] = TablesTree.build_tree(
                [tree.root, ], tables, tables_info)
            trees[t_name] = tree

        return trees, without_bind

    @classmethod
    def delete_nodes_from_tree(cls, tree, source, tables):
        """
        удаляет узлы дерева
        :param tree: TablesTree
        :param source: Datasource
        :param tables: list
        """

        def inner_delete(node):
            for child in node.childs[:]:
                if child.val in tables:
                    child.parent = None
                    node.childs.remove(child)
                else:
                    inner_delete(child)

        r_val = tree.root.val
        if r_val in tables:
            RedisSourceService.tree_full_clean(source)
            tree.root = None
        else:
            inner_delete(tree.root)
