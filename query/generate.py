# coding: utf-8


class QueryException(Exception):
    pass


SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP_BY"
HAVING = "HAVING"
ORDER_BY = "ORDER_BY"


class QueryGenerate(object):
    """
    Формирование SQL-запроса для получения данных
    """

    def __init__(self, cube_id, input):
        """
        Args:
            cube_id:
            input: dict
            Приблизительный словарь с входными данными такой:
            {"groups":{
                "<date_group_name>":  // Группировка по дате
                    "type": "DateGroup"
                    "field": "Название поля"
                    "filters": "Фильтры. Пока не реализовано. Да пока и не нужно"
                    "interval": "Интервал дат"
                    "range": "Область дат"
                },
                "<text_group_name>":{
                    ...
                    "match": Значения колонки, которые должны заматчится
                 },
            }
            aggs: {
            # агригируемые поля
                 "<aggrigation_field>":{
                 ...
                 }
             }

        при обработке каждого блока группы и агрегации возращается словарь
        с заполнеными клучевыми словами из "select, group by, where ...". в дальнейшем
         они собираются единый словарь по ключевым словам. Надо их преобразовать в SQL

        """
        self.cube_id = cube_id
        self.input = input
        self.key_words = {
            SELECT: [],
            WHERE: [],
            GROUP_BY: [],
            HAVING: [],
            ORDER_BY: []
        }

    def parse(self):
        grouping = False
        if self.input.get('groups', None):
            grouping = True
            self.group_fabric(self.input['groups']).run()
        if self.input.get('aggs', None):
            for agg in self.input.get('aggs'):
                self.update_key_words(AggregationParse(agg).run())
        if self.input.get('filters', None):
            Filters(self.input['filters'], grouping=grouping)
        return self.key_words

    @staticmethod
    def group_fabric(groups):
        for group in groups:
            if group['type'] == 'DateGroup':
                return DateGroupParse(group)
            elif group['type'] == 'TextGroup':
                return TextGroupParse(group)

    def update_key_words(self, key_words):
        for key, value in key_words:
            self.key_words[key].extend(value)


class Parse(object):
    """
    Обработка узла

    Attributes:
        name(str): Название выходного поля
        field(str): Название входного поля
        type(str): Тип узла
    """
    types = []

    def __init__(self, node):
        """

        Args:
            node(dict): Входной словарь
        """
        self.name, value = node.items()
        self.field = value['field']
        self.type = value['type']
        self.filter = node.get('filter', None)
        if self.type not in self.types:
            raise QueryException('Тип узла не соответсвует')

        self.filters = node.get('filters', None)

    def run(self):
        raise NotImplementedError

    def filtering(self):
        pass


class TextGroupParse(Parse):
    """
    Разбор текстового группирочного узла
    """

    types = ['TextGroup']

    def __init__(self, node):
        self.match = node.get('match', None)
        super(TextGroupParse, self).__init__(node)

    def run(self):
        res = dict()
        res[SELECT] = ['{field} AS {name}'.format(field=self.field, name=self.name)]
        res[GROUP_BY] = ['{field}'.format(field=self.name)]
        if self.match:
            res[WHERE] = ['{field} in {match_fields}'.format(
                field=self.name, match_fields=self.match
            )]
        return res


class DateGroupParse(Parse):
    """
    Разбор узла дат

    Attributes:
        range(tuple): Диапазон дат
        interval: Интервал дат
    """

    types = ['DateGroup']

    def __init__(self, node):
        self.range = node.get('range', None)
        self.interval = node.get('interval')
        super(DateGroupParse, self).__init__(node)

    def run(self):
        res = dict()
        res[SELECT] = ['{field} AS {name}'.format(field=self.field, name=self.name)]
        res[GROUP_BY] = ['{field}'.format(field=self.name)]
        if self.range:
            left_edge, right_edge = self.range[:]
            res[WHERE] = ["{field} > {left_edge} AND {field} < {right_edge}".format(
                field=self.name, left_edge=left_edge, right_edge=right_edge
            )]


class AggregationParse(Parse):
    """
    Разбор агрегирующего узла
    """
    types = ['avg', 'sum', 'min', 'max']

    def __init__(self, node):
        self.range = node.get('range', None)
        super(AggregationParse, self).__init__(node)

    def run(self):
        res = dict()
        res[SELECT] = ["{type}({field}} as {name}".format(type=self.type, field=self.field, name=self.name)]

        if self.range:
            left_edge, right_edge = self.range[:]
            res[HAVING] = ["{field} > {left_edge} AND {field} < {right_edge}".format(
                field=self.name, left_edge=left_edge, right_edge=right_edge
            )]
        return res

MATCH_ALL = "MATCH_ALL"
MATCH = "MATCH"


class Filters(object):
    """
    фильтрация данных

    Пока если до этого была группировка, то условия можно накладывать только на
    группируемые и агрегируемые поля

    ::
        {"fields": ["<field_1>", "<field_2>", "<field_3>", ...]
        "conditions":
            ["MATCH":
                {"field": "<field_1>", "value": [<match field list>]},
            ...
            "RANGE": {"field": "<field_2>", "value": [<left_edge>, <right_edge>]},
            "LTE": {"field": "<field_3>": "value": "<value>"},
            "LT": ...
            ...
            "OR": ["MATCH": ...,
                "RANGE": ...
                ]
            ],
            "AND": ...
        }
    """

    def __init__(self, node, grouping=False):
        self.grouping = grouping
        self.fields = node['fields'] if not grouping else None
        self.conditions = node['conditions']

    def run(self):
        res = dict()
        if self.fields and self.fields != "ALL":
            res[SELECT] = self.fields
        elif self.fields == "ALL":
            res[SELECT] = "*"

        res[WHERE] = []
        if self.conditions:
            for key, value in self.conditions:
                if key == [MATCH]:
                    res[WHERE].append("{field} in ({match_values})".format(
                        field=value['field'], match_values=", ".join(value[0])))
