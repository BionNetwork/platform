# coding: utf-8

import requests


class QueryException(Exception):
    pass


SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP_BY"
HAVING = "HAVING"
ORDER_BY = "ORDER_BY"


DATE_DIM = 'DateDim'
TEXT_DIM = 'TextDim'

class QueryGenerate(object):
    """
    Формирование SQL-запроса для получения данных
    """

    def __init__(self, cube_id, input):
        """
        Args:
            cube_id:
            input: dict
            {
            "dims": [
                {
                    "name": "d_name",
                    "type": "DateDim",
                    "field": "d",
                    "filters": {
                        "range": ["2015-01-01", "2017-01-01"]
                    },
                    "interval": "toStartOfQuarter",

                },
                {
                    "name": "org",
                    "type": "TextDim",
                    "field": "org",
                    "order": "desc",
                    "visible": True,
                    "filters": {
                        "match": ['Эттон-Центр'],
                        },
                    },
                ],
                "measures": [{
                    "name": "val_sum",
                    "type": "sum",
                    "field": "val",
                    "visible": True,
                    "filters": {
                        "lte": 3000
                    },
                }]
            }


            {"dims": [
                {
                    "field": "c_21_21_4864501795689730638_6619553242090680547",
                    "type": "TextDim",
                    "name": "date"
                },
                {
                    "field": "c_21_21_4864501795689730638_8261471818073815377",
                    "type": "TextDim",
                    "name": "org"
                }],
"measures":[
{
                    "field": "c_21_21_4864501795689730638_3478212977531136609",
                    "type": "sum",
                    "name": "remain"
                }
]
                }



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
        if self.input.get('dims', None):
            for dim in self.input['dims']:
                self.update_key_words(self.dimension_parser(dim).run())
        if self.input.get('measures', None):
            for measure in self.input.get('measures'):
                self.update_key_words(MeasureParse(measure).run())

        select_block = "SELECT %s" % ", ".join(self.key_words["SELECT"])
        where_block = "WHERE %s" % " AND ".join(self.key_words[WHERE]).replace('"', "'").replace("'", '"') if self.key_words[WHERE] else ""
        group_block = "GROUP BY %s" % ", ".join(self.key_words["GROUP_BY"])
        having_block = 'HAVING %s' % " AND ".join(self.key_words[HAVING]) if self.key_words[HAVING] else ""
        order_block = "ORDER BY %s" % ", ".join(self.key_words[ORDER_BY]) if self.key_words[ORDER_BY] else ""

        select_query = "{select_block} FROM {table} {where_block} {group_block} {having_block} {order_block}".format(
            select_block=select_block,
            table='t_21',
            where_block=where_block,
            group_block=group_block,
            order_block=order_block,
            having_block=having_block
        ).replace('"', "'")

        return requests.post('http://localhost:8123/', data='{0} FORMAT JSON'.format(select_query).encode('utf-8')).text

    @staticmethod
    def dimension_parser(dim):
        if dim['type'] == DATE_DIM:
            return DateDimParse(dim)
        elif dim['type'] == TEXT_DIM:
            return TextDimParse(dim)
        else:
            raise QueryException("Некорректный тип размерности")

    def update_key_words(self, key_words):
        for key, value in key_words.items():
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

        self.name = node.get('name', None) or self.generated_name
        self.field = node['field']
        self.type = node['type']
        self.visible = node.get('visible', True)
        self.order = node.get('order', None)

        self.filter = Filter
        self.filter_block = node.get('filters', None)

        if self.type not in self.types:
            raise QueryException('Тип узла не соответсвует')

    @property
    def generated_name(self):
        return self.field

    def run(self):
        raise NotImplementedError

    def filtering(self):
        pass


class TextDimParse(Parse):
    """
    Разбор текстового группирочного узла
    """

    types = [TEXT_DIM]

    def run(self):
        res = dict()
        res[SELECT] = ['{field} AS {name}'.format(field=self.field, name=self.name)] if self.visible else []
        res[GROUP_BY] = ['{field}'.format(field=self.name)]
        if self.order:
            res[ORDER_BY] = ['{field} {order_type}'.format(field=self.name, order_type=self.order)]

        if self.filter_block:
            res[WHERE] = [self.filter(self.name, self.filter_block).parse()]

        return res


class DateDimParse(Parse):
    """
    Разбор узла дат

    Attributes:
        range(tuple): Диапазон дат
        interval: Интервал дат
    """

    types = [DATE_DIM]

    def __init__(self, node):
        super(DateDimParse, self).__init__(node)
        self.filter = DateFilter
        self.interval = node.get('interval', None)

    def run(self):
        res = dict()
        if self.visible:
            if self.interval:
                res[SELECT] = ['{interval}({field}) AS {name}'.format(
                    interval=self.interval, field=self.field, name=self.name)]
            else:
                res[SELECT] = ['{field} AS {name}'.format(field=self.field, name=self.name)]
        res[GROUP_BY] = ['{field}'.format(field=self.name)]
        if self.filter_block:
            res[WHERE] = [self.filter(self.field, self.filter_block).parse()]

        return res


class MeasureParse(Parse):
    """
    Разбор агрегирующего узла
    """
    types = ['avg', 'sum', 'min', 'max']

    def generated_name(self):
        return "{agg}_{field}".format(agg=self.type, field=self.field)

    def __init__(self, node):
        super(MeasureParse, self).__init__(node)

    def run(self):
        res = dict()
        inner_name = "{type}({field})".format(type=self.type, field=self.field)
        res[SELECT] = ["{field} as {name}".format(field=inner_name, name=self.name)] if self.visible else []
        if self.filter_block:
            res[HAVING] = [self.filter(inner_name, self.filter_block).parse()]

        return res

LT = 'lt'
GT = 'gt'
LTE = 'lte'
GTE = 'gte'


class Filter(object):
    """
    фильтрация данных

    Пока если до этого была группировка, то условия можно накладывать только на
    группируемые и агрегируемые поля

    ::
        {
            "MATCH":[<match field list>],
            ...
            "RANGE": [<left_edge>, <right_edge>],
            "LTE": "<value>",
            "LT": ...
            ...
            "OR": ["MATCH": ...,
                "RANGE": ...
                ]
            ],
            "AND": ...
        }
    """

    def __init__(self, field, filter_node):
        self.field = field
        self.node = filter_node
        self.ret_list = []

    def parse(self):
        """
        Разбор условий фильтра поля

        Returns:

        """
        return self._parse(self.node)

    def _parse(self, conditions):
        """
        Разбор условий по условию

        Args:
            conditions(dict): Условия разбора
        Returns:

        """
        l = []
        for operator, value in conditions.items():
            l.append(getattr(self, operator)(value))
        return ' AND '.join(l)

    def _compare(self, operator, value):
        """
        Сравнение значений
        Args:
            operator(str): Оператор сравнения
            value: Сравниваемое значение

        Returns:
            итоговая строка с условием
        """
        return "{field} {operator} {value}".format(field=self.field, operator=operator, value=value)

    def _match(self, operator, value):
        """
        Проверка на совпадение/не совпадение значения
        Args:
            operator(str): оператор
            value(list): Набор фильтруемых значений
        """
        # TODO: Привести к общему виду
        if type(value[0]) is str:
            return '{field} {operator} ("{match_fields}")'.format(
                     field=self.field, operator=operator, match_fields='","'.join(value)
                 )
        else:
            return '{field} {operator} ({match_fields})'.format(
                field=self.field, operator=operator, match_fields=','.join(str(x) for x in value))

    def match(self, value):
        """
        Проверка на совпадение значения
        Args:
            value(list): Набор фильтруемых значений
        """
        return self._match('IN', value)

    def not_match(self, value):
        return self._match('NOT IN', value)

    def range(self, value):
        """
        Фильтрация по диапазону значений
        Args:
            value(list): Диапазон значений
        """
        if len(value) != 2:
            raise QueryException(
                'Число крайних значений диапазона для поля {0} не равно 2'.format(self.field))
        new_value = [{GT: value[0]}, {LT: value[1]}]
        return self._and(new_value)

    def eq(self, value):
        """
        Точное совпадание значения (=)
        Args:
            value: Сравниваемое значение

        """
        return self._compare('=', value)

    def not_eq(self, value):
        """
        Не совпадение значений
        Args:
            value:
        """

        return self._compare('<>', value)

    def lt(self, value):
        """
        Сравнение 'меньше' (<)
        Args:
            value: Сравниваемое значение

        Returns:

        """
        return self._compare('<', value)

    def lte(self, value):
        """
        Сравнение 'меньше или равно' (<=)
        Args:
            value: Сравниваемое значение

        Returns:

        """
        return self._compare('<=', value)

    def gt(self, value):
        """
        Сравнение 'больше' (>)
        Args:
            value: Сравниваемое значение
        """
        return self._compare('>', value)

    def gte(self, value):
        """
        Сравнение 'больше или равно' (>=)
        Args:
            value: Сравниваемое значение
        """
        return self._compare('>=', value)

    def _or(self, value):
        """
        Сравнение 'или'
        Args:
            value(list): Сравниваемые значения

        Returns:

        """
        return "("+" OR ".join([self._parse(x) for x in value])+")"

    def _and(self, value):
        """
        Сравнение 'и'
        Args:
            value: Сравниваемые значения

        Returns:

        """
        return "("+" AND ".join([self._parse(x) for x in value])+")"


class DateFilter(Filter):
    """
    Фильтрация дат
    """

    def _compare(self, operator, value):
        """
        Приводим значения к дате
        """
        return "toDate({field}) {operator} toDate('{value}')".format(field=self.field, operator=operator, value=value)
