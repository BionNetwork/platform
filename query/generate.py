# coding: utf-8

import requests
import json


class QueryException(Exception):
    pass


SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP_BY"
HAVING = "HAVING"
ORDER_BY = "ORDER_BY"


DATE_DIM = 'date'
TEXT_DIM = 'text'

FIELD_MEASURE = 'field'
EXPRESSION_MEASURE = 'expression'


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
                    "order_by": "desc",
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


{"transform": "true",
"dims": [
                {
                    "field": "c_21_21_4864501795689730638_6619553242090680547",
                    "type": "date",
                    "name": "date",
                    "order": "1"
                },
                {
                    "field": "c_21_21_4864501795689730638_8261471818073815377",
                    "type": "text",
                    "name": "org",
                    "order": "2"
                }],
"measures":[
{
                    "field": "c_21_21_4864501795689730638_3478212977531136609",
                    "type": "field",
                    "agg_type": "sum",
                    "name": "remain",
                    "order": "3"
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
        self.is_transform = input.get('transform', False)

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
            table='t_{0}'.format(self.cube_id),
            where_block=where_block,
            group_block=group_block,
            order_block=order_block,
            having_block=having_block
        ).replace('"', "'")

        data = json.loads(
            requests.post(
                'http://localhost:8123/', data='{0} FORMAT JSON'.format(select_query).encode('utf-8')).text)['data']
        if self.is_transform:
            data = self.transform(data)
        return data

    def transform(self, data):
        uniq_2_level = []
        orders = {}
        if self.input.get('dims', None):
            for dim in self.input['dims']:
                orders.update({dim["order"]: dim["name"]})

        if self.input.get('measures', None):
            for dim in self.input['measures']:
                orders.update({dim["order"]: dim["name"]})

        temp = {}
        res = []
        for each in data:
            main_field_value = each[orders["1"]]
            second_field_value = each[orders["2"]]
            measure_field_value = each[orders["3"]]

            del each[orders["1"]]
            if each[orders["2"]] not in uniq_2_level:
                uniq_2_level.append(each[orders["2"]])
            if temp.get(main_field_value, None):
                temp[main_field_value].update(
                    {second_field_value: measure_field_value}
                )
            else:
                temp[main_field_value] = {second_field_value: measure_field_value}

        for key, value in temp.items():
            row = [key]

            for second_el in uniq_2_level:
                if second_el in value.keys():

                    row.append(value[second_el])
                else:
                    row.append(0)
            res.append(row)

        return {"fields": uniq_2_level, "data": res}

    @staticmethod
    def dimension_parser(dim):
        if dim['type'] == DATE_DIM:
            return DateDimParse(dim)
        elif dim['type'] == TEXT_DIM:
            return TextDimParse(dim)
        else:
            raise QueryException("Некорректный тип размерности")

    @staticmethod
    def measure_parser(measure):
        if measure['type'] == FIELD_MEASURE:
            return MeasureParse(measure)
        elif measure['type'] == EXPRESSION_MEASURE:
            return ExpressionMeasureParse(measure)
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
        self.order_by = node.get('order_by', None)

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
        if self.order_by:
            res[ORDER_BY] = ['{field} {order_type}'.format(field=self.name, order_type=self.order_by)]

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
    types = [FIELD_MEASURE]
    agg_types = ['avg', 'sum', 'min', 'max']

    def generated_name(self):
        return "{agg}_{field}".format(agg=self.type, field=self.field)

    def __init__(self, node):
        super(MeasureParse, self).__init__(node)
        self.agg_type = node['agg_type']

    def run(self):
        res = dict()
        inner_name = "{type}({field})".format(type=self.agg_type, field=self.field)
        res[SELECT] = ["{field} as {name}".format(field=inner_name, name=self.name)] if self.visible else []
        if self.filter_block:
            res[HAVING] = [self.filter(inner_name, self.filter_block).parse()]

        return res


class ExpressionMeasureParse(MeasureParse):

    types = [EXPRESSION_MEASURE]

    def __init__(self, node):
        super(ExpressionMeasureParse, self).__init__(node)
        self.expression = node['expression']

    def run(self):
        res = dict()
        res[SELECT] = [Expression().parse(self.expression)]


LT = 'lt'
GT = 'gt'
LTE = 'lte'
GTE = 'gte'


class Filter(object):
    """
    фильтрация данных

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


class Expression(object):
    """
    Построение формулы для строки
    """

    def parse(self, expression):

        return self._parse(expression)

    def get_value(self, args):
        """

        Args:
            args(list): список аргументов выражения

        Returns: Аргументы, участвующие в выражении

        """
        return [self._parse(arg) if type(arg) is dict else arg for arg in args]

    def _validate(self, expression):
        """
        Валидация узла выражения
        Args:
            expression(dict): выражение

        Returns:
            bool - флаг успешной валидации
            str - строка ошибки

        """
        operator, expr_args = expression.items()
        if not getattr(self, operator, None):
            return False, "Оператора {0} указан некорректно".format(operator)
        if len(expr_args) != 2:
            return False, "Число аргументов выражения {0} не равно 2".format(operator)
        for arg in expr_args:
            if type(arg) not in (int, dict, str):
                return False, "Тип аргумента для выражения {0} некорректен".format(operator)
            if type(arg) is str:
                pass
        return True, ''

    def _operation(self, operator, first_value, second_value):
        return "{operator}({first_value}, {second_value})".format(
            operator=operator, first_value=first_value, second_value=second_value)

    def _parse(self, expression):
        """
        Обработка выражения
        Args:
            expression(dict): выражение
            ::
                {
                    "<operator>": ["<value>", "<value>"]
                    ...
                }
            , где value либо константа,
            либо название поля, участвующее в выражении,
            либо еще одно выражение

        Returns:
            str: Строка, характеризующее выражение в запросе

        """
        is_valid, error_message = self._validate(expression)
        if is_valid:
            operator, expr_args = expression.items()
            return getattr(self, operator, None)(self.get_value(expr_args))
        else:
            raise QueryException(error_message)

    def plus(self, arg1, arg2):
        self._operation('plus', arg1, arg2)

    def minus(self, arg1, arg2):
        self._operation('minus', arg1, arg2)

    def multiply(self, arg1, arg2):
        self._operation('multiply', arg1, arg2)

    def divide(self, arg1, arg2):
        self._operation('divide', arg1, arg2)

    def modulo(self, arg1, arg2):
        self._operation('modulo', arg1, arg2)

