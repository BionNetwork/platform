# coding: utf-8
from __future__ import unicode_literals
from django.conf import settings


class BaseEnum(object):
    """
        Базовый класс для перечислений
    """
    values = {}

    @classmethod
    def get_value(cls, key):
        if key not in cls.values:
            raise ValueError("Unknown key provided " + key)
        return cls.values[key]


class JoinTypes(BaseEnum):

    INNER, LEFT, RIGHT = ('inner', 'left', 'right')

    values = {
        INNER: "INNER JOIN",
        LEFT: "LEFT JOIN",
        RIGHT: "RIGHT JOIN",
    }


class Operations(object):

    EQ, LT, GT, LTE, GTE, NEQ = ('eq', 'lt', 'gt', 'lte', 'gte', 'neq')

    values = {
        EQ: '=',
        LT: '<',
        GT: '>',
        LTE: '<=',
        GTE: '>=',
        NEQ: '<>',
    }

    @staticmethod
    def get_value(operation_type):
        if operation_type not in Operations.values:
            raise ValueError("Unknown operation type provided " + operation_type)
        return Operations.values[operation_type]


class Database(object):
    """
    Базовыми возможности для работы с базами данных
    Получение информации о таблице, список колонок, проверка соединения и т.д.
    """
    def __init__(self, connection):
        self.connection = self.get_connection(connection)

    @staticmethod
    def get_connection(conn_info):
        """
        достает коннекшн бд
        :param conn_info: dict
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def lose_brackets(str_):
        """
        типы колонок приходят типа bigint(20), убираем скобки
        :param str_:
        :return: str
        """
        return str_.split('(')[0].lower()

    def get_query_result(self, query):
        """
        достает результат запроса
        :param query: str
        :return: list
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def _get_columns_query(source, tables):
        raise ValueError("Columns query is not realized")

    def generate_join(self, structure, main_table=None):
        """
        Генерация соединения таблиц для реляционных источников

        Args:
            structure: dict структура для генерации
            main_table: string основная таблица, участвующая в связях
        """

        separator = self.get_separator()

        # определяем начальную таблицу
        if main_table is None:
            main_table = structure['val']
            query_join = '{sep}{table}{sep}'.format(
                table=main_table, sep=separator)
        else:
            query_join = ''
        for child in structure['childs']:
            # определяем тип соединения
            query_join += " " + JoinTypes.get_value(child['join_type'])

            # присоединяем таблицу + ' ON '
            query_join += " " + '{sep}{table}{sep}'.format(
                table=child['val'], sep=separator) + " ON ("

            # список джойнов, чтобы перечислить через 'AND'
            joins_info = []

            # определяем джойны
            for joinElement in child['joins']:

                joins_info.append(("{sep}%s{sep}.{sep}%s{sep} %s {sep}%s{sep}.{sep}%s{sep}" % (
                    joinElement['left']['table'], joinElement['left']['column'],
                    Operations.get_value(joinElement['join']['value']),
                    joinElement['right']['table'], joinElement['right']['column']
                )).format(sep=separator))

            query_join += " AND ".join(joins_info) + ")"

            # рекурсивно обходим остальные элементы
            query_join += self.generate_join(child, child['val'])

        return query_join

    @staticmethod
    def get_rows_query():
        """
        возвращает селект запрос
        :raise: NotImplementedError
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_rows(self, cols, structure):
        """
        достает строки из соурса для превью
        :param cols: list
        :param structure: dict
        :return: list
        """
        query_join = self.generate_join(structure,)

        separator = self.get_separator()

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)

        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        query = self.get_rows_query().format(
            cols_str, query_join,
            settings.ETL_COLLECTION_PREVIEW_LIMIT, 0)

        records = self.get_query_result(query)
        return records

    @staticmethod
    def get_separator():
        """
            Возвращает ковычки( ' or " ) для запроса
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def get_statistic_query(source, tables):
        """
        строка для статистики таблицы
        :param source: Datasource
        :param tables: list
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_statistic(self, source, tables):
        """
        возвращает статистику таблиц
        :param source:
        :param tables:
        :return: list
        """
        stats_query = self.get_statistic_query(source, tables)
        stat_records = self.get_query_result(stats_query)
        stat_records = self.processing_statistic(stat_records)
        return stat_records

    @staticmethod
    def processing_statistic(records):
        """
        обработка статистики
        :param records: list
        :return: dict
        """
        return {x[0]: ({'count': int(x[1]), 'size': x[2]}
                       if (x[1] and x[2]) else None) for x in records}

    @staticmethod
    def local_table_create_query(key_str, cols_str):
        """
        запрос создания таблицы в локал хранилище
        :param key_str:
        :param cols_str:
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def local_table_insert_query(key_str):
        """
        запрос инсерта в таблицу локал хранилища
        :param key_str: str
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_explain_result(self, explain_row):
        """
        запрос получения количества строк в селект запросе
        :param explain_row: str (explain + запрос)
        :raise NotImplementedError:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def get_select_query():
        """
        возвращает селект запрос
        :return: str
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)
