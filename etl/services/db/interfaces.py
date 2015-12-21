# coding: utf-8
from __future__ import unicode_literals
from collections import defaultdict
from itertools import groupby
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
    db_map = None

    def __init__(self, connection):
        self.connection = self.get_connection(connection)

    @staticmethod
    def get_connection(conn_info):
        """
        Получение соединения к базе данных

        Args:
            conn_info(dict): Параметры подключения
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def lose_brackets(str_):
        """
        типы колонок приходят типа bigint(20), убираем скобки
        str -> str
        """
        return str_.split('(')[0].lower()

    def get_query_result(self, query):
        """
        Получаем результат запроса

        Args:
            query(str): Строка запроса

        Return:
            list: Результирующие данные
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def _get_columns_query(source, tables):
        """
         Получение запросов на получение данных о колонках, индексах и
         ограничениях

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            Кортеж из строк запросов в базу
        """
        raise ValueError("Columns query is not realized")

    @classmethod
    def processing_records(cls, col_records, index_records, const_records):
        """
        обработка колонок, констраинтов, индексов источника
        :param col_records: str
        :param index_records: str
        :param const_records: str
        :return: tuple
        """
        indexes = defaultdict(list)
        itable_name, icol_names, index_name, primary, unique = xrange(5)

        for ikey, igroup in groupby(index_records, lambda x: x[itable_name]):
            for ig in igroup:
                indexes[ikey].append({
                    "name": ig[index_name],
                    "columns": ig[icol_names].split(','),
                    "is_primary": ig[primary] == 't',
                    "is_unique": ig[unique] == 't',
                })

        constraints = defaultdict(list)
        (c_table_name, c_col_name, c_name, c_type,
         c_foreign_table, c_foreign_col, c_update, c_delete) = xrange(8)

        for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
            for ig in igroup:
                constraints[ikey].append({
                    "c_col_name": ig[c_col_name],
                    "c_name": ig[c_name],
                    "c_type": ig[c_type],
                    "c_f_table": ig[c_foreign_table],
                    "c_f_col": ig[c_foreign_col],
                    "c_upd": ig[c_update],
                    "c_del": ig[c_delete],
                })

        columns = defaultdict(list)
        foreigns = defaultdict(list)

        table_name, col_name, col_type = xrange(3)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key]
            t_consts = constraints[key]

            for x in group:
                is_index = is_unique = is_primary = False
                col = x[col_name]

                for i in t_indexes:
                    if col in i['columns']:
                        is_index = True
                        for c in t_consts:
                            const_type = c['c_type']
                            if col == c['c_col_name']:
                                if const_type == 'UNIQUE':
                                    is_unique = True
                                elif const_type == 'PRIMARY KEY':
                                    is_unique = True
                                    is_primary = True

                columns[key].append({"name": col,
                                     "type": (
                                         cls.db_map.DB_TYPES[
                                             cls.lose_brackets(x[col_type])] or
                                         x[col_type]),
                                     "is_index": is_index,
                                     "is_unique": is_unique,
                                     "is_primary": is_primary})

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'FOREIGN KEY':
                    foreigns[key].append({
                        "name": c['c_name'],
                        "source": {"table": key, "column": c["c_col_name"]},
                        "destination":
                            {"table": c["c_f_table"], "column": c["c_f_col"]},
                        "on_delete": c["c_del"],
                        "on_update": c["c_upd"],
                    })
        return columns, indexes, foreigns


    def generate_join(self, structure, main_table=None):
        """
        Генерация соединения таблиц для реляционных источников

        Args:
            structure(dict): структура для генерации
            main_table(string): основная таблица, участвующая в связях
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

    def get_rows_query(self, cols, structure):
        """
        Формирования строки запроса на получение данных из базы

        Args:
            cols(dict): Название колонок
            structure(dict): Структура данных

        Returns:
            str: Строка запроса на получения данных без пагинации
        """
        query_join = self.generate_join(structure,)

        separator = self.get_separator()

        pre_cols_str = '{sep}{0}{sep}.{sep}{1}{sep}'.format(
            '{table}', '{col}', sep=separator)

        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols])

        return self.db_map.row_query.format(
            cols_str, query_join,
            '{0}', '{1}')

    def get_rows(self, cols, structure):
        """
        Получаем записи из клиентской базы для предварительного показа

        Args:
            cols(dict): Название колонок
            structure(dict): Структура данных

        Returns:
            list of tuple: Данные по колонкам
        """

        query = self.get_rows_query(cols, structure).format(
            settings.ETL_COLLECTION_PREVIEW_LIMIT, 0)

        return self.get_query_result(query)

    @staticmethod
    def get_separator():
        """
        Возвращает кавычки( ' or " ) для запроса
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_tables(self, source):
        """
        Получение списка таблиц

        Args:
            source(`Datasource`): Источник

        Returns:
            list: Список словарей с названием таблиц
            ::
                [
                    {'name': 'REGIONS'},
                    {'name': 'LOCATIONS'},
                    ...
                ]
        """
        query = self.db_map.table_query.format(source.db)

        records = self.get_query_result(query)
        return map(lambda x: {'name': x[0], }, records)

    @classmethod
    def get_statistic_query(cls, source, tables):
        """
        строка для статистики таблицы

        Args:
             source(`Datasource`): источник
             tables(list): Список таблиц

        Returns:
            str: Строка запроса для получения статистичеких данных
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_statistic(self, source, tables):
        """
        возвращает статистику таблиц

        Args:
            source('Datasource'): источник
            tables(list): Список таблиц

        Returns:
            dict: Статистические данные. Формат ответа см.`processing_statistic`
        """
        stats_query = self.get_statistic_query(source, tables)
        stat_records = self.get_query_result(stats_query)
        return self.processing_statistic(stat_records)

    @staticmethod
    def processing_statistic(records):
        """
        обработка статистистических данных

        Args:
            records(list): Список кортежей вида
            ::
                [
                    (<table_name>, <row_num>, <table_size(Bytes)>),
                    ...
                ]

        Returns:
            dict: Словарь, форматированный к виду
            ::
                {
                    '<table_name>': {
                        'count': <row_num>,
                        'size': <table_size>
                    }
                    '<table_name2>': None  # Если нет данных
                     ...
                }
        """
        return {x[0]: ({'count': int(x[1]), 'size': x[2]}
                       if (x[1] and x[2]) else None) for x in records}

    def get_columns(self, source, tables):
        """
        Получение списка колонок в таблицах

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            Кортеж из списков, в каждом из которых возращается результат запроса
            выборки из базы данных о колонках, индексах и ограничениях таблиц.
            Например::
                (
                    [('DEPARTMENTS', 'DEPARTMENT_ID', 'NUMBER'), ...],
                    [('LOCATIONS', 'LOCATION_ID', 'LOC_ID_PK', 't', 't'),...],
                    [('DEPARTMENTS', 'LOCATION_ID', 'DEPT_LOC_FK', 'R',
                        'LOCATIONS', 'LOCATION_ID', None, 'NO ACTION'), ...],
                )

        """
        columns_query, consts_query, indexes_query = self._get_columns_query(
            source, tables)

        col_records = self.get_query_result(columns_query)
        index_records = self.get_query_result(indexes_query)
        const_records = self.get_query_result(consts_query)

        return col_records, index_records, const_records

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
        возвращает запрос на выборку данных

        Returns:
            str: Запрос на выборку
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)
