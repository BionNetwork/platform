# coding: utf-8

from collections import defaultdict
from itertools import groupby
import datetime
from django.conf import settings
import time
from contextlib import closing

from etl.constants import FIELD_NAME_SEP


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

    def __init__(self, source):
        """
        Присваиваем источник и устанавливаем подключение
        """
        self.source = source
        connection = self.get_source_data()
        self.connection = self.get_connection(connection)

    def get_source_data(self):
        """
        Возвращает список модели источника данных
        Returns:
            dict: словарь с информацией подключения
        """
        return {'db': self.source.db, 'host': self.source.host,
                'port': self.source.port, 'login': self.source.login,
                'password': self.source.password}

    @staticmethod
    def get_connection(conn_info):
        """
        Получение соединения к базе данных

        Args:
            conn_info(dict): Параметры подключения
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_structure_rows_number(self, structure, cols):
        """
        Получение предполагаемые кол-во строк
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def lose_brackets(str_):
        """
        типы колонок приходят типа bigint(20), убираем скобки
        str -> str
        """
        return str_.split('(')[0].lower()

    def get_query_result(self, query, **kwargs):
        """
        Получаем результат запроса

        Args:
            query(str): Строка запроса

        Return:
            list: Результирующие данные
        """
        cursor = self.connection.cursor()
        cursor.execute(query, kwargs)
        return cursor.fetchall()

    @staticmethod
    def get_tables_str(tables):
        # возвращает строку таблиц вида "('t1', 't2', ...)"
        return '(' + ', '.join(["'{0}'".format(y) for y in tables]) + ')'

    def _get_columns_query(self, source, tables):
        """
         Получение запросов на получение данных о колонках, индексах и
         ограничениях

        Args:
            source(`Datasource`): источник
            tables(list): список названий таблиц

        Returns:
            Кортеж из строк запросов в базу
        """
        tables_str = self.get_tables_str(tables)

        cols_query = self.get_columns_query(tables_str, source)
        constraints_query = self.get_constraints_query(tables_str, source)
        indexes_query = self.get_indexes_query(tables_str, source)

        return cols_query, constraints_query, indexes_query

    def get_columns_query(self, tables_str, source):
        # запрос на колонки
        return self.db_map.cols_query.format(tables_str, source.db)

    def get_constraints_query(self, tables_str, source):
        # запрос на ограничения
        return self.db_map.constraints_query.format(tables_str, source.db)

    def get_indexes_query(self, tables_str, source):
        # запрос на индексы
        return self.db_map.indexes_query.format(tables_str, source.db)

    def get_columns(self, source, tables):
        # список колонок таблиц
        tables_str = self.get_tables_str(tables)
        cols_query = self.get_columns_query(tables_str, source)
        return self.get_query_result(cols_query)

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
        itable_name, icol_names, index_name, primary, unique = range(5)

        for ikey, igroup in groupby(index_records, lambda x: x[itable_name]):
            for ig in igroup:
                indexes[ikey.lower()].append({
                    "name": ig[index_name],
                    "columns": ig[icol_names].split(','),
                    "is_primary": ig[primary] == True,
                    "is_unique": ig[unique] == True,
                })

        constraints = defaultdict(list)
        (c_table_name, c_col_name, c_name, c_type,
         c_foreign_table, c_foreign_col, c_update, c_delete) = range(8)

        for ikey, igroup in groupby(const_records, lambda x: x[c_table_name]):
            for ig in igroup:
                constraints[ikey.lower()].append({
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

        table_name, col_name, col_type, is_nullable, extra_, max_length = range(6)

        for key, group in groupby(col_records, lambda x: x[table_name]):

            t_indexes = indexes[key.lower()]
            t_consts = constraints[key.lower()]

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

                columns[key.lower()].append({"name": col,
                                     "type": (
                                         cls.db_map.DB_TYPES[
                                             cls.lose_brackets(x[col_type])] or
                                         x[col_type]),
                                     "is_index": is_index,
                                     "is_unique": is_unique,
                                     "is_primary": is_primary,
                                     "origin_type": x[col_type],
                                     "is_nullable": x[is_nullable],
                                     "extra": x[extra_],
                                     "max_length": x[max_length],
                                     })

            # находим внешние ключи
            for c in t_consts:
                if c['c_type'] == 'FOREIGN KEY':
                    foreigns[key.lower()].append({
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

        # FIXME временно ставим *, берем все столбы
        cols_str = ', '.join(
            [pre_cols_str.format(**x) for x in cols]) if cols else ' * '

        return self.db_map.row_query.format(
            cols_str, query_join,
            '%(limit)s', '%(offset)s')

    def get_table_data(self, table_name, limit, offset):

        return """SELECT * FROM {table_name}""".format(table_name=table_name)

    def get_rows(self, cols, structure):
        """
        Получаем записи из клиентской базы для предварительного показа

        Args:
            cols(dict): Название колонок
            structure(dict): Структура данных

        Returns:
            list of tuple: Данные по колонкам
        """

        query = self.get_rows_query(cols, structure)

        return self.get_query_result(
            query=query, limit=settings.ETL_COLLECTION_PREVIEW_LIMIT, offset=0)

    @staticmethod
    def get_separator():
        """
        Возвращает кавычки( ' or " ) для запроса
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_tables(self):
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
        query = self.db_map.table_query.format(self.source.db)

        records = self.get_query_result(query)
        return [{'name': x[0], } for x in records]

    @staticmethod
    def remote_table_create_query():
        """
        запрос на создание новой таблицы в БД клиента
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def reload_datasource_trigger_query(params):
        """
        запрос на создание триггеров в БД локально для размерностей и мер

        Args:
            params(dict): Параметры, необходимые для запроса

        Returns:
            str: Строка запроса
        """

        raise NotImplementedError("Method %s is not implemented" % __name__)

    def get_statistic(self, tables):
        """
        возвращает статистику таблиц

        Args:
            tables(list): Список таблиц

        Returns:
            dict: Статистические данные
            {'table_name': ({'count': кол-во строк, 'size': объем памяти строк)}
        """

        tables_str = self.get_tables_str(tables)
        stats_query = self.db_map.stat_query.format(tables_str, self.source.db)
        stat_records = self.get_query_result(stats_query)

        return self.processing_statistic(stat_records)

    @classmethod
    def get_interval_query(cls, cols_info):
        """
        список запросов на min max значений для колонок с датами

        Args:
            source(Datasource): экземпляр источника данных
            cols_info(list): Список данных о колонках

        """
        intervals_query = []
        for table, col_name, col_type, _, _, _ in cols_info:
            if col_type.lower() in cls.db_map.dates:
                query = "SELECT MIN({0}), MAX({0}) FROM {1}".format(
                        col_name, table)
                intervals_query.append([table, col_name, query])
        return intervals_query

    def get_intervals(self, cols_info):
        """
        Возращается список интервалов для полей типа Дата

        Args:
            source('Datasource'): Источник
            cols_info(list): Информация о колонках

        Returns:
            dict: Информация о крайних значениях дат
        """
        res = defaultdict(list)
        interval_queries = self.get_interval_query(cols_info)
        now = time.mktime(datetime.datetime.now().timetuple())
        for table, col, query in interval_queries:
            start_date, end_date = self.get_query_result(query)[0]
            res[table].append({
                'last_updated': now,
                'name': col,
                'startDate': start_date,
                'endDate': end_date,
            })
        return res

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
        return {x[0].lower(): ({'count': int(x[1]), 'size': x[2]}
                if (x[1] and x[2]) else None) for x in records}

    def get_columns_info(self, tables):
        """
        Получение списка колонок в таблицах

        Args:
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
            self.source, tables)

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


    @classmethod
    def get_page_select_query(cls, table_name, cols):
        """
        Формирование строки запроса на получение данных (с дальнейшей пагинацией)

        Args:
            table_name(unicode): Название таблицы
            cols(list): Список получаемых колонок
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def local_table_insert_query(table_name, cols_num):
        """
        Запрос на добавление в новую таблицу локал хранилища

        Args:
            table_name(str): Название таблиц
            cols_num(int): Число столбцов
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
        return "SELECT {0} FROM {1}"

    @classmethod
    def remote_table_create_query(cls):
        """
        запрос на создание новой таблицы в БД клиента

        Returns:
            str: строка запроса
        """
        return cls.db_map.remote_table_query

    @classmethod
    def remote_triggers_create_query(cls):
        """
        запрос на создание триггеров в БД клиента

        Returns:
            str: строка запроса
        """
        return cls.db_map.remote_triggers_query

    @classmethod
    def get_primary_key(cls, table, db):
        """
        Запрос на получение первичного ключа
        Args:
            table(str): название таблицы
            db(str): название базы данных

        Returns:
            str: запрос на получение первичного ключа
        """
        return cls.db_map.pr_key_query.format("('{0}')".format(table), db)

    @classmethod
    def delete_primary_query(cls, table, primary):
        return cls.db_map.delete_primary_key.format(table, primary)

    @staticmethod
    def get_remote_trigger_names(table_name):
        """
        Названия создаваемых триггеров для remote источников
        Args:
            table_name:
        """
        raise NotImplementedError("Method %s is not implemented" % __name__)

    @staticmethod
    def get_date_table_names(col_type):
        """
        Получене запроса на создание таблицы даты
        Returns:
            list: Список строк с названием и типом колонок для таблицы дат
        """
        date_table_col_names = ['"time_id" integer PRIMARY KEY']
        date_table_col_names.extend([
            '"{0}" {1}'.format(field, f_type) for field, f_type
            in col_type])
        return date_table_col_names

    @staticmethod
    def get_table_create_col_names(fields, time_table_name):
        col_names = ['"cdc_key" text PRIMARY KEY']
        for table, field in fields:
            field_name = "{0}{1}{2}".format(table, FIELD_NAME_SEP, field['name'])
            if field['type'] != 'timestamp':

                col_names.append('"{0}" {1}'.format(
                    field_name, field['type']))
            else:
                col_names.append('"{0}" integer REFERENCES {1} (time_id)'.format(
                    field_name, time_table_name))

        return col_names

    @staticmethod
    def cdc_key_delete_query(table_name):

        return 'DELETE from {0} where cdc_key = ANY(%s);'.format(table_name, '%s')

    @staticmethod
    def get_fetchall_result(connection, query, **kwargs):
        """
        возвращает результат fetchall преобразованного запроса с аргументами
        """
        with connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(query, kwargs)
                return cursor.fetchall()

    def get_processed_for_triggers(self, columns):
        """
        Получает инфу о колонках, возвращает преобразованную инфу
        для создания триггеров
        """

        cols_str = ''
        new = ''
        old = ''
        cols = ''
        sep = self.get_separator()

        for col in columns:
            name = col['name']
            new += 'NEW.{0}, '.format(name)
            old += 'OLD.{0}, '.format(name)
            cols += ('{name}, '.format(name=name))
            cols_str += ' {sep}{name}{sep} {typ}{length},'.format(
                sep=sep, name=name, typ=col['type'],
                length='({0})'.format(col['max_length'])
                if col['max_length'] is not None else ''
            )

        return {
            'cols_str': cols_str, 'new': new, 'old': old, 'cols': cols,
        }

    @staticmethod
    def get_processed_indexes(indexes):
        """
        Получает инфу об индексах, возвращает преобразованную инфу
        для создания триггеров
        """
        # indexes_query смотреть
        index_cols_i, index_name_i = 1, 2

        return [[index[index_cols_i], index[index_name_i]] for index in indexes]

    @staticmethod
    def get_required_indexes():
        # название и колонки индексов, необходимые для вспомогательной таблицы триггеров
        return {
            '{0}_created': ['cdc_created_at', ],
            '{0}_synced': ['cdc_synced', ],
            '{0}_syn_upd': ['cdc_synced', 'cdc_updated_at', ],
        }
