# coding: utf-8

from __future__ import unicode_literals
import logging

import os
import sys
from datetime import datetime, timedelta
import json
import calendar
import math

import requests
from psycopg2 import errorcodes
from etl.constants import *
from etl.services.db.factory import DatabaseService
from etl.services.middleware.base import EtlEncoder
from pymondrian.schema import (
    Schema, PhysicalSchema, Table, Cube as CubeSchema,
    Dimension as DimensionSchema, Attribute, Level,
    Hierarchy, MeasureGroup, Measure as MeasureSchema,
    Key, Name, ForeignKeyLink, ReferenceLink)
from pymondrian.generator import generate
from etl.helpers import DataSourceService
from core.models import (
    Datasource, Dimension, Measure, DatasourceMeta,
    DatasourceMetaKeys, DatasourceSettings, Dataset, DatasetToMeta)
from django.conf import settings
from django.core.urlresolvers import reverse
from djcelery import celery
from itertools import groupby, izip
from etl.services.queue.base import *

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"


logger = logging.getLogger(__name__)

ASC = 1


@celery.task(name=CREATE_DATASET)
def create_dataset(task_id, channel):
    return CreateDataset(task_id, channel).load_data()


@celery.task(name=MONGODB_DATA_LOAD)
def load_mongo_db(task_id, channel):
    return LoadMongodb(task_id, channel).load_data()


@celery.task(name=DB_DATA_LOAD)
def load_db(task_id, channel):
    return LoadDb(task_id, channel).load_data()


@celery.task(name=GENERATE_DIMENSIONS)
def load_dimensions(task_id, channel):
    return LoadDimensions(task_id, channel).load_data()


@celery.task(name=GENERATE_MEASURES)
def load_measures(task_id, channel):
    return LoadMeasures(task_id, channel).load_data()


@celery.task(name=MONGODB_DELTA_LOAD)
def update_mongo_db(task_id, channel):
    return UpdateMongodb(task_id, channel).load_data()


@celery.task(name=DB_DETECT_REDUNDANT)
def detect_redundant(task_id, channel):
    return DetectRedundant(task_id, channel).load_data()


@celery.task(name=DB_DELETE_REDUNDANT)
def delete_redundant(task_id, channel):
    return DeleteRedundant(task_id, channel).load_data()


@celery.task(name=CREATE_TRIGGERS)
def create_triggers(task_id, channel):
    return CreateTriggers(task_id, channel).load_data()


@celery.task(name=CREATE_CUBE)
def create_cube(task_id, channel):
    return CreateCube(task_id, channel).load_data()


class CreateDataset(TaskProcessing):
    """
    Создание Dataset
    """

    def processing(self):

        dataset, created = Dataset.objects.get_or_create(key=self.key)
        self.context['dataset_id'] = dataset.id

        if not self.context['db_update']:
            self.next_task_params = (
                MONGODB_DATA_LOAD, load_mongo_db, self.context)
        else:
            self.next_task_params = (
                MONGODB_DELTA_LOAD, update_mongo_db, self.context)


class LoadMongodb(TaskProcessing):
    """
    Первичная загрузка данных в Mongodb
    """

    def processing(self):
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
        structure = self.context['tree']
        source = Datasource.objects.get(**self.context['source'])
        meta_info = json.loads(self.context['meta_info'])

        page = 1
        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

        # общее количество строк в запросе
        rows_count, loaded_count = DataSourceService.get_structure_rows_number(
            source, structure,  cols), 0
        self.publisher.rows_count = rows_count
        self.publisher.publish(TLSE.START)

        col_names = ['_id', '_state', '_date']
        for t_name, col_group in groupby(cols, lambda x: x["table"]):
            for x in col_group:
                col_names.append(
                    STANDART_COLUMN_NAME.format(x["table"], x["col"]))

        # находим бинарные данные для 1) создания ключей 2) инсерта в монго
        binary_types_list = get_binary_types_list(cols, col_types)

        # создаем коллекцию и индексы в Mongodb
        collection = MongodbConnection(
            self.get_table(STTM_DATASOURCE), indexes=[
                ('_id', ASC), ('_state', ASC), ('_date', ASC)]).collection

        # Коллекция с текущими данными
        current_collection_name = self.get_table(STTM_DATASOURCE_KEYS)
        MongodbConnection.drop(current_collection_name)
        current_collection = MongodbConnection(
            current_collection_name, indexes=[('_id', ASC)]).collection

        source_db_connect = SourceDbConnect(
            DataSourceService.get_source_rows_query(source, structure, cols),
            source)

        tables_key_creator = [
            RowKeysCreator(table=table, cols=cols, meta_data=value)
            for table, value in meta_info.iteritems()]

        while True:
            rows = source_db_connect.fetchall(limit=limit, offset=(page-1)*limit)
            if not rows:
                break

            data_to_insert = []
            data_to_current_insert = []
            for ind, record in enumerate(rows, start=1):
                row_key = calc_key_for_row(
                    record, tables_key_creator, (page - 1) * limit + ind,
                    binary_types_list)

                # бинарные данные оборачиваем в Binary(), если они имеются
                new_record = process_binary_data(record, binary_types_list)

                record_normalized = (
                    [row_key, STSE.IDLE, EtlEncoder.encode(datetime.now())] +
                    [EtlEncoder.encode(rec_field) for rec_field in new_record])
                data_to_insert.append(dict(izip(col_names, record_normalized)))
                data_to_current_insert.append(dict(_id=row_key))
            try:
                collection.insert_many(data_to_insert, ordered=False)
                current_collection.insert_many(
                    data_to_current_insert, ordered=False)
                loaded_count += ind
                print 'inserted %d rows to mongodb. Total inserted %s/%s.' % (
                    ind, loaded_count, rows_count)
            except Exception as e:
                self.error_handling(e.message)

            # обновляем информацию о работе таска
            self.queue_storage.update()
            self.publisher.loaded_count += limit
            self.publisher.publish(TLSE.PROCESSING)
            self.queue_storage['percent'] = (
                100 if self.publisher.is_complete else self.publisher.percent)

            page += 1

        self.context['rows_count'] = rows_count
        self.next_task_params = (DB_DATA_LOAD, load_db, self.context)


class LoadDb(TaskProcessing):

    def processing(self):
        """
        Загрузка данных из Mongodb в базу данных
        """
        self.key = self.context['checksum']
        self.user_id = self.context['user_id']
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
        # structure = self.context['tree']
        self.db_update = self.context['db_update']
        rows_count, loaded_count = self.context['rows_count'], 0

        source = Datasource()
        source.set_from_dict(**self.context['source'])
        self.publisher.rows_count = self.context['rows_count']
        self.publisher.publish(TLSE.START)

        processed_cols, clear_col_names, date_fields = self.get_processed_cols(
            cols, col_types)

        col_names = DataSourceService.get_table_create_col_names(
                processed_cols, self.key)

        source_table_name = self.get_table(STTM_DATASOURCE)
        source_collection = MongodbConnection(source_table_name).collection

        # работа с таблицей дат
        self.create_date_tables()

        if not self.db_update:
            # создаем таблицу sttm_datasource_
            LocalDbConnect(DataSourceService.get_table_create_query(
                source_table_name, ', '.join(col_names)))

        binary_types_dict = get_binary_types_dict(cols, col_types)
        local_insert = LocalDbConnect(DataSourceService.get_table_insert_query(
            source_table_name, len(clear_col_names)), execute=False)

        limit, offset = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT, 0
        last_row = None

        # Пишем данные в базу
        while True:
            try:
                collection_cursor = source_collection.find(
                    {'_state': STSE.IDLE},
                    limit=limit, skip=offset)
                rows_dict = []
                for record in collection_cursor:
                    temp_dict = {}
                    for ind, col_name in enumerate(clear_col_names):
                        # если дата, достаем id
                        if col_name in date_fields:
                            # если дата есть, то достаем ее из self.date_tables,
                            # если дата пустая, проставляем id = 0
                            temp_dict.update({
                                str(ind): self.date_tables[record[col_name]]
                                if record[col_name] is not None else 0
                                })
                        else:
                            temp_dict.update(
                                {str(ind): record['_id'] if col_name == 'cdc_key'
                                    else record[col_name]})
                    rows_dict.append(temp_dict)
                if not rows_dict:
                    break

                local_insert.execute(self.binary_wrap(
                    rows_dict, binary_types_dict), many=True)
                offset += limit
                loaded_count += len(rows_dict)
                print 'inserted %d rows to database. Total inserted %s/%s.' % (
                    len(rows_dict), loaded_count, rows_count)
            except Exception as e:
                self.was_error = True
                # код и сообщение ошибки
                pg_code = getattr(e, 'pgcode', None)

                err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
                err_msg += e.message
                self.error_handling(err_msg, pg_code)
            else:
                last_row = rows_dict[-1]  # получаем последнюю запись
                # обновляем информацию о работе таска
                self.queue_storage.update()
                self.publisher.loaded_count += limit
                self.publisher.publish(TLSE.PROCESSING)
                self.queue_storage['percent'] = (
                    100 if self.publisher.is_complete
                    else self.publisher.percent)

        source_collection.update_many(
            {'_state': STSE.IDLE}, {'$set': {'_state': STSE.LOADED}})

        # работа с datasource_meta
        DataSourceService.update_datasource_meta(
            self.key, source, cols, json.loads(self.context['meta_info']),
            last_row, self.context['dataset_id'])
        if last_row:
            DataSourceService.update_collections_stats(
                self.context['collections_names'], last_row['0'])

        if self.context['cdc_type'] != DatasourceSettings.TRIGGERS:
            self.next_task_params = (DB_DETECT_REDUNDANT, detect_redundant, {
                'checksum': self.key,
                'db_update': self.db_update,
                'user_id': self.user_id,
                'source_id': source.id,
                'cols': self.context['cols'],
                'col_types': self.context['col_types'],
                'dataset_id': self.context['dataset_id'],
                'meta_info': self.context['meta_info'],
                'rows_count': rows_count,
            })
        else:
            self.next_task_params = (CREATE_TRIGGERS, create_triggers, {
                'checksum': self.key,
                'db_update': self.db_update,
                'user_id': self.user_id,
                'tables_info': self.context['tables_info'],
                'source_id': source.id,
                'cols': self.context['cols'],
                'col_types': self.context['col_types'],
                'dataset_id': self.context['dataset_id'],
                'meta_info': self.context['meta_info'],
                'rows_count': rows_count,
            })

    def get_processed_cols(self, cols, col_types):

        processed_cols = []
        # простой список колонок
        clear_col_names = ['cdc_key']
        # информация о колонках с типом дата
        date_fields = []

        for obj in cols:
            table = obj['table']
            col = obj['col']
            col_type = TYPES_MAP.get(
                col_types['{0}.{1}'.format(table, col)].lower())

            processed_cols.append((table, {'name': col, 'type': col_type}))

            st_col_name = STANDART_COLUMN_NAME.format(table, col)
            clear_col_names.append(st_col_name)

            if col_type in ['timestamp']:
                date_fields.append(st_col_name)

        return processed_cols, clear_col_names, date_fields

    def create_date_tables(self):
        """
        Создание таблиц дат

        Args:
            date_intervals_info(list):
                Список данных об интервалах дат в таблицах
            ::
                [
                    (
                        <table_name1>,
                        [
                            {'startDate': <start_date1>, 'endDate': <end_date1>,
                            'name': <col_name1>},
                            ...
                        ]
                    )
                    ...
                ]
        """

        self.date_tables = {}

        date_intervals_info = fetch_date_intervals(
                json.loads(self.context['meta_info']))
        # список всех интервалов
        intervals = reduce(
            lambda x, y: x[1]+y[1], date_intervals_info, ['', []])

        if not intervals:
            return

        # если имеются колонки с типом дата
        insert_query = DataSourceService.get_table_insert_query(
            self.get_table(TIME_TABLE), DATE_TABLE_COLS_LEN)
        insert_db_connect = LocalDbConnect(insert_query, execute=False)

        # как минимум создадим таблицу с 1 записью c id = 0
        # для пустых значений, если новая закачка
        if not self.db_update:
            LocalDbConnect(DataSourceService.get_table_create_query(
                self.get_table(TIME_TABLE),
                ', '.join(DataSourceService.get_date_table_names(DTCN.types))))

            none_row = {'0': 0}
            none_row.update({str(x): None for x in range(1, DATE_TABLE_COLS_LEN)})
            insert_db_connect.execute([none_row], many=True)

        # если в таблице в колонке все значения дат пустые, то
        # startDate и endDate = None, отфильтруем
        not_none_intervals = [
            i for i in intervals if i['startDate'] is not None and
            i['endDate'] is not None
        ]

        if not not_none_intervals:
            return
        # если имеются численные интервалы
        # новые границы дат, пришедшие с пользователя
        start_date = min(
            [datetime.strptime(interval['startDate'], "%d.%m.%Y").date()
             for interval in not_none_intervals])
        end_date = max(
            [datetime.strptime(interval['endDate'], "%d.%m.%Y").date()
             for interval in not_none_intervals])

        delta = end_date - start_date

        # первое заполнение таблицы дат
        if not self.db_update:
            # +1 потому start_date тоже суем
            records = self.prepare_dates_records(
                start_date, delta.days + 1, 0)

            insert_db_connect.execute(records, many=True)

        # update таблицы дат
        else:
            select_dates_query = DatabaseService.get_select_dates_query(
                self.get_table(TIME_TABLE))
            dates_db_connect = LocalDbConnect(select_dates_query, execute=False)
            db_exists_dates = dates_db_connect.fetchall()

            exists_dates = {x[0].strftime("%d.%m.%Y"): x[1] for x in db_exists_dates}

            self.date_tables = exists_dates

            # если в таблице дат имелись записи кроме id = 0
            if exists_dates:
                max_id = max(exists_dates.itervalues())
                dates_list = [x[0] for x in db_exists_dates]
                exist_min_date = min(dates_list)
                exist_max_date = max(dates_list)

                min_delta = (exist_min_date - start_date).days

                if min_delta > 0:
                    records = self.prepare_dates_records(
                        start_date, min_delta, max_id)

                    insert_db_connect.execute(records, many=True)

                    max_id += min_delta

                max_delta = (end_date - exist_max_date).days

                if max_delta > 0:
                    records = self.prepare_dates_records(
                        exist_max_date+timedelta(days=1), max_delta, max_id)

                    insert_db_connect.execute(records, many=True)

                    max_id += max_delta

            # если в таблице дат имелся только 1 запись с id = 0
            else:
                # +1 потому start_date тоже суем
                records = self.prepare_dates_records(
                    start_date, delta.days + 1, 0)

                insert_db_connect.execute(records, many=True)

    def prepare_dates_records(self, start_date, days_count, max_id):
        # список рекордов для таблицы дат
        records = []
        for day_delta in range(days_count):
            current_day = start_date + timedelta(days=day_delta)
            current_day_str = current_day.isoformat()
            month = current_day.month
            records.append({
                    '0': max_id + day_delta + 1,
                    '1': current_day_str,
                    '2': calendar.day_name[current_day.weekday()],
                    '3': current_day.year,
                    '4': month,
                    '5': current_day.strftime('%B'),
                    '6': current_day.day,
                    '7': current_day.isocalendar()[1],
                    '8': int(math.ceil(float(month) / 3)),

                 })
            self.date_tables.update(
                {current_day.strftime("%d.%m.%Y"): max_id + day_delta + 1})

        return records


class LoadDimensions(TaskProcessing):
    """
    Создание рамерности, измерения олап куба
    """
    table_prefix = DIMENSIONS
    actual_fields_type = ['text', Measure.TIME, Measure.DATE, Measure.TIMESTAMP]

    @staticmethod
    def get_column_title(meta_info, table, column):
        title = None
        for col_info in meta_info[table]['columns']:
            if col_info['name'] == column['name']:
                title = col_info.get('title', None)
                break
        return title

    def get_actual_fields(self, meta_data):
        """
        Фильтруем поля по необходимому нам типу

        Args:
            meta_data: Метаданные всех полей

        Returns:
            list of tuple: Метаданные отфильтрованных полей
        """
        actual_fields = []
        for record in meta_data:

            for field in json.loads(record['meta__fields'])['columns']:
                f_type = TYPES_MAP.get(field['type'])
                if f_type in self.actual_fields_type:
                    actual_fields.append((
                        record['meta__collection_name'], field))

        return actual_fields

    def processing(self):
        self.key = self.context['checksum']
        # Наполняем контекст
        source = Datasource.objects.get(id=self.context['source_id'])
        meta_tables = {
            k: v for (k, v) in
            DatasourceMeta.objects.filter(
                datasource=source).values_list('collection_name', 'id')}
        meta_data = DatasourceMetaKeys.objects.filter(
            value=self.key).values(
            'meta__collection_name', 'meta__fields', 'meta__stats')
        self.actual_fields = self.get_actual_fields(meta_data)

        if not self.context['db_update']:

            create_col_names = DataSourceService.get_table_create_col_names(
                self.actual_fields, self.key)
            LocalDbConnect(DataSourceService.get_table_create_query(
                self.get_table(self.table_prefix), ', '.join(create_col_names)))

            try:
                self.save_fields()
            except Exception as e:
                # код и сообщение ошибки
                pg_code = getattr(e, 'pgcode', None)

                err_msg = '%s: ' % errorcodes.lookup(pg_code) if pg_code else ''
                err_msg += e.message
                self.error_handling(err_msg)

        # Сохраняем метаданные
        self.save_meta_data(
            self.user_id, self.actual_fields, meta_tables)

        self.create_reload_triggers()

        if not self.last_task:
            self.set_next_task_params()

    def set_next_task_params(self):
        self.next_task_params = (
            GENERATE_MEASURES, load_measures, self.context)

    def save_meta_data(self, user_id, fields, meta_tables):
        """
        Сохранение метаинформации
0
        Args:
            user_id(int): id пользователя
            fields(list): данные о полях
            meta_tables(dict): ссылка на метаданные хранилища
        """

        meta_info = json.loads(self.context['meta_info'])

        level = dict()
        for table, field in fields:
            datasource_meta_id = DatasourceMeta.objects.get(
                id=meta_tables[table])
            table_name = STANDART_COLUMN_NAME.format(table, field['name'])
            level.update(dict(
                type=field['type'], level_type='regular', visible=True,
                column=table_name, unique_members=field['is_unique'],
                caption=table_name,
                )
            )

            data = dict(
                name=table_name,
                has_all=True,
                table_name=table_name,
                level=level,
                primary_key='id',
                foreign_key=None
            )

            title = self.get_column_title(meta_info, table, field)

            dimension, created = Dimension.objects.get_or_create(
                name=table_name,
                user_id=user_id,
                datasources_meta=datasource_meta_id,
                type=Dimension.TIME_DIMENSION if field['type'] in ['timestamp']
                else Dimension.STANDART_DIMENSION
            )
            # ставим title размерности
            dimension.title = title if title is not None else table_name
            dimension.data = json.dumps(data)
            dimension.save()

    def get_splitted_table_column_names(self):
        """
        Возвращает имена колонки вида 'table__column'
        """
        column_names = []
        for table, field in self.actual_fields:
            column_names.append(
                STANDART_COLUMN_NAME.format(table, field['name']))
        return column_names

    def filter_columns(self, cols):
        """
        Достаем инфу только тех колонок, которые используются
        в мерах и размерностях
        """
        dim_meas_cols_info = []
        for (act_table, col_info) in self.actual_fields:
            for c in cols:
                if c['table'] == act_table and c['col'] == col_info['name']:
                    dim_meas_cols_info.append(c)
        return dim_meas_cols_info

    def save_fields(self):
        """
        Заполняем таблицу данными
        """

        column_names = ['cdc_key']
        rows_count, loaded_count = self.context['rows_count'], 0
        date_fields_order = {}
        index = 1
        for table, field in self.actual_fields:
            if field['type'] in ['timestamp']:
                date_fields_order.update(
                    {index: STANDART_COLUMN_NAME.format(table, field['name'])})
            column_names.append(
                STANDART_COLUMN_NAME.format(table, field['name']))
            index += 1
        col_nums = len(column_names)

        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])

        dim_meas_cols = self.filter_columns(cols)

        # инфа о бинарных данных для инсерта в постгрес
        binary_types_dict = get_binary_types_dict(dim_meas_cols, col_types)

        local_insert = LocalDbConnect(DataSourceService.get_table_insert_query(
            self.get_table(self.table_prefix), col_nums), execute=False)

        step, offset = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT, 0
        source_connect = LocalDbConnect(DataSourceService.get_page_select_query(
            self.get_table(STTM_DATASOURCE), column_names), execute=False)

        while True:
            rows = source_connect.fetchall(limit=step, offset=offset)
            if not rows:
                break
            rows_dict = []
            for record in rows:
                temp_dict = {}
                for ind in xrange(col_nums):
                    # if ind in date_fields_order.keys():
                    #     # заменяем дату идентификатором из связой таблицы
                    #     i = self.date_tables[record[ind].date()] if record[ind] else 0
                    #     temp_dict.update({str(ind): i})
                    # else:
                    temp_dict.update({str(ind): record[ind]})

                rows_dict.append(temp_dict)
            local_insert.execute(self.binary_wrap(
                rows_dict, binary_types_dict), many=True)
            loaded_count += len(rows_dict)
            print ('inserted %d %s to database. '
                   'Total inserted %s/%s.' % (
                    len(rows_dict), self.table_prefix, loaded_count, rows_count))
            offset += step

            self.queue_storage.update()

    def create_reload_triggers(self):
        """
        Создание триггеров для размерностей и мер,
        обеспечивающих синхронизацию данных для источника
        """
        local_instance = DataSourceService.get_local_instance()
        sep = local_instance.get_separator()

        column_names = ['cdc_key'] + self.get_splitted_table_column_names()
        insert_cols = []
        select_cols = []
        for col in column_names:
            insert_cols.append('NEW.{1}{0}{1}'.format(col, sep))
            select_cols.append('{1}{0}{1}'.format(col, sep))

        query_params = dict(
            new_table=self.get_table(self.table_prefix),
            orig_table=self.get_table(STTM_DATASOURCE),
            del_condition="{0}cdc_key{0}=OLD.{0}cdc_key{0}".format(sep),
            insert_cols=','.join(insert_cols),
            cols="({0})".format(','.join(select_cols)),
        )

        reload_trigger_query = (
            DataSourceService.reload_datasource_trigger_query(query_params))

        LocalDbConnect(reload_trigger_query).execute()


class LoadMeasures(LoadDimensions):
    """
    Создание мер
    """
    table_prefix = MEASURES
    actual_fields_type = [Measure.INTEGER, Measure.BOOLEAN]

    def save_meta_data(self, user_id, fields, meta_tables):
        """
        Сохранение информации о мерах

        Args:
            user_id(int): id пользователя
            fields(list): данные о полях
            meta_tables(dict): ссылка на метаданные хранилища
        """

        meta_info = json.loads(self.context['meta_info'])

        for table, field in fields:
            datasource_meta_id = DatasourceMeta.objects.get(
                id=meta_tables[table])
            table_name = STANDART_COLUMN_NAME.format(
                table, field['name'])

            title = self.get_column_title(meta_info, table, field)

            measure, created = Measure.objects.get_or_create(
                name=table_name,
                type=field['type'],
                user_id=user_id,
                datasources_meta=datasource_meta_id
            )
            # ставим title мере
            measure.title = title if title is not None else table_name
            measure.save()

    def set_next_task_params(self):
        self.next_task_params = (
            CREATE_CUBE, create_cube, self.context)

    def processing(self):
        super(LoadMeasures, self).processing()


class UpdateMongodb(TaskProcessing):

    def processing(self):
        """
        1. Процесс обновленения данных в коллекции `sttm_datasource_delta_{key}`
        новыми данными с помощью `sttm_datasource_keys_{key}`
        2. Создание коллекции `sttm_datasource_keys_{key}` c ключами для
        текущего состояния источника
        """
        self.key = self.context['checksum']
        cols = json.loads(self.context['cols'])
        col_types = json.loads(self.context['col_types'])
        structure = self.context['tree']
        source = Datasource.objects.get(**self.context['source'])
        meta_info = json.loads(self.context['meta_info'])

        rows_count, loaded_count = DataSourceService.get_structure_rows_number(
            source, structure,  cols), 0

        # общее количество строк в запросе
        self.publisher.rows_count = rows_count
        self.publisher.publish(TLSE.START)

        col_names = ['_id', '_state', '_date']
        for t_name, col_group in groupby(cols, lambda x: x["table"]):
            for x in col_group:
                col_names.append(
                    STANDART_COLUMN_NAME.format(x["table"], x["col"]))

        # находим бинарные данные для 1) создания ключей 2) инсерта в монго
        binary_types_list = get_binary_types_list(cols, col_types)

        collection = MongodbConnection(self.get_table(STTM_DATASOURCE)).collection

        # Коллекция с текущими данными
        current_collection_name = self.get_table(STTM_DATASOURCE_KEYS)
        MongodbConnection.drop(current_collection_name)
        current_collection = MongodbConnection(
            current_collection_name, indexes=[('_id', ASC)]).collection

        # Дельта-коллекция
        delta_collection = MongodbConnection(
            self.get_table(STTM_DATASOURCE_DELTA), indexes=[
                ('_id', ASC), ('_state', ASC), ('_date', ASC)]).collection

        source_query = DataSourceService.get_source_rows_query(
            source, structure, cols)
        source_db_connect = SourceDbConnect(source_query, source)

        tables_key_creator = [
            RowKeysCreator(table=table, cols=cols, meta_data=value)
            for table, value in meta_info.iteritems()]

        # Выявляем новые записи в базе и записываем их в дельта-коллекцию
        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        page = 1
        while True:
            rows = source_db_connect.fetchall(limit=limit, offset=(page-1)*limit)
            if not rows:
                break
            data_to_insert = []
            data_to_current_insert = []
            for ind, record in enumerate(rows, start=1):
                row_key = calc_key_for_row(
                    record, tables_key_creator, (page - 1) * limit + ind,
                    binary_types_list)

                # бинарные данные оборачиваем в Binary(), если они имеются
                new_record = process_binary_data(record, binary_types_list)

                if not collection.find({'_id': row_key}).count():
                    delta_rows = (
                        [row_key, DTSE.NEW, EtlEncoder.encode(datetime.now())] +
                        [EtlEncoder.encode(rec_field) for rec_field in new_record])
                    data_to_insert.append(dict(izip(col_names, delta_rows)))

                data_to_current_insert.append(dict(_id=row_key))
            loaded_count += ind
            print 'updated %d rows to mongodb. Total inserted %s/%s.' % (
                ind, loaded_count, rows_count)

            try:
                if data_to_insert:
                    delta_collection.insert_many(data_to_insert, ordered=False)
                current_collection.insert_many(
                    data_to_current_insert, ordered=False)
            except Exception as e:
                self.error_handling(e.message)
            page += 1

        # Обновляем основную коллекцию новыми данными
        page = 1
        while True:
            delta_data = delta_collection.find(
                {'_state': DTSE.NEW},
                limit=limit, skip=(page-1)*limit).sort('_date', ASC)
            to_ins = []
            for record in delta_data:
                record['_state'] = STSE.IDLE
                to_ins.append(record)
            if not to_ins:
                break
            try:
                collection.insert_many(to_ins, ordered=False)
            except Exception as e:
                self.error_handling(e.message)

            page += 1

        # Обновляем статусы дельты-коллекции
        delta_collection.update_many(
            {'_state': DTSE.NEW}, {'$set': {'_state': DTSE.SYNCED}})

        self.context['rows_count'] = rows_count
        self.next_task_params = (DB_DATA_LOAD, load_db, self.context)


class DetectRedundant(TaskProcessing):

    def processing(self):
        """
        Выявление записей на удаление
        """
        self.key = self.context['checksum']
        source_collection = MongodbConnection(
            self.get_table(STTM_DATASOURCE)).collection
        current_collection = MongodbConnection(
            self.get_table(STTM_DATASOURCE_KEYS)).collection

        # Обновляем коллекцию всех ключей
        all_keys_collection_name = self.get_table(STTM_DATASOURCE_KEYSALL)
        all_keys_collection = MongodbConnection(
            all_keys_collection_name,
            indexes=[('_state', ASC), ('_deleted', ASC)]).collection

        source_collection.aggregate(
            [{"$match": {"_state": STSE.LOADED}},
             {"$project": {"_id": "$_id", "_state": {"$literal": AKTSE.NEW}}},
             {"$out": "%s" % all_keys_collection_name}])

        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        page = 1
        while True:

            to_delete = []
            records_for_del = list(all_keys_collection.find(
                {'_state': AKTSE.NEW}, limit=limit, skip=(page - 1) * limit))
            if not len(records_for_del):
                break
            for record in records_for_del:
                row_key = record['_id']
                if not current_collection.find({'_id': row_key}).count():
                    to_delete.append(row_key)
            try:
                all_keys_collection.update_many(
                    {'_id': {'$in': to_delete}},
                    {'$set': {'_deleted': True}})

                source_collection.delete_many({'_id': {'$in': to_delete}})
            except Exception as e:
                self.error_handling(e.message)

            page += 1

        all_keys_collection.update_many(
            {'_state': AKTSE.NEW}, {'$set': {'_state': AKTSE.SYNCED}})

        self.next_task_params = (
            DB_DELETE_REDUNDANT, delete_redundant, self.context)


class DeleteRedundant(TaskProcessing):


    """
    Удаление записей из таблицы-источника

    1. Находит в коллекции <STTM_DATASOURCE_KEYSALL> записи с условием
        '_deleted': True
    2. Удаляет эти записи из таблицы <STTM_DATASOURCE_KEYSALL>
    """

    def processing(self):
        self.key = self.context['checksum']
        del_collection = MongodbConnection(
            self.get_table(STTM_DATASOURCE_KEYSALL)).collection

        delete_connect = LocalDbConnect(DataSourceService.cdc_key_delete_query(
            self.get_table(STTM_DATASOURCE)), execute=False)

        limit = 100
        page = 1
        while True:
            delete_delta = del_collection.find(
                {'_deleted': True}, limit=limit, skip=(page-1)*limit)
            delete_records = [str(record['_id']) for record in delete_delta]
            if not delete_records:
                break
            try:
                delete_connect.execute((delete_records,))
            except Exception as e:
                self.error_handling(e.message)
            page += 1

        self.next_task_params = (
            GENERATE_DIMENSIONS, load_dimensions, self.context)


class CreateTriggers(TaskProcessing):

    def processing(self):
        """
        Создание триггеров в БД пользователя
        """
        tables_info = self.context['tables_info']

        source = Datasource.objects.get(id=self.context['source_id'])

        db_instance = DatabaseService.get_source_instance(source)

        remote_table_create_query = db_instance.remote_table_create_query()
        remote_triggers_create_query = db_instance.remote_triggers_create_query()

        connection = db_instance.connection
        cursor = connection.cursor()

        for table, columns in tables_info.iteritems():

            table_name = '_etl_datasource_cdc_{0}'.format(table)
            tables_str = "('{0}')".format(table_name)

            cdc_cols_query = db_instance.db_map.cdc_cols_query.format(
                tables_str, source.db, 'public')

            cursor.execute(cdc_cols_query)
            fetched_cols = cursor.fetchall()

            existing_cols = {k: v for (k, v) in fetched_cols}

            required_indexes = {k.format(table_name): v
                                for k, v in REQUIRED_INDEXES.iteritems()}

            for_triggers = DatabaseService.get_processed_for_triggers(
                source, columns)
            cols_str = for_triggers['cols_str']

            # если таблица существует
            if existing_cols:
                # удаление primary key, если он есть
                primary_query = db_instance.get_primary_key(table_name, source.db)
                cursor.execute(primary_query)
                primary = cursor.fetchall()

                if primary:
                    primary_name = primary[0][0]
                    del_pr_query = db_instance.delete_primary_query(
                        table_name, primary_name)
                    cursor.execute(del_pr_query)

                # добавление недостающих колонок, не учитывая cdc-колонки
                new_came_cols = [
                    {
                        'col_name': x['name'],
                        'col_type': x["type"],
                        'max_length': '({0})'.format(x['max_length'])
                            if x['max_length'] is not None else '',
                        'nullable': 'not null' if x['is_nullable'] == 'NO' else ''
                    }
                    for x in columns]

                diff_cols = [x for x in new_came_cols
                             if x['col_name'] not in existing_cols]

                add_col_q = db_instance.db_map.add_column_query
                del_col_q = db_instance.db_map.del_column_query

                for d_col in diff_cols:
                    cursor.execute(add_col_q.format(
                        table_name,
                        d_col['col_name'],
                        d_col['col_type'],
                        d_col['max_length'],
                        d_col['nullable']
                    ))
                    connection.commit()

                # проверка cdc-колонок на существование и типы
                cdc_required_types = db_instance.db_map.cdc_required_types

                for cdc_k, v in cdc_required_types.iteritems():
                    if cdc_k not in existing_cols:
                        cursor.execute(add_col_q.format(
                            table_name, cdc_k, v["type"], "", v["nullable"]))
                    else:
                        # если типы не совпадают
                        if not existing_cols[cdc_k].startswith(v["type"]):
                            cursor.execute(del_col_q.format(table_name, cdc_k))
                            cursor.execute(add_col_q.format(
                                table_name, cdc_k, v["type"], "", v["nullable"]))

                connection.commit()

                # проверяем индексы на колонки и существование,
                # лишние индексы удаляем

                indexes_query = db_instance.db_map.indexes_query.format(
                    tables_str, source.db)
                cursor.execute(indexes_query)
                exist_indexes = cursor.fetchall()

                exist_indexes = DatabaseService.get_processed_indexes(
                    source, exist_indexes)

                index_cols_i, index_name_i = 0, 1

                create_index_q = db_instance.db_map.create_index_query
                drop_index_q = db_instance.db_map.drop_index_query

                allright_index_names = []

                for index in exist_indexes:
                    index_name = index[index_name_i]

                    if index_name not in required_indexes:
                        cursor.execute(drop_index_q.format(index_name, table_name))
                    else:
                        index_cols = sorted(index_name[index_cols_i].split(','))
                        if index_cols != required_indexes[index_name]:
                            cursor.execute(drop_index_q.format(index_name, table_name))
                            cursor.execute(create_index_q.format(
                                index_name, table_name,
                                ','.join(required_indexes[index_name]),
                                source.db))

                        allright_index_names.append(index_name)

                diff_indexes_names = [
                    x for x in required_indexes if x not in allright_index_names]

                for d_index in diff_indexes_names:
                    cursor.execute(
                        create_index_q.format(
                            d_index, table_name,
                            ','.join(required_indexes[d_index])))

                connection.commit()

            # если таблица не существует
            else:
                # создание таблицы у юзера
                cursor.execute(remote_table_create_query.format(
                    table_name, cols_str))

                # создание индексов
                create_index_q = db_instance.db_map.create_index_query

                for index_name, index_cols in required_indexes.iteritems():
                    cursor.execute(create_index_q.format(
                        index_name, table_name,
                        ','.join(index_cols), source.db))

                connection.commit()

            trigger_commands = remote_triggers_create_query.format(
                orig_table=table, new_table=table_name,
                new=for_triggers['new'],
                old=for_triggers['old'],
                cols=for_triggers['cols'])

            # multi queries of mysql, delimiter $$
            for query in trigger_commands.split('$$'):
                cursor.execute(query)

            connection.commit()

        cursor.close()
        connection.close()

        self.next_task_params = (
            GENERATE_DIMENSIONS, load_dimensions, {
                'db_update': self.context['db_update'],
                'checksum': self.key,
                'user_id': self.user_id,
                'source_id': self.context['source_id'],
                'cols': self.context['cols'],
                'col_types': self.context['col_types'],
                'dataset_id': self.context['dataset_id'],
                'meta_info': self.context['meta_info'],
                'rows_count': self.context['rows_count'],
            })


class CreateCube(TaskProcessing):

    """
    Создание схемы
    """

    def processing(self):

        dataset_id = self.context['dataset_id']
        dataset = Dataset.objects.get(id=dataset_id)
        key = dataset.key

        meta_ids = DatasetToMeta.objects.filter(
            dataset_id=dataset_id).values_list('meta_id', flat=True)

        dimensions = Dimension.objects.filter(datasources_meta_id__in=meta_ids)
        measures = Measure.objects.filter(datasources_meta_id__in=meta_ids)

        if not dimensions.exists() and not measures.exists():
            pass
        # <Schema>
        cube_key = "cube_{key}".format(key=key)

        schema = Schema(name=cube_key,
                        description='Cube schema')

        physical_schema = PhysicalSchema()

        dimension_table, measure_table = (
            Table(self.get_table(DIMENSIONS)), Table(self.get_table(MEASURES)))
        physical_schema.add_tables([dimension_table, measure_table])

        cube = CubeSchema(name=cube_key, caption=cube_key,
                          visible=True, cache=False, enabled=True)

        dimension = DimensionSchema(
                name='Dim Table', table=self.get_table(DIMENSIONS), key="Cdc Key")
        dimension.add_attribute(Attribute(name="Cdc Key", key_column="cdc_key"))

        measure_group = MeasureGroup(
            name=self.get_table(MEASURES), table=self.get_table(MEASURES))

        for dim in dimensions:
            # Если не размерность времени, то создаем атрибуты внутри уже
            #  созданной размености, иначе создаем новые размерости под каждое
            # временое поле
            name = dim.name
            title = dim.title

            if dim.type == 'SD':
                dim_attribute = Attribute(name=title, key_column=name)
                dimension.add_attribute(dim_attribute)

                level = Level(
                    attribute=title, visible=True)
                hierarchy = Hierarchy(name='Hierarchy %s' % title)
                hierarchy.add_level(level)
                dimension.add_hierarchies([hierarchy])
            else:
                dim_attribute = Attribute(name=title, key_column='%s_id' % name)
                dimension.add_attribute(dim_attribute)
                measure_group.dimension_links.add_dimension_link(
                    ReferenceLink(
                        dimension='Time Dim', via_dimension='Dim Table',
                        via_attribute=title, attribute='Date'))

        time_table_name = self.get_table(TIME_TABLE)
        time_dim = DimensionSchema(
            name='Time Dim', table=time_table_name,
            key='Time Dim Key', type='TIME')

        year = Attribute(
            name='Year', key_column=DTCN.THE_YEAR, level_type='TimeYears'
        )
        quarter = Attribute(
            name='Quarter', level_type='TimeQuarters',
            attr_key=Key(columns=[DTCN.THE_YEAR, DTCN.QUARTER]),
            attr_name=Name(columns=[DTCN.QUARTER])
        )
        month = Attribute(
            name='Month', level_type='TimeMonths',
            attr_key=Key(columns=[DTCN.THE_YEAR, DTCN.MONTH_THE_YEAR]),
            attr_name=Name(columns=[DTCN.MONTH_THE_YEAR])
        )
        week = Attribute(
            name='Week', level_type='TimeWeeks',
            attr_key=Key(columns=[DTCN.THE_YEAR, DTCN.WEEK_OF_YEAR]),
            attr_name=Name(columns=[DTCN.WEEK_OF_YEAR])
        )
        day = Attribute(
            name='Day', level_type='TimeDays',
            attr_key=Key(columns=[DTCN.TIME_ID]),
            attr_name=Name(columns=[DTCN.DAY_OF_MONTH])
        )
        month_name = Attribute(
            name='Month Name',
            attr_key=Key(columns=[DTCN.THE_YEAR, DTCN.MONTH_THE_YEAR]),
            attr_name=Name(columns=[DTCN.THE_MONTH])
        )
        date = Attribute(
            name='Date', key_column=DTCN.THE_DATE
        )
        time_id = Attribute(
            name='Time Dim Key', key_column=DTCN.TIME_ID
        )
        time_dim.add_attributes([
            year, quarter, month, week, day, month_name, date, time_id])

        time_hierarchy_1 = Hierarchy(name='Time', has_all=False)
        for level_name in ['Year', 'Quarter', 'Month']:
            time_hierarchy_1.add_level(Level(attribute=level_name))
        time_hierarchy_2 = Hierarchy(name='Weekly', has_all=True)
        for level_name in ['Year', 'Week', 'Day']:
            time_hierarchy_2.add_level(Level(attribute=level_name))

        time_dim.add_hierarchies([time_hierarchy_1, time_hierarchy_2])

        cube.add_dimension(time_dim)

        physical_schema.add_table(Table(time_table_name))

        cube.measure_groups.add_measure_group(measure_group)

        measure_group.dimension_links.add_dimension_link(
                ForeignKeyLink(
                    dimension='Dim Table', foreign_key_column='cdc_key'))

        for measure in measures:
            measure_schema = MeasureSchema(
                name=measure.name, column=measure.name, caption=measure.title,
                visible=True if measure.visible else False, aggregator='sum')
            measure_group.measures_tag.add_measures(measure_schema)

        cube.add_dimension(dimension)
        schema.add_physical_schema(physical_schema)
        schema.add_cube(cube)

        xml = generate(schema, output=1)

        resp = requests.post('{0}{1}'.format(
            settings.API_HTTP_HOST, reverse('api:import_schema')),
            data={'key': cube_key, 'data': xml,
                  'user_id': self.context['user_id'], }
        )

        if resp.json()['status'] == 'success':
            cube_id = resp.json()['id']
            logger.info('Created cube %s' % cube_id)
        else:
            self.error_handling(resp.json()['message'])
            logger.error('Error creating cube')
            logger.error(resp.json()['message'])

# write in console: python manage.py celery -A etl.tasks worker
