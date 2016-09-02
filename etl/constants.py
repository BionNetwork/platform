# coding: utf-8

from __future__ import unicode_literals

# Разделитель
FIELD_NAME_SEP = '__'

# соответствие типов в редисе и типов для создания таблиц локально
TYPES_MAP = {
    'integer': 'integer',
    'double precision': 'double precision',
    'text': 'text',
    'timestamp': 'timestamp',
    'binary': 'bytea',
    'bool': 'boolean',
}

# Название задач
MONGODB_DATA_LOAD = 'etl:load_data:mongo'
DB_DATA_LOAD = 'etl:cdc:load_data'
MONGODB_DELTA_LOAD = 'etl:cdc:load_delta'
DB_DETECT_REDUNDANT = 'etl:cdc:detect_redundant'
DB_DELETE_REDUNDANT = 'etl:cdc:delete_redundant'
GENERATE_DIMENSIONS = 'etl:database:generate_dimensions'
GENERATE_MEASURES = 'etl:database:generate_measures'
CREATE_TRIGGERS = 'etl:tasks:create_triggers'
CREATE_CUBE = 'etl:database:generate_cube'

# Префиксы названий таблиц
STTM_DATASOURCE = 'sttm_datasource'  # Временная загружаемая таблица
STTM_DATASOURCE_DELTA = 'sttm_datasource_delta'  # таблица для докачки
STTM_DATASOURCE_KEYS = 'sttm_datasource_keys'  # Текущее состояния польз. таблиц
STTM_DATASOURCE_KEYSALL = 'sttm_datasource_keysall'  # Таблица всех ключей
DIMENSIONS = 'dimensions'  # Таблица размерностей
MEASURES = 'measures'  # Таблица мер
TIME_TABLE = 'time_by_day'  # Таблица дат

# Строки формирования названия колонок
STANDART_COLUMN_NAME = '{0}__{1}'
MONGODB_DB_NAME = 'etl'  # база данных в монго

# название локального триггера для мер, либо для размерностей
LOCAL_TRIGGER_NAME = "for_{0}_{1}"

# название удаленной таблицы триггера
REMOTE_TRIGGER_TABLE_NAME = "_etl_datasource_cdc_{0}"

# количество колонок в таблице дат
DATE_TABLE_COLS_LEN = 9


# Название задач MULTI

CREATE_DATASET = 'etl:database:create_dataset'
MONGODB_DATA_LOAD_MONO = 'etl:load_data:mongo_mono'
MONGODB_DATA_LOAD_MULTI = 'etl:load_data:mongo_multi'
MONGODB_DATA_LOAD_CALLBACK = 'etl:load_data:mongo_callback'
PSQL_FOREIGN_TABLE = 'etl:load_data:psql_foreign_table'
PSQL_VIEW = 'etl:load_data:psql_view'
WAREHOUSE_LOAD = 'etl:load_data:clickhouse_load'
CREATE_DATE_TABLE = 'etl:load_data:create_date_table'
META_INFO_SAVE = 'etl:database:save_meta'


# Префиксы названий таблиц
STTM = 'sttm_'  # Временная загружаемая таблица Mongo
FDW = 'fdw_'
VIEW = 'view_'

MONGODB = 'mongodb'

RDBMS_SERVER = 'rdbms_server'
CSV_SERVER = 'csv_srv'

# Префиксы названий MATERIALIZED VIEW для мер/размерностей
MEASURES_MV = 'mv_measures_{0}'
DIMENSIONS_MV = 'mv_dimensions_{0}'

# Название таблицы в ClickHouse
CLICK_TABLE = "t_{0}"
# Название колонки в ClickHouse
# (буква вначале+cube_id+source_id+хэш таблицы+хэш колонки)
CLICK_COLUMN = "c_{0}"
