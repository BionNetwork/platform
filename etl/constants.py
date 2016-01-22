# coding: utf-8

# Разделитель
FIELD_NAME_SEP = '__'

# соответствие типов в редисе и типов для создания таблиц локально
TYPES_MAP = {
    'integer': 'integer',
    'double precision': 'double precision',
    'text': 'text',
    'timestamp': 'timestamp',
    'binary': 'bytea',
}

# Название задач
CREATE_DATASET = 'etl:database:create_dataset'
MONGODB_DATA_LOAD = 'etl:load_data:mongo'
DB_DATA_LOAD = 'etl:cdc:load_data'
MONGODB_DELTA_LOAD = 'etl:cdc:load_delta'
DB_DETECT_REDUNDANT = 'etl:cdc:detect_redundant'
DB_DELETE_REDUNDANT = 'etl:cdc:delete_redundant'
GENERATE_DIMENSIONS = 'etl:database:generate_dimensions'
GENERATE_MEASURES = 'etl:database:generate_measures'
CREATE_TRIGGERS = 'etl.tasks.create_triggers'

# Префиксы названий таблиц
STTM_DATASOURCE = 'sttm_datasource'  # Временная загружаемая таблица
STTM_DATASOURCE_DELTA = 'sttm_datasource_delta'  # таблица для докачки новых данных
STTM_DATASOURCE_KEYS = 'sttm_datasource_keys'  # Текущее состояния пользовательских таблиц
STTM_DATASOURCE_KEYSALL = 'sttm_datasource_keysall'  # Таблица всех ключей
DIMENSIONS = 'dimensions'  # Таблица размерностей
MEASURES = 'measures'  # Таблица мер
