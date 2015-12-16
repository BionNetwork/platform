# coding: utf-8

__author__ = 'damir'

from collections import defaultdict


MSSQL_TYPES = defaultdict(lambda: 0)


ints = [
    'int',
    'tinyint',
    'smallint',
    'bigint',
]
floats = [
    'float',
    'decimal',
    'numeric',
    'real',
]
texts = [
    'char',
    'varchar',
    'text',
    'nchar',
    'nvarchar',
    'ntext',
    'binary',
    'varbinary',
    'image',
]
dates = [
    'time',
    'Date',
    'smalldatetime',
    'datetime',
    'datetime2',
    'datetimeoffset',
]

for i in ints:
    MSSQL_TYPES[i] = 'integer'

for i in floats:
    MSSQL_TYPES[i] = 'double precision'

for i in texts:
    MSSQL_TYPES[i] = 'text'

for i in dates:
    MSSQL_TYPES[i] = 'timestamp'

cols_query = """
    SELECT table_name, column_name, data_type FROM information_schema.columns
            where table_name in {0} and table_schema = '{1}' order by table_name;
"""

indexes_query = """

    SELECT
     TableName = t.name,
     IndexName = ind.name,
     IndexId = ind.index_id,
     ColumnId = ic.index_column_id,
     ColumnName = col.name,
     ind.*,
     ic.*,
     col.*
    FROM
         sys.indexes ind
    INNER JOIN
         sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id
    INNER JOIN
         sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
    INNER JOIN
         sys.tables t ON ind.object_id = t.object_id
    WHERE
         ind.is_primary_key = 0
         AND ind.is_unique = 0
         AND ind.is_unique_constraint = 0
         AND t.is_ms_shipped = 0
    ORDER BY
         t.name, ind.name, ind.index_id, ic.index_column_id

"""

constraints_query = """

"""

stat_query = """
    SELECT TABLE_NAME, 1, 1 FROM INFORMATION_SCHEMA.TABLES
    where table_name in {0} and table_schema = '{1}' order by table_name;
"""
