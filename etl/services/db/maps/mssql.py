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
booleans = [
    'bit',
]

for i in ints:
    MSSQL_TYPES[i] = 'integer'

for i in floats:
    MSSQL_TYPES[i] = 'double precision'

for i in texts:
    MSSQL_TYPES[i] = 'text'

for i in dates:
    MSSQL_TYPES[i] = 'timestamp'

for i in booleans:
    MSSQL_TYPES[i] = 'bool'

cols_query = """
    SELECT table_name, column_name, data_type FROM information_schema.columns
            where table_name in {0} and table_catalog = '{1}' order by table_name;
"""

indexes_query = """
    SELECT t.name, ind.name,
    CASE ind.is_primary_key WHEN 1 THEN 't' else 'f' END,
    CASE ind.is_unique WHEN 1 THEN 't' else 'f' END,
    col.name
    FROM sys.indexes ind
    INNER JOIN
        sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id
    INNER JOIN
        sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
    INNER JOIN
        sys.tables t ON ind.object_id = t.object_id
    WHERE
        t.name in {0}
"""

constraints_query = """
    select a.TABLE_NAME, b.COLUMN_NAME, a.CONSTRAINT_NAME, a.CONSTRAINT_TYPE,
    fks.REFERENCED_TABLE_NAME, fks.REFERENCED_COLUMN_NAME, r.UPDATE_RULE, r.DELETE_RULE
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS as a
    inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE as b on a.CONSTRAINT_NAME=b.CONSTRAINT_NAME
    left JOIN information_schema.REFERENTIAL_CONSTRAINTS as r ON b.CONSTRAINT_NAME = r.CONSTRAINT_NAME

    left join (
        SELECT
         KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME
        ,KCU1.TABLE_NAME AS FK_TABLE_NAME
        ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME
        ,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME
        ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME
    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC

    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1
        ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
        AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
        AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME

    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2
        ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
        AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
        AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
        AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION
    ) as fks on a.TABLE_NAME=fks.FK_TABLE_NAME and b.COLUMN_NAME=fks.FK_COLUMN_NAME
    and a.CONSTRAINT_NAME=fks.FK_CONSTRAINT_NAME

    WHERE a.TABLE_NAME in {0} and a.TABLE_CATALOG = '{1}';
"""

stat_query = """
    SELECT t.NAME, p.rows, (sum(a.used_pages) * 8)
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    inner join information_schema.tables as tables on t.name=tables.TABLE_NAME
    WHERE t.NAME in {0} and tables.table_catalog = '{1}'
    GROUP BY t.NAME, p.rows
"""

rows_query = """
    WITH Results_CTE AS
    (SELECT {0}, ROW_NUMBER() OVER (ORDER BY {4}) AS RowNum FROM {1})
    SELECT {5} FROM Results_CTE
    WHERE RowNum > {3}
    AND RowNum <= {2}+{3}
"""
