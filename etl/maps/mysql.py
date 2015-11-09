# coding: utf-8


from collections import defaultdict


MYSQL_TYPES = defaultdict(lambda: 0)


ints = [
    'int',
    'tinyint',
    'smallint',
    'mediumint',
    'bigint',
]
floats = [
    'float',
    'double',
    'decimal',
]
texts = [
    'char',
    'varchar',
    'text',
    'blob',
    'tinytext',
    'tinyblob',
    'mediumtext',
    'mediumblob',
    'longtext',
    'longblob',
    'enum',
]
dates = [
    'date',
    'datetime',
    'timestamp',
    'time',
    'year',
]

for i in ints:
    MYSQL_TYPES[i] = 'integer'

for i in floats:
    MYSQL_TYPES[i] = 'double precision'

for i in texts:
    MYSQL_TYPES[i] = 'text'

for i in dates:
    MYSQL_TYPES[i] = 'timestamp'

cols_query = """
    SELECT table_name, column_name, column_type FROM information_schema.columns
            where table_name in {0} and table_schema = '{1}' order by table_name;
"""

indexes_query = """
    SELECT
    table_name, group_concat(column_name), index_name,
    CASE index_name WHEN 'PRIMARY' THEN 't' else 'f' END as isprimary,
    CASE non_unique WHEN 0 THEN 't' else 'f' END as isunique

    FROM INFORMATION_SCHEMA.STATISTICS WHERE table_name in {0} and TABLE_SCHEMA = '{1}'

    GROUP BY table_name, index_name;
"""

constraints_query = """
    select b.TABLE_NAME, b.column_name, a.constraint_name, a.constraint_type,
    b.referenced_table_name, b.REFERENCED_COLUMN_NAME, r.update_rule, r.delete_rule
    from information_schema.TABLE_CONSTRAINTS as a
    inner join information_schema.KEY_COLUMN_USAGE as b on a.CONSTRAINT_NAME=b.CONSTRAINT_NAME
    left JOIN information_schema.REFERENTIAL_CONSTRAINTS as r ON b.CONSTRAINT_NAME = r.CONSTRAINT_NAME
    WHERE a.TABLE_NAME = b.TABLE_NAME and a.TABLE_NAME in {0} and a.TABLE_SCHEMA = '{1}';
"""

stat_query = """
    SELECT TABLE_NAME, TABLE_ROWS as count, DATA_LENGTH as size FROM INFORMATION_SCHEMA.TABLES
    where table_name in {0} and table_schema = '{1}' order by table_name;
"""