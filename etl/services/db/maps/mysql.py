# coding: utf-8


from collections import defaultdict


DB_TYPES = defaultdict(lambda: 0)


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
    'tinytext',
    'mediumtext',
    'longtext',
    'enum',
]
dates = [
    'date',
    'datetime',
    'timestamp',
    'time',
    'year',
]

blobs = [
    'blob',
    'tinyblob',
    'mediumblob',
    'longblob',
    'varbinary',
    'binary',
]

booleans = [
    'bool',
    'boolean',
]

for i in ints:
    DB_TYPES[i] = 'integer'

for i in floats:
    DB_TYPES[i] = 'double precision'

for i in texts:
    DB_TYPES[i] = 'text'

for i in dates:
    DB_TYPES[i] = 'timestamp'

for i in blobs:
    DB_TYPES[i] = 'binary'

for i in booleans:
    DB_TYPES[i] = 'bool'

table_query = """
    SELECT table_name FROM information_schema.tables
            where table_schema='{0}' order by table_name;
        """

cols_query = """
    SELECT table_name, column_name, column_type, is_nullable,
    case extra when 'auto_increment' then extra else null end
    FROM information_schema.columns
            where table_name in {0} and table_schema = '{1}' order by table_name;
"""

cdc_cols_query = """
    SELECT column_name, column_type FROM information_schema.columns
            where table_name in {0} and table_schema = '{1}' order by table_name;
"""

add_column_query = """
    alter table {0} add {1} {2} {3};
"""

del_column_query = """
    alter table {0} drop column {1};
"""

create_index_query = """
    CREATE INDEX {0} ON {1} ({2});
"""

drop_index_query = """
    drop index {0} on {1};
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

remote_table_query = """
    CREATE TABLE IF NOT EXISTS `{0}` (
        {1}
        `cdc_created_at` timestamp NOT NULL,
        `cdc_updated_at` timestamp,
        `cdc_delta_flag` smallint NOT NULL,
        `cdc_synced` smallint NOT NULL
    );
"""

cdc_required_types = {
    "cdc_created_at": {"type": "timestamp", "nullable": "NOT NULL"},
    "cdc_updated_at": {"type": "timestamp", "nullable": ""},
    "cdc_delta_flag": {"type": "smallint", "nullable": "NOT NULL"},
    "cdc_synced": {"type": "smallint", "nullable": "NOT NULL"},
}

remote_triggers_query = """
    DROP TRIGGER IF EXISTS `cdc_{orig_table}_insert` $$
    CREATE TRIGGER `cdc_{orig_table}_insert` AFTER INSERT ON `{orig_table}`
    FOR EACH ROW BEGIN
    INSERT INTO `{new_table}` ({cols} `cdc_created_at`, `cdc_updated_at`, `cdc_delta_flag`, `cdc_synced`)
    VALUES ({new} now(), null, 1, 0);
    END
    $$

    DROP TRIGGER IF EXISTS `cdc_{orig_table}_update` $$
    CREATE  TRIGGER `cdc_{orig_table}_update` AFTER UPDATE ON `{orig_table}`
    FOR EACH ROW BEGIN
    INSERT INTO `{new_table}` ({cols} `cdc_created_at`, `cdc_updated_at`, `cdc_delta_flag`, `cdc_synced`)
    VALUES ({new} now(), null, 2, 0);
    END
    $$

    DROP TRIGGER IF EXISTS `cdc_{orig_table}_delete` $$
    CREATE  TRIGGER `cdc_{orig_table}_delete` AFTER DELETE ON `{orig_table}`
    FOR EACH ROW BEGIN
    INSERT INTO `{new_table}` ({cols} `cdc_created_at`, `cdc_updated_at`, `cdc_delta_flag`, `cdc_synced`)
    VALUES ({old} now(), null, 3, 0);
    END
"""

row_query = """
    SELECT {0} FROM {1} LIMIT {2} OFFSET {3};
"""


pr_key_query = """
    select a.constraint_name
    from information_schema.TABLE_CONSTRAINTS as a
    inner join information_schema.KEY_COLUMN_USAGE as b on a.CONSTRAINT_NAME=b.CONSTRAINT_NAME
    WHERE a.constraint_type='PRIMARY KEY' and a.TABLE_NAME = b.TABLE_NAME
    and a.TABLE_NAME in {0} and a.TABLE_SCHEMA = '{1}';
"""

delete_primary_key = """
    ALTER TABLE {0} drop primary key
"""

drop_index = """drop index {0} on {1}"""
