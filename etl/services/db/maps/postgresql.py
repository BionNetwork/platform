# coding: utf-8


from collections import defaultdict


PSQL_TYPES = defaultdict(lambda: 0)


ints = [
    'smallint',
    'integer',
    'bigint',
    'smallserial',
    'serial',
    'bigserial',
]

floats = [
    'decimal',
    'numeric',
    'real',
    'double precision',
]

texts = [
    'character varying',
    'character',
    'char',
    'text',
]
dates = [
    'timestamp without time zone',
    'timestamp with time zone',
    'timestamp',
    'date',
    'time without time zone',
    'time with time zone',
    'interval',
]

for i in ints:
    PSQL_TYPES[i] = 'integer'

for i in floats:
    PSQL_TYPES[i] = 'double precision'

for i in texts:
    PSQL_TYPES[i] = 'text'

for i in dates:
    PSQL_TYPES[i] = 'timestamp'

cols_query = """
    SELECT table_name, column_name, data_type FROM information_schema.columns
            where table_name in {0} and table_catalog = '{1}' and table_schema = '{2}' order by table_name;
"""

constraints_query = """
    SELECT
        t.relname AS table_name,
        ic.column_name,
        c.conname AS constraint_name,
        CASE c.contype
          WHEN 'c' THEN 'CHECK'
          WHEN 'f' THEN 'FOREIGN KEY'
          WHEN 'p' THEN 'PRIMARY KEY'
          WHEN 'u' THEN 'UNIQUE'
        END AS "constraint_type",
        t2.relname AS references_table,
        ic2.column_name  AS references_column,
        CASE confupdtype
          WHEN 'a' THEN 'NO ACTION'
          WHEN 'r' THEN 'RESTRICT'
          WHEN 'c' THEN 'CASCADE'
          WHEN 'n' THEN 'SET NULL'
          WHEN 'd' THEN 'SET DEFAULT'
        END AS on_update,
        CASE confdeltype
          WHEN 'a' THEN 'NO ACTION'
          WHEN 'r' THEN 'RESTRICT'
          WHEN 'c' THEN 'CASCADE'
          WHEN 'n' THEN 'SET NULL'
          WHEN 'd' THEN 'SET DEFAULT'
        END AS on_delete
    FROM pg_constraint c
        LEFT JOIN pg_class t  ON c.conrelid  = t.oid
        LEFT JOIN pg_class t2 ON c.confrelid = t2.oid
        LEFT JOIN information_schema.columns as ic on
        ic.table_name = t.relname and
        array_to_string(c.conkey, ' ') = cast(ic.ordinal_position as text)
        LEFT JOIN information_schema.columns as ic2 on
        ic2.table_name = t2.relname and
        array_to_string(c.confkey, ' ') = cast(ic2.ordinal_position as text)
            WHERE t.relname in {0} order by t.relname
    """

indexes_query = """
        SELECT t.relname, string_agg(a.attname, ','), i.relname, ix.indisprimary,
        ix.indisunique FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid
        WHERE a.attnum = ANY(ix.indkey) AND  t.relkind = 'r' AND  t.relname in {0}
        group by t.relname, i.relname, ix.indisprimary, ix.indisunique order by t.relname
    """

stat_query = """
    SELECT relname, reltuples as count, relpages*8192 as size FROM pg_class
    where oid in {0};
"""
