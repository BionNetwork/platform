# coding: utf-8


from collections import defaultdict


DB_TYPES = defaultdict(lambda: 0)


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

blobs = [
    'bytea',
]

booleans = [
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
            where table_schema='public' order by table_name;
        """

cols_query = """
    SELECT table_name, column_name, data_type as column_type, is_nullable,
    case when substring(column_default, 0, 8) = 'nextval'
         then 'serial' else null end as extra
    FROM information_schema.columns
    where table_name in {0} and table_catalog = '{1}' and
          table_schema = '{2}' order by table_name;
"""

cdc_cols_query = """
    SELECT column_name, data_type FROM information_schema.columns
    where table_name in {0} and table_catalog = '{1}' and
          table_schema = '{2}' order by table_name;
"""

add_column_query = """
    alter table {0} add column {1} {2} {3};
"""

del_column_query = """
    alter table {0} drop column {1};
"""

create_index_query = """
    CREATE INDEX {0} ON {1} ({2});
"""

drop_index_query = """
    drop index {0};
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
    where relname in {0};
"""


remote_table_query = """
    CREATE TABLE IF NOT EXISTS "{0}" (
        {1}
        "cdc_created_at" timestamp NOT NULL,
        "cdc_updated_at" timestamp,
        "cdc_delta_flag" smallint NOT NULL,
        "cdc_synced" smallint NOT NULL
    );
"""

cdc_required_types = {
    "cdc_created_at": {"type": "timestamp", "nullable": "NOT NULL"},
    "cdc_updated_at": {"type": "timestamp", "nullable": ""},
    "cdc_delta_flag": {"type": "smallint", "nullable": "NOT NULL"},
    "cdc_synced": {"type": "smallint", "nullable": "NOT NULL"},
}


remote_triggers_query = """
    CREATE OR REPLACE FUNCTION process_{new_table}_audit() RETURNS TRIGGER AS $cdc_audit$
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO "{new_table}" ({cols} "cdc_created_at", "cdc_updated_at", "cdc_delta_flag", "cdc_synced")
            SELECT {old} now(), null, 3, 0;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO "{new_table}" ({cols} "cdc_created_at", "cdc_updated_at", "cdc_delta_flag", "cdc_synced")
            SELECT {new} now(), null, 2, 0;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO "{new_table}" ({cols} "cdc_created_at", "cdc_updated_at", "cdc_delta_flag", "cdc_synced")
            SELECT {new} now(), null, 1, 0;
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$cdc_audit$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS "{new_table}_audit" on "{orig_table}";

CREATE TRIGGER "{new_table}_audit"
AFTER INSERT OR UPDATE OR DELETE ON "{orig_table}"
    FOR EACH ROW EXECUTE PROCEDURE process_{new_table}_audit();
"""

row_query = """
        SELECT {0} FROM {1} LIMIT {2} OFFSET {3};
"""

pr_key_query = """
    SELECT c.conname AS constraint_name FROM pg_constraint c
        LEFT JOIN pg_class t  ON c.conrelid  = t.oid
            WHERE t.relname in {0} and c.contype = 'p' order by t.relname
"""


delete_primary_key = """
    alter table {0} drop constraint {1}
"""

drop_index = """drop index {0}"""

check_table_exists = """
    SELECT EXISTS (
        SELECT * FROM   information_schema.tables
        WHERE table_name = '{0}' AND table_catalog = '{1}'
   );
"""


dimension_measure_triggers_query = """
    CREATE OR REPLACE FUNCTION reload_{new_table}_records() RETURNS TRIGGER AS $dim_meas_recs$
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            DELETE FROM "{new_table}" WHERE {del_condition};
            RETURN OLD;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO "{new_table}" {cols} SELECT {insert_cols};
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$dim_meas_recs$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS "for_{new_table}" on "{orig_table}";

CREATE TRIGGER "for_{new_table}"
AFTER INSERT OR DELETE ON "{orig_table}"
    FOR EACH ROW EXECUTE PROCEDURE reload_{new_table}_records();
"""
