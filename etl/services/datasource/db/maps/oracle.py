from collections import defaultdict

DB_TYPES = defaultdict(lambda: 0)

ints = [
    'integer',
    'shortinteger',
    'longinteger',
]

floats = [
    'number',
    'decimal',
    'shortdecimal',
    'binary_float',
    'binary_decimal',
]

texts = [
    'char',
    'varchar',
    'varchar2',
    'nchar',
    'nvarchar2',
    'clob',
    'nclob',
    'long',
]

dates = [
    'date',
    'timestamp',
]

blobs = [
    'blob',
    'binary',
    'varbinary',
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

table_query = """SELECT table_name FROM user_tables"""

cols_query = """SELECT table_name, column_name, data_type,
                    CASE WHEN nullable='N' THEN 'NO'
                    ELSE 'YES' END as is_nullable,
                    null as extra, CHAR_COL_DECL_LENGTH
 FROM user_tab_columns WHERE table_name IN {0}"""

cdc_cols_query = """
    SELECT column_name, data_type
        FROM user_tab_columns WHERE table_name IN {0}
"""

add_column_query = """
    alter table "{0}" add {1} {2}{3} {4}
"""

del_column_query = """
    alter table "{0}" drop column "{1}"
"""

create_index_query = """
    CREATE INDEX "{0}" ON "{1}" ({2})
"""

drop_index_query = """
    drop index "{0}"
"""

constraints_query = """
    SELECT ucc1.table_name, ucc1.column_name, uc.constraint_name, uc.constraint_type,
    ucc2.table_name, ucc2.column_name, '', uc.delete_rule
    FROM user_constraints uc, user_cons_columns ucc1, user_cons_columns ucc2
    WHERE ucc1.table_name in {0}
    AND uc.constraint_name = ucc1.constraint_name
    AND uc.r_constraint_name = ucc2.constraint_name
    AND ucc1.POSITION = ucc2.POSITION
    ORDER BY ucc1.TABLE_NAME, uc.constraint_name
    """


indexes_query = """
    SELECT i.table_name, c.column_name, i.index_name, i.uniqueness
    FROM all_ind_columns c, all_indexes i
    WHERE i.table_name IN {0} and i.index_name = c.index_name
"""

row_query = """
    SELECT {0} FROM (SELECT {1}, ROW_NUMBER() OVER (ORDER BY ROWNUM) AS rn
    FROM {2}) WHERE rn BETWEEN {4} AND {4}+{3}
"""

stat_query = """
    SELECT ut.table_name, ut.num_rows, s.t_size
      FROM user_tables ut
    JOIN (
    SELECT segment_name, segment_type, bytes t_size
      FROM dba_segments
        WHERE segment_type='TABLE' AND segment_name in {0}
    ) s
    ON ut.table_name = s.segment_name
"""

remote_table_query = """
    DECLARE cnt NUMBER;
    begin
    SELECT count(*) INTO cnt FROM user_tables WHERE table_name = '{0}';
    IF cnt = 0 THEN EXECUTE IMMEDIATE
    'CREATE TABLE "{0}" (
        {1}
        "CDC_CREATED_AT" TIMESTAMP NOT NULL,
        "CDC_UPDATED_AT" TIMESTAMP,
        "CDC_DELTA_FLAG" NUMBER NOT NULL,
        "CDC_SYNCED" NUMBER NOT NULL)';
    END IF;END;
"""

cdc_required_types = {
    "CDC_CREATED_AT": {"type": "TIMESTAMP", "nullable": "NOT NULL"},
    "CDC_UPDATED_AT": {"type": "TIMESTAMP", "nullable": ""},
    "CDC_DELTA_FLAG": {"type": "NUMBER", "nullable": "NOT NULL"},
    "CDC_SYNCED": {"type": "NUMBER", "nullable": "NOT NULL"},
}

remote_triggers_query = """
    CREATE OR REPLACE TRIGGER "cdc_{orig_table}_insert"
    AFTER INSERT ON "{orig_table}"
    FOR EACH ROW BEGIN
    INSERT INTO "{new_table}"
    ({cols} "CDC_CREATED_AT", "CDC_UPDATED_AT", "CDC_DELTA_FLAG", "CDC_SYNCED")
    VALUES ({new} (select current_timestamp from dual), null, 1, 0); END; $$

    CREATE OR REPLACE TRIGGER "cdc_{orig_table}_update"
    AFTER UPDATE ON "{orig_table}"
    FOR EACH ROW BEGIN
    INSERT INTO "{new_table}"
    ({cols} "CDC_CREATED_AT", "CDC_UPDATED_AT", "CDC_DELTA_FLAG", "CDC_SYNCED")
    VALUES ({new} (select current_timestamp from dual), null, 2, 0); END; $$

    CREATE OR REPLACE TRIGGER "cdc_{orig_table}_delete"
    AFTER DELETE ON "{orig_table}"
    FOR EACH ROW BEGIN
    INSERT INTO "{new_table}"
    ({cols} "CDC_CREATED_AT", "CDC_UPDATED_AT", "CDC_DELTA_FLAG", "CDC_SYNCED")
    VALUES ({old} (select current_timestamp from dual), null, 3, 0); END;
"""

pr_key_query = """
    SELECT c.CONSTRAINT_NAME FROM user_constraints c
    where c.TABLE_NAME in {0} and c.CONSTRAINT_TYPE='P'
"""

delete_primary_key = """
    ALTER TABLE "{0}" DROP CONSTRAINT {1}
"""
