from collections import defaultdict

DB_TYPES = defaultdict(lambda: 0)

ints = [
    'number',
]
floats = [

]
texts = [
    'char',
    'varchar',
    'varchar2',
    'nchar',
    'nvarchar2',
]
dates = [

]

for i in ints:
    DB_TYPES[i] = 'integer'

for i in floats:
    DB_TYPES[i] = 'double precision'

for i in texts:
    DB_TYPES[i] = 'text'

for i in dates:
    DB_TYPES[i] = 'timestamp'

table_query = """SELECT table_name FROM user_tables"""

columns_query = """SELECT table_name, column_name, data_type,
                    CASE WHEN nullable='N' THEN 'NO'
                    ELSE 'YES' END as is_nullable,
                    null as extra
 FROM user_tab_columns WHERE table_name IN {0}"""


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
    SELECT {0} FROM (
    SELECT {1}, ROW_NUMBER() OVER (ORDER BY ROWNUM) AS rn FROM {2})
    WHERE rn BETWEEN {3} AND {4}
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