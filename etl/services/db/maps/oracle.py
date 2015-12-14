from collections import defaultdict

ORACLE_TYPES = defaultdict(lambda: 0)

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
    ORACLE_TYPES[i] = 'integer'

for i in floats:
    ORACLE_TYPES[i] = 'double precision'

for i in texts:
    ORACLE_TYPES[i] = 'text'

for i in dates:
    ORACLE_TYPES[i] = 'timestamp'


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