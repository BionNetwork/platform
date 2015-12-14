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