# coding: utf-8


__author__ = 'damir(GDR)'

# 'FORMAT JSON' для сlickhouse
MIN_MAX_QUERY = """SELECT MIN({column_name}), MAX({column_name}) FROM {table_name};"""

# 'FORMAT JSON' для сlickhouse
DISTINCT_QUERY = """SELECT DISTINCT {column_name} FROM {table_name};"""

FILTER_QUERIES = {
    'timestamp': MIN_MAX_QUERY,
    'text': DISTINCT_QUERY,
    'bool': DISTINCT_QUERY,
}
