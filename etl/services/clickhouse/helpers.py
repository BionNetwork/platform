# coding: utf-8
from __future__ import unicode_literals, division

__author__ = 'damir(GDR)'

MIN_MAX_QUERY = """SELECT toDate(MIN({column_name})), toDate(MAX({column_name}))
                   FROM {table_name} FORMAT JSON;"""

DISTINCT_QUERY = """SELECT DISTINCT {column_name}
                    FROM {table_name} FORMAT JSON;"""

FILTER_QUERIES = {
    'timestamp': MIN_MAX_QUERY,
    'text': DISTINCT_QUERY,
    'bool': DISTINCT_QUERY,
}
