# coding: utf-8

from __future__ import unicode_literals

from collections import defaultdict


def group_by_source(columns_info):
    """
    Группировка по соурсам, на всякий пожарный перед загрузкой
    """
    sid_grouped = defaultdict(dict)

    for sid, tables in columns_info.iteritems():
        sid_grouped[str(sid)].update(tables)

    return dict(sid_grouped)
