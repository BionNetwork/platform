# coding: utf-8



from collections import defaultdict


def group_by_source(columns_info):
    """
    Группировка по соурсам, на всякий пожарный перед загрузкой
    """
    sid_grouped = defaultdict(dict)

    for sid, tables in columns_info.items():
        sid_grouped[str(sid)].update(tables)

    return dict(sid_grouped)


def extract_tables_info(columns):

    tables_dict = {}

    for sid, tables in columns.items():
        tables_dict[sid] = list(tables.keys())
    return tables_dict
