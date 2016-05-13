# coding: utf-8
from __future__ import unicode_literals
# from etl.services.datasource.repository.storage import RedisSourceService
# from etl.services.datasource.base import DataSourceService
# from etl.services.queue.base import (
#     TaskService, TaskStatusEnum, TaskErrorCodeEnum)
#
# __author__ = 'miholeus'
# """
# Эти классы используются во view.py, tasks.py для более быстрого доступа к сервисам
# Класс играет своего рода container
# """


def split_file_sub_tree(sub_tree):
    """
    """
    childs = sub_tree['childs']
    sub_tree['childs'] = []
    items = [sub_tree, ]

    while childs:
        new_childs = []
        for child in childs:
            items.append({'val': child['val'], 'childs': [], })
            new_childs.extend(child['childs'])
        childs = new_childs

    return items
