# coding: utf-8
from __future__ import unicode_literals
from etl.services.datasource.repository.storage import RedisSourceService
from etl.services.datasource.base import DataSourceService
from etl.services.queue.base import (
    TaskService, TaskStatusEnum, TaskErrorCodeEnum, run_task,
    MONGODB_DATA_LOAD, DB_DATA_LOAD)

__author__ = 'miholeus'
"""
Эти классы используются во view.py, tasks.py для более быстрого доступа к сервисам
Класс играет своего рода container
"""
