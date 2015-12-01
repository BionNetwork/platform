# coding: utf-8
from django.conf import settings
from etl.services.db.factory import DatabaseService
from etl.services.db.interfaces import BaseEnum
from etl.services.datasource.repository.storage import RedisSourceService
from core.models import (QueueList, Queue, QueueStatus)
import json
import datetime


class QueueStorage(object):
    """
    Класс работает с информацией работающего таска пользователя,
    обеспечивает контроль над входными данными,
    имеется allowed_keys - список разрешенных ключей для редис словаря
    """
    allowed_keys = [
        'id', 'user_id', 'date_created', 'date_updated', 'status', 'percent']

    def __init__(self, queue_redis_dict):
        """
        :type queue_redis_dict: redis_collections.Dict
        """
        self.queue = queue_redis_dict

    def __getitem__(self, key):
        if key in self.allowed_keys:
            return self.queue[key]
        else:
            raise KeyError('Неверный ключ для словаря информации задачи!')

    def __setitem__(self, key, val):
        if key in self.allowed_keys:
            self.queue[key] = val
        else:
            raise KeyError('Неверный ключ для словаря информации задачи!')


class TaskService:
    """
    Добавление новых задач в очередь
    Управление пользовательскими задачами
    """
    def __init__(self, name):
        self.name = name

    def add_task(self, arguments):
        """
        Добавляем задачу юзеру в список задач и возвращаем идентификатор заадчи
        :type tree: dict дерево источника
        :param user_id: integer
        :param data: dict
        :param source_dict: dict
        :return: integer
        """

        task = QueueList.objects.create(
            queue=Queue.objects.get(name=self.name),
            queue_status=QueueStatus.objects.get(title=TaskStatusEnum.IDLE),
            arguments=json.dumps(arguments),
            app='etl',
            checksum='',
        )

        task_id = task.id
        # канал для таска
        new_channel = settings.SOCKET_CHANNEL.format(arguments['user_id'], task_id)

        # добавляем канал подписки в редис
        channels = RedisSourceService.get_user_subscribers(arguments['user_id'])
        channels.append({
            "channel": new_channel,
            "queue_id": task_id,
            "namespace": self.name,
            "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        return task_id, new_channel

    @staticmethod
    def update_task_status(task_id, status_id, error_code=None, error_msg=None):
        """
            Меняем статусы тасков
        """
        task = QueueList.objects.get(id=task_id)
        task.queue_status = QueueStatus.objects.get(title=status_id)

        if status_id == TaskStatusEnum.ERROR:
            task.comment = 'code: {0}, message: {1}'.format(
                error_code, error_msg)

        task.save()

    @classmethod
    def table_create_query_for_loading_task(
        cls, local_instance, table_key, cols_str):
        """
            Получение запроса на создание новой таблицы
            для локального хранилища данных
        """
        create_query = DatabaseService.get_table_create_query(
            local_instance, table_key, cols_str)
        return create_query

    @classmethod
    def table_insert_query_for_loading_task(cls, local_instance, table_key):
        """
            Получение запроса на заполнение таблицы
            для локального хранилища данных
        """
        insert_query = DatabaseService.get_table_insert_query(
            local_instance, table_key)
        return insert_query


class TaskErrorCodeEnum(BaseEnum):
    """
        Коды ошибок тасков пользователя
    """
    DEFAULT_CODE = '1050'

    values = {
        DEFAULT_CODE: '1050',
    }


class TaskStatusEnum(BaseEnum):
    """
        Статусы тасков
    """
    IDLE, PROCESSING, ERROR, DONE, DELETED = ('idle', 'processing', 'error',
                                              'done', 'deleted', )
    values = {
        IDLE: " В ожидании",
        PROCESSING: "В обработке",
        ERROR: "Ошибка",
        DONE: "Выполнено",
        DELETED: "Удалено",
    }
