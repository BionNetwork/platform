# coding: utf-8
from celery import group
from django.conf import settings
from etl.services.db.factory import DatabaseService
from etl.services.db.interfaces import BaseEnum
from etl.services.datasource.repository.storage import RedisSourceService
from core.models import (QueueList, Queue, QueueStatus)
import json
import datetime
from etl.services.middleware.base import datetime_now_str


class QueueStorage(object):
    """
    Класс работает с информацией работающего таска пользователя,
    обеспечивает контроль над входными данными,
    имеется allowed_keys - список разрешенных ключей для редис словаря
    """
    allowed_keys = [
        'id', 'user_id', 'date_created', 'date_updated', 'status', 'percent']

    def __init__(self, queue_redis_dict, task_id, user_id):
        """
        :type queue_redis_dict: redis_collections.Dict
        """
        self.queue = queue_redis_dict
        self.task_id = task_id
        self.user_id = user_id
        self.set_init_params()

    def __getitem__(self, key):
        if key not in self.allowed_keys:
            raise KeyError('Неверный ключ для словаря информации задачи!')
        return self.queue[key]

    def __setitem__(self, key, val):
        if key not in self.allowed_keys:
            raise KeyError('Неверный ключ для словаря информации задачи!')

    def set_init_params(self):
        """
        Загрузка инициазицонных данных
        """
        self.queue['task_id'] = self.task_id
        self.queue['user_id'] = self.user_id
        self.queue['date_created'] = datetime_now_str()
        self.queue['date_updated'] = None
        self.queue['status'] = TaskStatusEnum.PROCESSING
        self.queue['percent'] = 0

    def update(self, status=None):
        self.queue['date_created'] = datetime_now_str()
        if status:
            self.queue['status'] = status


def run_task(task_params):
    """
    Args:
        task_params(tuple or list):
    """
    if type(task_params) == tuple:
        task_id, channel = TaskService(task_params[0]).add_task(
            arguments=task_params[2])
        task_params[1](task_id, channel).load_data()
        return [channel]
    else:
        group_tasks = []
        channels = []
        for each in task_params:
            task_id, channel = TaskService(each[0]).add_task(
                arguments=each[2])
            group_tasks.append(each[1](task_id, channel).load_data())
            channels.append(channel)
        # group(group_tasks)
        return channels


class TaskService(object):
    """
    Добавление новых задач в очередь
    Управление пользовательскими задачами
    """
    def __init__(self, name):
        """
        Args:
            name(str): Имя задачи
        """
        self.name = name
        self.task_id = None

    def add_task(self, arguments):
        """
        Добавляем задачу юзеру в список задач и возвращаем идентификатор заадчи

        Args:
            arguments(dict): Необходимые для выполнения задачи данные
            table_key(str): ключ

        Returns:
            task_id(int): id задачи
            new_channel(str): Название канала для сокетов
        """

        task = QueueList.objects.create(
            queue=Queue.objects.get(name=self.name),
            queue_status=QueueStatus.objects.get(title=TaskStatusEnum.IDLE),
            arguments=json.dumps(arguments),
            app='etl',
            checksum=arguments.get('checksum', ''),
        )

        task_id = task.id
        # канал для задач
        new_channel = settings.SOCKET_CHANNEL.format(
            arguments['user_id'], task_id)

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
    def get_queue(task_id, user_id):
        """
        Информация о ходе работы задач
        :param task_id:
        """
        queue_dict = RedisSourceService.get_queue_dict(task_id)
        return QueueStorage(queue_dict, task_id, user_id)

    @staticmethod
    def update_task_status(task_id, status_id, error_code=None, error_msg=None):
        """
        Меняем статусы задач
        """
        task = QueueList.objects.get(id=task_id)
        task.queue_status = QueueStatus.objects.get(title=status_id)

        if status_id == TaskStatusEnum.ERROR:
            task.comment = 'code: {0}, message: {1}'.format(
                error_code, error_msg)

        task.save()


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


class TaskLoadingStatusEnum(BaseEnum):
    """
    Состояние загрузки задачи
    """
    START, PROCESSING, FINISH, ERROR = ('start', 'processing', 'finish', 'error')
    values = {
        START: "Старт",
        PROCESSING: "В обработке",
        FINISH: "Выполнено",
        ERROR: "Ошибка",
    }

TLSE = TaskLoadingStatusEnum


class SourceTableStatusEnum(BaseEnum):
    """
    Статус состояния таблице-источнике
    """

    IDLE, LOADED = ('idle', 'loaded')

    values = {
        IDLE: "Выполнено",
        LOADED: "Загружено"
    }

STSE = SourceTableStatusEnum


class DeltaTableStatusEnum(BaseEnum):
    """
    Статусы состояния в дельта-таблице
    """

    NEW, SYNCED = ('new', 'synced')

    values = {
        NEW: "Новое",
        SYNCED: "Синхронизировано",
    }

DTSE = DeltaTableStatusEnum


class DeleteTableStatusEnum(BaseEnum):
    """
    Статусы состояния в дельта-таблице
    """

    NEW, DELETED = ('new', 'deleted')

    values = {
        NEW: "Новое",
        DELETED: "Удалено",
    }

DelTSE = DeleteTableStatusEnum

