# coding: utf-8
import binascii
from celery import group
from pymongo import IndexModel
from psycopg2 import Binary
import pymongo
from pymongo import IndexModel
import logging
import brukva
import json
import datetime
from itertools import izip
from bson import binary, Binary

from django.conf import settings
from etl.constants import TYPES_MAP
from etl.services.datasource.base import DataSourceService
from etl.services.db.interfaces import BaseEnum
from etl.services.datasource.repository.storage import RedisSourceService
from core.models import (QueueList, Queue, QueueStatus)
from etl.services.middleware.base import datetime_now_str, get_table_name
from core.exceptions import TaskError
from core.helpers import HashEncoder
import json
import datetime
from itertools import izip
from bson import binary

from etl.services.middleware.base import datetime_now_str
from . import client, settings

__all__ = [
    'TLSE',  'STSE', 'RPublish', 'RowKeysCreator',
    'calc_key_for_row', 'TableCreateQuery', 'InsertQuery', 'MongodbConnection',
    'DeleteQuery', 'AKTSE', 'DTSE', 'get_single_task', 'get_binary_types_list',
    'process_binary_data', 'get_binary_types_dict', 'WhetherTableExistsQuery'
]


client = brukva.Client(host=settings.REDIS_HOST,
                       port=int(settings.REDIS_PORT),
                       selected_db=settings.REDIS_DB)
client.connect()

logger = logging.getLogger(__name__)


class TaskProcessing(object):
    """
    Базовый класс, отвечающий за про процесс выполнения celery-задач

    Attributes:
        task_id(int): id задачи
        channel(str): Канал передачи на клиент
        last_task(bool): Флаг последней задачи
        user_id(str): id пользователя
        context(dict): контекстные данные для задачи
        was_error(bool): Факт наличия ошибки
        err_msg(unicode): Текст ошибки, если случилась
        publisher(`RPublish`): Посыльный к клиенту о текущем статусе задачи
        queue_storage(`etl.services.queue.base.QueueStorage`):
            Посыльный к redis о текущем статусе задачи
        key(str): Ключ
        next_task_params(tuple): Набор данных для след. задачи
    """

    def __init__(self, task_id, channel, last_task=False):
        """
        Args:
            task_id(int): id задачи
            channel(str): Канал передачи на клиент
        """
        self.task_id = task_id
        self.channel = channel
        self.last_task = last_task
        self.user_id = None
        self.context = None
        self.was_error = False
        self.err_msg = ''
        self.publisher = RPublish(self.channel, self.task_id)
        self.queue_storage = None
        self.key = None
        self.next_task_params = None

    def gtm(self, prefix):
        """
        Формирование название таблицы из префикса и ключа
        Args:
            prefix(unicode): Префикс

        Returns:
            unicode: Название таблицы
        """
        return get_table_name(prefix, self.key)

    @staticmethod
    def binary_wrap(data, rules):
        for each in data:
            for k, v in each.iteritems():
                if rules.get(k):  # if binary data
                    each[k] = Binary(v)
        return data

    def prepare(self):
        """
        Подготовка к задаче:
            1. Получение контекста выполнения
            2. Инициализация служб, следящие за процессов выполения
        """
        task = QueueList.objects.get(id=self.task_id)
        self.context = json.loads(task.arguments)
        self.key = task.checksum
        self.user_id = self.context['user_id']
        self.queue_storage = TaskService.get_queue(self.task_id, self.user_id)
        TaskService.update_task_status(self.task_id, TaskStatusEnum.PROCESSING)

    def load_data(self):
        """
        Точка входа
        """
        self.prepare()
        try:
            self.processing()
        except Exception as e:
            # В любой непонятной ситуации меняй статус задачи на ERROR
            TaskService.update_task_status(
                self.task_id, TaskStatusEnum.ERROR,
                error_code=TaskErrorCodeEnum.DEFAULT_CODE,
                error_msg=e.message)
            self.publisher.publish(TLSE.ERROR, msg=e.message)
            RedisSourceService.delete_queue(self.task_id)
            RedisSourceService.delete_user_subscriber(
                self.user_id, self.task_id)
            logger.exception(self.err_msg)
            raise
        self.exit()
        if self.next_task_params:
            get_single_task(*self.next_task_params)

    def processing(self):
        """
        Непосредственное выполнение задачи
        """
        raise NotImplementedError

    def error_handling(self, err_msg, err_code=None):
        """
        Обработка ошибки

        Args:
            err_msg(unicode): Текст ошибки
            err_code(unicode): Код ошибки
        """
        self.was_error = True
        # fixme перезаписывается при каждой ошибке
        self.err_msg = err_msg
        TaskService.update_task_status(
            self.task_id, TaskStatusEnum.ERROR,
            error_code=err_code or TaskErrorCodeEnum.DEFAULT_CODE,
            error_msg=self.err_msg)

        self.queue_storage['status'] = TaskStatusEnum.ERROR

        # сообщаем об ошибке
        self.publisher.publish(TLSE.ERROR, self.err_msg)
        logger.exception(self.err_msg)

    def exit(self):
        """
        Корректное завершение вспомогательных служб
        """
        if self.was_error:
            # меняем статус задачи на 'Ошибка'
            TaskService.update_task_status(
                self.task_id, TaskStatusEnum.ERROR,
                error_code=TaskErrorCodeEnum.DEFAULT_CODE,
                error_msg=self.err_msg)
            if not self.publisher.is_complete:
                self.publisher.publish(TLSE.FINISH)

        else:
            # меняем статус задачи на 'Выполнено'
            TaskService.update_task_status(self.task_id, TaskStatusEnum.DONE, )
            self.queue_storage.update(TaskStatusEnum.DONE)

        # удаляем инфу о работе таска
        RedisSourceService.delete_queue(self.task_id)
        # удаляем канал из списка каналов юзера
        RedisSourceService.delete_user_subscriber(self.user_id, self.task_id)


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
        self.queue['date_updated'] = datetime_now_str()
        if status:
            self.queue['status'] = status


class RPublish(object):
    """
    Запись в Редис состоянии загрузки

    Attributes:
        channel(str): Канал передачи на клиент
        task_id(int): id задачи
        rows_count(int): Приблизительно число обрабатываемых строк
        is_complete(bool): Флаг завершения задачи
        loaded_count(float): Число загруженных данных
    """

    def __init__(self, channel, task_id, is_complete=False):
        """
        Args:
            channel(str): Канал передачи на клиент
            task_id(int): id задачи
            is_complete(bool): Флаг завершения задачи
        """
        self.channel = channel
        self.task_id = task_id
        self.rows_count = None
        self.is_complete = is_complete
        self.loaded_count = 0.0

    @property
    def percent(self):
        return (int(round(self.loaded_count/self.rows_count*100))
                if self.rows_count else 0)

    def publish(self, status, msg=None):
        """
        Публиция состояния на клиент

        Args:
            status(str): Статус задачи
            msg(unicode): Дополнительное сообщение (при ошибке)
        """
        percent = self.percent
        if status == TLSE.FINISH or percent >= 100:
            client.publish(self.channel, json.dumps(
                {'percent': 100,
                 'taskId': self.task_id,
                 'event': TLSE.FINISH}
            ))
            self.is_complete = True
        elif status == TLSE.START:
            client.publish(self.channel, json.dumps(
                {'percent': 0,
                 'taskId': self.task_id,
                 'event': TLSE.START}
            ))
        elif status == TLSE.PROCESSING:
            client.publish(self.channel, json.dumps(
                {'percent': percent,
                 'taskId': self.task_id,
                 'event': TLSE.PROCESSING}
            ))
        else:
            client.publish(self.channel, json.dumps(
                {'percent': percent,
                 'taskId': self.task_id,
                 'event': TLSE.ERROR,
                 'msg': msg}
            ))


def get_single_task(task_name, task_def, params):
    """
    Args:
        task_name(str): Название задачи
        task_def(func): Исполняемая функция
        params(dict): Контекст выполнения

    Returns:
        `Signature`: Celery-задача к выполнению
        list: Список каналов для сокетов
    """
    if not task_name:
        return
    task_id, channel = TaskService(task_name).add_task(
        arguments=params)
    # return task_def.apply_async((task_id, channel),), [channel]
    return task_def(task_id, channel), [channel]


class RowKeysCreator(object):
    """
    Расчет ключа для таблицы
    """

    def __init__(self, table, cols, meta_data=None, primary_keys=None):
        """
        Args:
            table(str): Название таблицы
            cols(list): Список словарей с названиями колонок и соотв. таблиц
            primary_keys(list): Список уникальных ключей
        """
        self.table = table
        self.cols = cols
        self.primary_keys = primary_keys
        # primary_keys_indexes(list): Порядковые номера первичных ключей
        self.primary_keys_indexes = list()
        if meta_data:
            self.set_primary_key(meta_data)

    def calc_key(self, row, row_num):
        """
        Расчет ключа для строки таблицы либо по первичному ключу
        либо по номеру и значениям этой строки

        Args:
            row(tuple): Строка данных
            row_num(int): Номер строки

        Returns:
            int: Ключ строки для таблицы
        """
        l = [y for (x, y) in zip(self.cols, row) if x['table'] == self.table]
        if self.primary_keys:
            l.append(''.join(
                [str(row[index]) for index in self.primary_keys_indexes]))
        else:
            l.append(row_num)
        return HashEncoder.encode(
                reduce(lambda res, x: '%s%s' % (res, x), l).encode("utf8"))

    def set_primary_key(self, data):
        """
        Установка первичного ключа, если есть

        Args:
            data(dict): метаданные по колонкам таблицы
        """

        for record in data['indexes']:
            if record['is_primary']:
                self.primary_keys = record['columns']
                for ind, value in enumerate(self.cols):
                    if value['table'] == self.table and value['col'] in self.primary_keys:
                        self.primary_keys_indexes.append(ind)
                break


def process_binary_data(record, binary_types_list):
    """
    Если данные бинарные, то оборачиваем в bson.binary.Binary
    """
    new_record = list()

    for (rec, is_binary) in izip(record, binary_types_list):
        new_record.append(binary.Binary(
            rec) if is_binary and rec is not None else rec)

    new_record = tuple(new_record)
    return new_record


def process_binaries_for_row(row, binary_types_list):
    """
    Если пришли бинарные данные,
    то вычисляем ключ отдельный для каждого из них
    """
    new_row = []
    for i, r in enumerate(row):
        if binary_types_list[i] and r is not None:
            r = binascii.b2a_base64(r)
        new_row.append(r)
    return tuple(new_row)


def calc_key_for_row(row, tables_key_creators, row_num, binary_types_list):
    """
    Расчет ключа для отдельно взятой строки

    Args:
        row(tuple): строка данных
        tables_key_creators(list of RowKeysCreator()): список экземпляров
        класса, создающие ключи для строки конкретной таблицы
        row_num(int): Номер строки

    Returns:
        int: Ключ для строки
    """
    # преобразуем бинары в строку, если они есть
    row = process_binaries_for_row(row, binary_types_list)

    if len(tables_key_creators) > 1:
        row_values_for_calc = [
            str(each.calc_key(row, row_num)) for each in tables_key_creators]
        return HashEncoder.encode(''.join(row_values_for_calc))
    else:
        return tables_key_creators[0].calc_key(row, row_num)


def get_binary_types_list(cols, col_types):
    """
    Информация о бинарниках для генерации ключа
    Args:
        cols(list): Данные о колонках
        col_types(): Данные о типах

    Returns:
        list: Список флагов о бинарности поля

    """
    binary_types_list = []

    for i, obj in enumerate(cols, start=1):
        dotted = '{0}.{1}'.format(obj['table'], obj['col'])
        map_type = TYPES_MAP.get(col_types[dotted])
        binary_types_list.append(map_type == TYPES_MAP.get('binary'))

    return binary_types_list


def get_binary_types_dict(cols, col_types):
    """
    Информация о бинарниках для инсерта в бд
    Args:
        cols(list): Данные о колонках
        col_types(): Данные о типах

    Returns:
        dict: Пронумированный словарь с флагами о бинарности поля
    """
    binary_types_dict = {}

    for i, obj in enumerate(cols, start=1):
        dotted = '{0}.{1}'.format(obj['table'], obj['col'])
        map_type = TYPES_MAP.get(col_types[dotted])
        binary_types_dict[str(i)] = map_type == TYPES_MAP.get('binary')

    return binary_types_dict


class LocalDbConnect(object):

    connection = DataSourceService.get_local_instance().connection

    def __init__(self, query, execute=True):

        self.query = query
        if execute:
            self.execute()

    def execute(self, args=None, many=False):
        with self.connection:
            with self.connection.cursor() as cursor:
                if not many:
                    cursor.execute(self.query, args)
                else:
                    cursor.executemany(self.query, args)

    def fetchall(self, args=None):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(self.query, args)
                return cursor.fetchall()


class SourceDbConnect(LocalDbConnect):

    connection = None

    def __init__(self, query, source, execute=False):
        self.connection = DataSourceService.get_source_connection(source)
        super(SourceDbConnect, self).__init__(query, execute)


class MongodbConnection(object):

    def __init__(self, name, db_name='etl', indexes=None):
        connection = pymongo.MongoClient(
            settings.MONGO_HOST, settings.MONGO_PORT)
        # database name
        db = connection[db_name]
        self.collection = db[name]
        if indexes:
            self.set_indexes(indexes)

    def get_collection(self, collection_name, db_name='etl'):
        """
        Получение коллекции с указанным названием

        Args:
            db_name(str): Название базы
            collection_name(str): Название коллекции
        """
        connection = pymongo.MongoClient(
            settings.MONGO_HOST, settings.MONGO_PORT)
        # database name
        db = connection[db_name]
        self.collection = db[collection_name]
        return self.collection

    @staticmethod
    def drop(collection_name, db_name='etl'):
        connection = pymongo.MongoClient(
            settings.MONGO_HOST, settings.MONGO_PORT)
        # database name
        db = connection[db_name]
        db.drop_collection(collection_name)

    def set_indexes(self, index_list):
        """
        Установка индексов

        Args:
            index_list(list): Список необходимых индексов
            ::
                [('_id', ASCENDING), ('_state', ASCENDING),
                        ('_date', ASCENDING)]
        """
        self.collection.create_indexes(
            [IndexModel([index]) for index in index_list])


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
        try:
            queue = Queue.objects.get(name=self.name)
        except Queue.DoesNotExist:
            raise TaskError("Очередь с именем %s не существует" % self.name)

        task = QueueList.objects.create(
            queue=queue,
            queue_status=QueueStatus.objects.get(title=TaskStatusEnum.IDLE),
            arguments=json.dumps(arguments),
            app='etl',
            checksum=arguments.get('checksum', ''),
        )

        task_id = task.id
        # канал для задач
        new_channel = settings.SOCKET_CHANNEL.format(
            arguments['user_id'], task_id)

        channel_data = {
            "channel": new_channel,
            "queue_id": task_id,
            "namespace": self.name,
            "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        # добавляем канал подписки в редис
        RedisSourceService.set_user_subscribers(
            arguments['user_id'], channel_data)

        return task_id, new_channel


    @staticmethod
    def get_queue(task_id, user_id):
        """
        Информация о ходе работы задач

        Returns:
            QueueStorage: ...
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
        IDLE: u"В ожидании",
        PROCESSING: u"В обработке",
        ERROR: u"Ошибка",
        DONE: u"Выполнено",
        DELETED: u"Удалено",
    }


class TaskLoadingStatusEnum(BaseEnum):
    """
    Состояние загрузки задачи
    """
    START, PROCESSING, FINISH, ERROR = ('start', 'processing', 'finish', 'error')
    values = {
        START: u"Старт",
        PROCESSING: u"В обработке",
        FINISH: u"Выполнено",
        ERROR: u"Ошибка",
    }

TLSE = TaskLoadingStatusEnum


class SourceTableStatusEnum(BaseEnum):
    """
    Статус состояния записей в таблице-источнике
    """

    IDLE, LOADED = ('idle', 'loaded')

    values = {
        IDLE: u"Выполнено",
        LOADED: u"Загружено"
    }

STSE = SourceTableStatusEnum


class DeltaTableStatusEnum(BaseEnum):
    """
    Статусы состояния записей в дельта-таблице
    """

    NEW, SYNCED = ('new', 'synced')

    values = {
        NEW: u"Новое",
        SYNCED: u"Синхронизировано",
    }

DTSE = DeltaTableStatusEnum


class AllKeysTableStatusEnum(BaseEnum):
    """
    Статусы состояния записей в таблице с данными на удаления
    """

    NEW, DELETED, SYNCED = ('new', 'deleted', 'synced')

    values = {
        NEW: u"Новое",
        DELETED: u"Удалено",
        SYNCED: u"Синхронизировано",
    }

AKTSE = AllKeysTableStatusEnum

date_fields = [
    ('raw_date', 'timestamp'),
    ('weekday', 'text'),
    ('year', 'integer'),
    ('month', 'integer'),
    ('month_text', 'text'),
    ('day', 'integer'),
    ('week_of_year', 'integer'),
    ('quarter', 'integer')
]


class DateTableColumnsName(BaseEnum):
    """
        Статусы тасков
    """
    TIME_ID, RAW_DATE, WEEKDAY, YEAR, MONTH, MONTH_TEXT, DAY, WEEK_OF_YEAR, QUARTER = (
        'time_id', 'raw_date', 'weekday', 'year', 'month', 'month_text',
        'day', 'week_of_year', 'quarter')

    values = {
        TIME_ID: u'id',
        RAW_DATE: u'Дата',
        WEEKDAY: u'День недели',
        YEAR: u'Год',
        MONTH: u'Месяц',
        MONTH_TEXT: u'Месяц текст',
        DAY: u'День',
        WEEK_OF_YEAR: u'Неделя года',
        QUARTER: u'Квартал',
    }

    types = [
        (RAW_DATE, 'timestamp'),
        (WEEKDAY, 'text'),
        (YEAR, 'integer'),
        (MONTH, 'integer'),
        (MONTH_TEXT, 'text'),
        (DAY, 'integer'),
        (WEEK_OF_YEAR, 'integer'),
        (QUARTER, 'integer'),
    ]

DTCN = DateTableColumnsName

