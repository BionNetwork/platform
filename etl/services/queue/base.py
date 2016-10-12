# coding: utf-8


import binascii
import json
import logging

import pymongo
import requests
from bson import binary
from pymongo import IndexModel

from django.conf import settings

from core.models import QueueList, QueueStatus
from etl.helpers import datetime_now_str, HashEncoder
from etl.services.datasource.db.interfaces import BaseEnum


logger = logging.getLogger(__name__)


class PusherCodes(object):
    """
    Список кодов
    """
    FT = 1001
    VIEW = 1002
    DM = 1003
    FINAL = 1004


class Pusher(object):
    """
    Уведомитель PHP сервера о ходе загрузки
    """
    def __init__(self, cube_id):
        """
        cube_id - идентификатор канала
        php_url - канал
        """
        self.cube_id = cube_id
        self.php_url = "{0}{1}:{2}".format(
            settings.PHP_SCHEMA, settings.PHP_HOST, settings.PHP_PORT)

    def push(self, code, msg=None, data=None):
        # FIXME temporary structure of data
        info = {
            'cube_id': self.cube_id,
            'code': code,
            'msg': msg,
            'data': data,
        }
        try:
            resp = requests.post(self.php_url, json.dumps(info))
        except requests.exceptions.ConnectionError as e:
            print("Problem with notification push: {0}".format(e.message))

    def push_foreign_table(self, table):
        """
        Уведомление о создании foreign table
        """
        msg = "{0}: Загружено во временную таблицу!".format(table)
        self.push(PusherCodes.FT, msg)

    def push_view(self, table):
        """
        Уведомление о создании view
        """
        msg = "{0}: Создано view!".format(table)
        self.push(PusherCodes.VIEW, msg)

    def push_dim_meas(self):
        """
        Уведомление о создании мер и размерностей
        """
        msg = "Созданы меры и размерности!"
        self.push(PusherCodes.DM, msg)

    def push_final(self, data):
        """
        Возвращает инфу о загруженных таблицах и колонках
        """
        self.push(PusherCodes.FINAL, data=data)


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
        self.queue[key] = val

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

    def update(self, **queue_info):
        for k, v in list(queue_info.items()):
            if k not in self.allowed_keys:
                raise KeyError('Неверный ключ для словаря информации задачи!')
            self.queue[k] = v

        self.queue['date_updated'] = datetime_now_str()


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
        # percent = self.percent
        # if status == TLSE.FINISH or percent >= 100:
        #     client.publish(self.channel, json.dumps(
        #         {'percent': 100,
        #          'taskId': self.task_id,
        #          'event': TLSE.FINISH}
        #     ))
        #     self.is_complete = True
        # elif status == TLSE.START:
        #     client.publish(self.channel, json.dumps(
        #         {'percent': 0,
        #          'taskId': self.task_id,
        #          'event': TLSE.START}
        #     ))
        # elif status == TLSE.PROCESSING:
        #     client.publish(self.channel, json.dumps(
        #         {'percent': percent,
        #          'taskId': self.task_id,
        #          'event': TLSE.PROCESSING}
        #     ))
        # else:
        #     client.publish(self.channel, json.dumps(
        #         {'percent': percent,
        #          'taskId': self.task_id,
        #          'event': TLSE.ERROR,
        #          'msg': msg}
        #     ))


def set_task(func, context):
    """
    Args:
        func: Исполняемая функция
        context(dict): Контекст выполнения

    Returns:
        `Signature`: Celery-задача к выполнению
        list: Список каналов для сокетов
    """
    task_name = func.name

    # FIXME подумать чо тут и как тут для мультизагрузки
    task_id, channel = TaskService(task_name).add_task(
        arguments=context)
    # return task_def.apply_async((task_id, channel, context),), [channel]
    return func(task_id, channel, context), [channel]


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


def process_binary_data(record, binary_types_list, process_func=None):
    """
    Если данные бинарные, то оборачиваем в bson.binary.Binary,
    если process_func задан, то бинарные данные обрабатываем ею,
    например для расчета хэша бин данных, который используется для
    подсчета cdc_key используем binascii.b2a_base64
    """
    process_func = process_func or binary.Binary

    new_record = list()

    for (rec, is_binary) in zip(record, binary_types_list):
        new_record.append(
            process_func(bytes(rec)) if is_binary and rec is not None else rec)

    new_record = tuple(new_record)
    return new_record


def calc_key_for_row(row, tables_key_creators, row_num, binary_types_list=None):
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
    if binary_types_list:
        row = process_binary_data(row, binary_types_list, binascii.b2a_base64)

    if len(tables_key_creators) > 1:
        row_values_for_calc = [
            str(each.calc_key(row, row_num)) for each in tables_key_creators]
        return HashEncoder.encode(''.join(row_values_for_calc))
    else:
        return tables_key_creators[0].calc_key(row, row_num)


def simple_key_for_row(row, row_num):
    """
    Calculating hash for record by record and record order number
    """
    return HashEncoder.encode(
        reduce(lambda res, x: '%s%s' % (res, x), [row, row_num]).encode("utf8"))


def get_binary_types_list(cols, col_types):
    # инфа о бинарниках для генерации ключа
    # генерит список [False, False, True, ...] - инфа
    # о том, какие данные в строке бинарные
    binary_types_list = []

    for i, obj in enumerate(cols, start=1):
        dotted = '{0}.{1}'.format(obj['table'], obj['col'])
        map_type = TYPES_MAP.get(col_types[dotted])
        binary_types_list.append(map_type == TYPES_MAP.get('binary'))

    return binary_types_list


def get_binary_types_dict(cols, col_types):
    # инфа о бинарниках для инсерта в бд
    # генерит словарь {'1': False, '2': False, '3': True, ...} - инфа
    # о том, какие данные в строке бинарные
    binary_types_list = get_binary_types_list(cols, col_types)
    binary_types_dict = {
        str(i): k for i, k in enumerate(binary_types_list, start=1)}

    return binary_types_dict


def fetch_date_intervals(meta_info):
    """
    возвращает интервалы для таблицы дат
    :type meta_info: dict
    """
    date_intervals_info = [
        (t_name, t_info['date_intervals'])
        for (t_name, t_info) in meta_info.items()]

    return date_intervals_info


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
        # FIXME раскоментить, когда с мультисоурсами разберемся
        # try:
        #     queue = Queue.objects.get(name=self.name)
        # except Queue.DoesNotExist:
        #     raise TaskError("Очередь с именем %s не существует" % self.name)

        task = QueueList.objects.create(
            queue_id=1,
            # queue=queue,
            queue_status=QueueStatus.objects.get(title=TaskStatusEnum.IDLE),
            arguments=json.dumps(arguments),
            app='etl',
            checksum='555555555',
        )

        task_id = task.id

        return task_id

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
        IDLE: "В ожидании",
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
    Статус состояния записей в таблице-источнике
    """

    IDLE, LOADED = ('idle', 'loaded')

    values = {
        IDLE: "Выполнено",
        LOADED: "Загружено"
    }

STSE = SourceTableStatusEnum


class TableRecordStatusEnum(BaseEnum):
    """
    Статус состояния записей в таблице-источнике!
    PREV - означает, что запись загружена в предыдущей загрузке
    NEW - означает, что запись загружена в нынешней загрузке
    """

    PREV, NEW = list(range(2))


TRSE = TableRecordStatusEnum


class DeltaTableStatusEnum(BaseEnum):
    """
    Статусы состояния записей в дельта-таблице
    """

    NEW, SYNCED = ('new', 'synced')

    values = {
        NEW: "Новое",
        SYNCED: "Синхронизировано",
    }

DTSE = DeltaTableStatusEnum


class AllKeysTableStatusEnum(BaseEnum):
    """
    Статусы состояния записей в таблице с данными на удаления
    """

    NEW, DELETED, SYNCED = ('new', 'deleted', 'synced')

    values = {
        NEW: "Новое",
        DELETED: "Удалено",
        SYNCED: "Синхронизировано",
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
    (TIME_ID, THE_DATE, THE_DAY, THE_YEAR, MONTH_THE_YEAR, THE_MONTH,
        DAY_OF_MONTH, WEEK_OF_YEAR, QUARTER) = (
        'time_id', 'the_date', 'the_day', 'the_year', 'month_of_year',
        'the_month', 'day_of_month', 'week_of_year', 'quarter')

    values = {
        TIME_ID: 'id',
        THE_DATE: 'Дата',
        THE_DAY: 'День недели',
        THE_YEAR: 'Год',
        MONTH_THE_YEAR: 'Месяц',
        THE_MONTH: 'Месяц текст',
        DAY_OF_MONTH: 'День',
        WEEK_OF_YEAR: 'Неделя года',
        QUARTER: 'Квартал',
    }

    types = [
        (THE_DATE, 'timestamp'),
        (THE_DAY, 'text'),
        (THE_YEAR, 'integer'),
        (MONTH_THE_YEAR, 'integer'),
        (THE_MONTH, 'text'),
        (DAY_OF_MONTH, 'integer'),
        (WEEK_OF_YEAR, 'integer'),
        (QUARTER, 'integer'),
    ]

DTCN = DateTableColumnsName

