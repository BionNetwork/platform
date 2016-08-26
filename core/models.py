# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime

from django.db import models
from core.db.models.fields import *
from django.contrib.auth.models import AbstractUser
from django.utils import timezone

from djchoices import ChoiceItem, DjangoChoices
from core.model_helpers import MultiPrimaryKeyModel

from .db.services import RetryQueryset
from .helpers import (get_utf8_string, users_avatar_upload_path,
                      users_file_upload_path)

"""
Базовые модели приложения
"""


class ConnectionChoices(DjangoChoices):
    """Типы подключения"""
    POSTGRESQL = ChoiceItem(1, 'Postgresql')
    MYSQL = ChoiceItem(2, 'Mysql')
    MS_SQL = ChoiceItem(3, 'MsSql')
    ORACLE = ChoiceItem(4, 'Oracle')
    EXCEL = ChoiceItem(5, 'Excel')
    CSV = ChoiceItem(6, 'Csv')
    TXT = ChoiceItem(7, 'Text')

CC = ConnectionChoices


class Datasource(models.Model):
    """Источники данных содержат список всех подключений
    для сбора данных из разных источников
    """

    def __str__(self):
        return ' '.join(
            ["Datasource:", self.get_source_type(),
             self.name or '', self.host or '', self.db or '']
        )

    def get_source_type(self):
        # Название типа соурса
        return ConnectionChoices.values.get(self.conn_type)

    def was_created_recently(self):
        return self.create_date >= timezone.now() - datetime.timedelta(days=1)

    name = models.CharField(
        verbose_name='Название источника', max_length=255, null=True)
    db = models.CharField(max_length=255, help_text="База данных", null=True)
    host = models.CharField(
        max_length=255, help_text="имя хоста", db_index=True, null=True)
    port = models.IntegerField(help_text="Порт подключения", null=True)
    login = models.CharField(max_length=1024, null=True, help_text="логин")
    password = models.CharField(max_length=255, null=True, help_text="пароль")
    create_date = models.DateTimeField(
        'create_date', help_text="дата создания", auto_now_add=True, db_index=True)
    user_id = models.IntegerField(help_text='идентификатор пользователя')
    conn_type = models.SmallIntegerField(
        verbose_name='Тип подключения', choices=ConnectionChoices.choices,
        default=ConnectionChoices.POSTGRESQL)
    file = models.FileField(
        verbose_name='Файл', upload_to=users_file_upload_path,
        null=True, max_length=500)

    objects = models.Manager.from_queryset(RetryQueryset)()

    def save(self, *args, **kwargs):
        if self.pk is None:
            saved_file = self.file
            self.file = None
            super(Datasource, self).save(*args, **kwargs)

            self.file = saved_file
            self.save()
        else:
            super(Datasource, self).save(*args, **kwargs)

    def get_connection_dict(self):
        return {
            'host': get_utf8_string(self.host),
            'login': get_utf8_string(self.login or ''),
            'password': get_utf8_string(self.password or ''),
            'db': get_utf8_string(self.db),
            'port': self.port,
            'conn_type': self.conn_type,
            # доп параметры для селери тасков юзера
            'id': self.id,
            'user_id': self.user_id,
        }

    @property
    def is_file(self):
        # признак источника-файла
        return self.conn_type in [CC.EXCEL, CC.CSV, CC.TXT, ]

    def get_file_path(self):
        # возвращает полный путь файла
        if self.is_file:
            return self.file.path

    def get_source_info(self):
        """
        Инфа соурса
        """
        if not self.is_file:
            return {
                'name': get_utf8_string(self.name or ''),
                'host': get_utf8_string(self.host or ''),
                'db': get_utf8_string(self.db or ''),
                'source_id': self.id,
                'user_id': self.user_id,
                'is_file': False,
            }
        else:
            return {
                'name': get_utf8_string(self.name or self.file.name),
                'source_id': self.id,
                'user_id': self.user_id,
                'file_name': self.file.name,
                'is_file': True,
            }

    def set_from_dict(self, **data):
        """Заполнение объекта из словаря"""
        self.__dict__.update(data)
        if 'id' in data:
            self.id = data['id']

    class Meta:
        db_table = "datasources"
        unique_together = ('host', 'db', 'user_id')


class DatasourceSettings(models.Model):
    """
    Таблица настроек для источников
    """
    TRIGGERS = 'apply_triggers'
    CHECKSUM = 'apply_checksum'
    SETTING_CDC_NAME = 'cdc_type'
    name = models.CharField(max_length=255, verbose_name=u'Название', db_index=True)
    value = models.CharField(max_length=255, verbose_name=u'Значение')
    datasource = models.ForeignKey(Datasource, verbose_name=u'Источник', related_name='settings')

    class Meta:
        db_table = "datasources_settings"

    def __str__(self):
        return self.value


# FIXME: К удалению
class DatasourceMeta(models.Model):
    """
    Мета информация для источников данных
    """

    collection_name = models.CharField(
        max_length=255, help_text="название коллекции")
    fields = models.TextField(help_text="мета-информация полей")
    stats = models.TextField(help_text="статистика", null=True)
    create_date = models.DateTimeField(
        'create_date', help_text="дата создания",
        auto_now_add=True, db_index=True)
    update_date = models.DateTimeField(
        'update_date', help_text="дата обновления",
        auto_now=True, db_index=True)
    datasource = models.ForeignKey(Datasource, on_delete=models.CASCADE)

    objects = models.Manager.from_queryset(RetryQueryset)()

    class Meta:
        db_table = "datasources_meta"


class DatasourceMetaKeys(models.Model):
    """
    Ключ к динамически создаваемым таблицам
    """
    meta = models.ForeignKey(
        DatasourceMeta, verbose_name=u'Метаданные', related_name=u'meta_keys')
    value = models.CharField(verbose_name=u'Ключ', max_length=255)

    class Meta:
        db_table = 'datasources_meta_keys'
        unique_together = ('meta', 'value')


class User(AbstractUser):
    """
    Модель пользователей, унаследованная от Django User
    """

    phone = models.CharField(verbose_name='Телефон', max_length=32, null=True, blank=True)
    skype = models.CharField(verbose_name='Skype', max_length=50, null=True, blank=True)
    site = models.CharField(verbose_name='Cайт', max_length=50, null=True, blank=True)
    city = models.CharField(verbose_name='Город', max_length=50, null=True, blank=True)
    middle_name = models.CharField(max_length=50, blank=True, verbose_name='Отчество', default='')
    birth_date = models.DateField(verbose_name='Дата рождения', null=True, blank=True)
    verify_email_uuid = models.CharField(max_length=50, null=True, blank=True)
    avatar_small = models.ImageField(
        verbose_name='Аватар preview', upload_to=users_avatar_upload_path,
        null=True, blank=True, max_length=500)
    avatar = models.ImageField(
        verbose_name='Аватар', upload_to=users_avatar_upload_path, null=True,
        blank=True, max_length=500)

    # objects = models.Manager.from_queryset(RetryQueryset)()

    class Meta:
        db_table = "users"
        verbose_name = 'Пользователь'
        verbose_name_plural = 'Пользователи'


class Dimension(models.Model):
    """
    Размерности для кубов
    """
    STANDART_DIMENSION = 'SD'
    TIME_DIMENSION = 'TD'
    DIMENSION_TYPE = (
        (STANDART_DIMENSION, 'OTHER'),
        (TIME_DIMENSION, 'TIME'),
    )
    name = models.CharField(
        verbose_name="название измерения", max_length=255, db_index=True)
    title = models.CharField(verbose_name="название", max_length=255)
    type = models.CharField(
        verbose_name="тип измерения", max_length=255,
        choices=DIMENSION_TYPE, default=STANDART_DIMENSION)
    visible = models.BooleanField(verbose_name="виден", default=True)
    high_cardinality = models.BooleanField(
        verbose_name="cardinality", default=False)
    data = models.TextField(verbose_name="иерархии", null=True, blank=True)
    create_date = models.DateTimeField(
        verbose_name="дата создания", auto_now_add=True, db_index=True)
    update_date = models.DateTimeField(
        verbose_name="дата обновления", auto_now=True, db_index=True)
    user = models.ForeignKey(User, verbose_name=u'Пользователь')
    datasources_meta = models.ForeignKey(
        DatasourceMeta, related_name='dimension')

    class Meta:
        db_table = "dimensions"
        verbose_name = 'Размерность'
        verbose_name_plural = 'Размерности'

    def get_dimension_type(self):
        return ('OTHER' if self.type == self.STANDART_DIMENSION
            else 'TIME')


class Measure(models.Model):
    """Меры для кубов"""
    STRING = 'string'
    INTEGER = 'integer'
    NUMERIC = 'numeric'
    BOOLEAN = 'boolean'
    DATE = 'date'
    TIME = 'time'
    TIMESTAMP = 'timestamp'
    BYTEA = 'bytea'
    MEASURE_TYPE = (
        (STRING, 'string'),
        (INTEGER, 'integer'),
        (NUMERIC, 'numeric'),
        (BOOLEAN, 'boolean'),
        (DATE, 'date'),
        (TIME, 'time'),
        (TIMESTAMP, 'timestamp'),
        (BYTEA, 'bytea'),
    )

    SUM = 'sum'
    AGR_FUNCTIONS = (
        (SUM, 'sum'),
    )

    name = models.CharField(
        verbose_name="Название меры", max_length=255, db_index=True)
    title = models.CharField(verbose_name="Название", max_length=255)
    type = models.CharField(
        verbose_name="Тип измерения",
        choices=MEASURE_TYPE, default=INTEGER, max_length=50)
    aggregator = models.CharField(
        verbose_name="Функция агрегирования",
        choices=AGR_FUNCTIONS, null=True, max_length=50)
    format_string = models.CharField(
        verbose_name="Строка форматирования", max_length=255,
        null=True, blank=True)
    visible = models.BooleanField(verbose_name="Виден", default=True)
    create_date = models.DateTimeField(
        verbose_name="дата создания", auto_now_add=True, db_index=True)
    update_date = models.DateTimeField(
        verbose_name="дата обновления", auto_now=True, db_index=True)
    user = models.ForeignKey(User, verbose_name=u'Пользователь')
    datasources_meta = models.ForeignKey(
        DatasourceMeta, related_name='measure')

    class Meta:
        db_table = "measures"
        verbose_name = 'Мера'
        verbose_name_plural = 'Меры'


class QueueStatus(models.Model):
    """
    Статусы очередей
    """
    title = models.CharField(verbose_name='Название', max_length=50,
                             null=False, blank=False)
    description = models.CharField(verbose_name='Определение', max_length=50,
                                   null=True, blank=True)

    class Meta:
        db_table = "queue_status"


class Queue(models.Model):
    """
    Модель очередей
    """
    name = models.CharField(verbose_name='Название', max_length=50,
                            null=False, blank=False)
    interval = models.IntegerField(
        verbose_name='Интервал', null=True, blank=True)
    is_active = models.BooleanField('Активен', null=False, blank=False,
                                    default=True)

    class Meta:
        db_table = "queue"


class QueueList(models.Model):
    """
    Модель списка очередей
    """
    queue = models.ForeignKey(Queue, verbose_name='ид очереди',
                              null=False, blank=False)
    queue_status = models.ForeignKey(
        QueueStatus, verbose_name='статус очереди', null=False, blank=False)
    arguments = models.TextField(verbose_name='параметры запуска задачи',
                                 null=False, blank=False)
    app = models.CharField(verbose_name='модуль/приложение', max_length=50,
                           null=False, blank=False)
    date_created = models.DateTimeField(
        verbose_name="дата создания", auto_now_add=True,
        null=False, blank=False, db_index=True)
    update_date = models.DateTimeField(
        verbose_name="дата обновления", auto_now=True)
    comment = models.CharField(verbose_name='коментарий', max_length=1024,
                               null=True, blank=True)
    checksum = models.CharField(verbose_name='контрольная сумма', db_index=True,
                                max_length=255, null=False, blank=False)
    percent = models.FloatField(verbose_name='процент выполнения задачи',
                                null=True, blank=True)

    class Meta:
        db_table = "queue_list"
        index_together = ["queue", "date_created", "queue_status"]


class Cube(models.Model):
    name = models.CharField(max_length=1024, verbose_name="название куба")
    data = XmlField(verbose_name="xml схема куба", null=False)
    create_date = models.DateTimeField(
        verbose_name="дата создания", auto_now_add=True, db_index=True)
    update_date = models.DateTimeField(
        verbose_name="дата обновления", auto_now=True, db_index=True)
    user = models.ForeignKey(User, verbose_name=u'Пользователь')
    dataset = models.ForeignKey('Dataset', verbose_name=u'Датасет')

    class Meta:
        db_table = "cubes"


class DatasetStateChoices(DjangoChoices):
    """
    Cтатусы для Dataset
    """
    IDLE = ChoiceItem(1, 'В ожидании данных')
    FILLUP = ChoiceItem(2, 'Наполнение данных')
    DIMCR = ChoiceItem(3, 'Создание размерностей')
    MSRCR = ChoiceItem(4, 'Создание мер')
    LOADED = ChoiceItem(5, 'Загрузка данных завершилась')


class Dataset(models.Model):
    """
    Модель
    """
    key = models.TextField(verbose_name=u'Ключ', unique=True)
    date_created = models.DateTimeField(
        verbose_name="Дата создания", auto_now_add=True, db_index=True)
    update_date = models.DateTimeField(
        verbose_name="Дата обновления", auto_now=True, db_index=True)
    state = models.SmallIntegerField(
        verbose_name='Статус', choices=DatasetStateChoices.choices,
        default=DatasetStateChoices.IDLE, db_index=True)

    @classmethod
    def update_state(cls, dataset_id, state):
        # замена статуса датасета
        instance = cls.objects.get(id=dataset_id)
        instance.state = state
        instance.save()

    class Meta:
        db_table = "datasets"


class DatasetToMeta(models.Model, MultiPrimaryKeyModel):
    """
    Модель связи Мета источника и Dataset
    """
    meta = models.ForeignKey(
        DatasourceMeta, verbose_name=u'Мета источника')
    # FIXME обманка для Джанги, т.к. 1 primary key быть обязан
    # FIXME всегда при регистрации модели
    # FIXME на самом деле в миграции формируется primary key (meta, dataset)
    dataset = models.ForeignKey(
        Dataset, verbose_name=u'Данные', primary_key=True)

    def delete(self, using=None):
        MultiPrimaryKeyModel.delete(self, using)

    # переопределяем save, ставим force_insert=True,
    # чтобы inst.save() вызывал в бд тока инсерт запрос, а не
    # апдейт+инсерт
    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        models.Model.save(self, force_insert=True)

    class Meta:
        db_table = "datasets_to_meta"


class DatasourcesTrigger(models.Model):
    """
    Таблица созданных триггеров
    """
    name = models.CharField(
        verbose_name="Название", max_length=1024, db_index=True, null=False)
    src = models.TextField(verbose_name='текст триггера')
    collection_name = models.CharField(
        verbose_name="Название коллекции", max_length=1024, db_index=True)
    datasource = models.ForeignKey(Datasource, verbose_name=u'Источник')

    class Meta:
        db_table = "datasources_trigger"


class DatasourcesJournal(models.Model):
    """
    Таблица-журнал для триггеров
    """
    name = models.CharField(
        verbose_name="Название таблицы триггера источника",
        max_length=1024, db_index=True)
    collection_name = models.CharField(
        verbose_name="Название коллекции", max_length=1024, db_index=True)
    date_created = models.DateTimeField(
        verbose_name="дата создания", auto_now_add=True)
    date_updated = models.DateTimeField(
        verbose_name="дата обновления", auto_now=True)
    rows_read = models.IntegerField(verbose_name='Считано', default=0)
    rows_written = models.IntegerField(verbose_name='Записано', default=0)
    trigger = models.ForeignKey(DatasourcesTrigger, verbose_name="Триггер")

    class Meta:
        db_table = "datasources_journal"


class ColumnTypeChoices(DjangoChoices):
    """
    Cтатусы для Dataset
    """
    STRING = ChoiceItem(1, 'string')


class Columns(models.Model):
    """
    Колонки в кубе
    """
    name = models.CharField(verbose_name="Название", max_length=255)
    dataset = models.ForeignKey(Dataset, verbose_name="Хранилище")
    original_name = models.CharField(
        verbose_name="Название в источнике", max_length=255)
    original_table = models.CharField(
        verbose_name="Название таблицы в источнике",
        max_length=255, default='')
    source = models.ForeignKey(Datasource, verbose_name="Источник")
    type = models.CharField(
        verbose_name="Тип", choices=ColumnTypeChoices.choices,
        default=ColumnTypeChoices.STRING, max_length=20)
    format_string = models.CharField(verbose_name="размерность", max_length=20)
    visible = models.BooleanField(default=True)
    date_created = models.DateTimeField(
        verbose_name="дата создания", auto_now_add=True)
    date_updated = models.DateTimeField(
        verbose_name="дата обновления", auto_now=True)

    class Meta:
        db_table = 'columns'

    def __str__(self):
        return "{0}, {1} ({2})".format(
            self.dataset_id, self.source_id, self.original_name)
