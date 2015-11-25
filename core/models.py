# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.utils import timezone
from djchoices import ChoiceItem, DjangoChoices

from .db.services import RetryQueryset
from .helpers import get_utf8_string

"""
Базовые модели приложения
"""


class ConnectionChoices(DjangoChoices):
    """Типы подключения"""
    POSTGRESQL = ChoiceItem(1, 'Postgresql')
    MYSQL = ChoiceItem(2, 'Mysql')


class Datasource(models.Model):
    """Источники данных содержат список всех подключений
    для сбора данных из разных источников
    """

    def __str__(self):
        return "<Datasource object> " + self.host + " " + self.db

    def was_created_recently(self):
        return self.create_date >= timezone.now() - datetime.timedelta(days=1)

    db = models.CharField(max_length=255, help_text="База данных", null=False)
    host = models.CharField(max_length=255, help_text="имя хоста", db_index=True)
    port = models.IntegerField(help_text="Порт подключения")
    login = models.CharField(max_length=1024, null=True, help_text="логин")
    password = models.CharField(max_length=255, null=True, help_text="пароль")
    create_date = models.DateTimeField('create_date', help_text="дата создания", auto_now_add=True, db_index=True)
    user_id = models.IntegerField(help_text='идентификатор пользователя')
    conn_type = models.SmallIntegerField(
        verbose_name='Тип подключения', choices=ConnectionChoices.choices,
        default=ConnectionChoices.POSTGRESQL)

    objects = models.Manager.from_queryset(RetryQueryset)()

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

    def set_from_dict(self, **data):
        """Заполнение объекта из словаря"""
        self.__dict__.update(data)
        if 'id' in data:
            self.id = data['id']

    class Meta:
        db_table = "datasources"
        unique_together = ('host', 'db', 'user_id')


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
        (STANDART_DIMENSION, 'StandardDimension'),
        (STANDART_DIMENSION, 'TimeDimension'),
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


class Measure(models.Model):
    """Меры для кубов"""
    STRING = 1
    INTEGER = 2
    NUMERIC = 3
    BOOLEAN = 4
    DATE = 5
    TIME = 6
    TIMESTAMP = 7
    MEASURE_TYPE = (
        (STRING, 'String'),
        (INTEGER, 'Integer'),
        (NUMERIC, 'Numeric'),
        (BOOLEAN, 'Boolean'),
        (DATE, 'Date'),
        (TIME, 'Time'),
        (TIMESTAMP, 'Timestamp')
    )

    SUM = 1
    AGR_FUNCTIONS = (
        (SUM, 'sum'),
    )

    name = models.CharField(
        verbose_name="Название меры", max_length=255, db_index=True)
    title = models.CharField(verbose_name="Название", max_length=255)
    type = models.SmallIntegerField(
        verbose_name="Тип измерения",
        choices=MEASURE_TYPE, default=STRING)
    aggregator = models.SmallIntegerField(
        verbose_name="Функция агрегирования",
        choices=AGR_FUNCTIONS, default=SUM)
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

    class META:
        db_table = 'measures'
        verbose_name = 'Мера'
        verbose_name_plural = 'Меры'
