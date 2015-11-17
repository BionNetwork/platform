# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.utils import timezone
from .db.services import RetryQueryset

from djchoices import ChoiceItem, DjangoChoices

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
    name = models.CharField(verbose_name="название измерения", max_length=255, null=False, blank=False, db_index=True)
    title = models.CharField(verbose_name="название", max_length=255, null=False, blank=False)
    type = models.CharField(verbose_name="тип измерения", max_length=255, null=False, blank=False)
    visible = models.BooleanField(verbose_name="виден", null=False, default=True)
    high_cardinality = models.BooleanField(verbose_name="cardinality", null=False, default=False)
    data = models.TextField(verbose_name="иерархии", null=True, blank=True)
    create_date = models.DateTimeField(verbose_name="дата создания", db_index=True)
    update_date = models.DateTimeField(verbose_name="дата обновления", db_index=True)
    user = models.ForeignKey(User)
    datasources_meta = models.ForeignKey(DatasourceMeta)

    class Meta:
        db_table = "dimensions"
        verbose_name = 'Размерности'
        verbose_name_plural = 'Размерности'
