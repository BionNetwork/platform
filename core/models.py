# -*- coding: utf-8 -*-


import os
import datetime

from django.core.exceptions import ValidationError
from django.db import models
from django.contrib.postgres.fields import JSONField

from core.db.models.fields import *
from django.contrib.auth.models import AbstractUser
from django.utils import timezone

from djchoices import ChoiceItem, DjangoChoices

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
    # FIXME remove user_id
    user_id = models.IntegerField(help_text='идентификатор пользователя', null=True)
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

    def get_dir_path(self):
        # возвращает полный путь директории, в которой лежит файл
        if self.is_file:
            return os.path.dirname(self.get_file_path())

    def get_temp_dir_path(self):
        # возвращает полный путь для време
        return os.path.join(self.get_dir_path(), 'temp')

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

    def source_temp_copy(self, new_file):
        """
        Сохранение временного файла для замены источника на основе файла
        """
        copy = Datasource(conn_type=self.conn_type, user_id=1)
        temp_file_path = os.path.join(
            self.get_temp_dir_path(), new_file.name)
        copy.file.save(temp_file_path, new_file, save=False)

        return copy

    def mark_file_name_as_old(self):
        """
        Переименовываем старый файл при замене
        """
        if self.is_file and self.file:
            file_path = self.get_file_path()
            base_dir = self.get_dir_path()
            file_name = os.path.basename(file_path)
            new_file_name = "old-{0}-{1}".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                file_name
            )
            os.rename(file_path,
                      os.path.join(base_dir, new_file_name))

    def remove_temp_file(self):
        """
        Удаляет временный файл
        """
        os.remove(self.get_file_path())

    def validate_file_copy(self):
        """
        Путь временной копии файла для валидации
        """
        if self.is_file and self.file:
            path = self.file.path
            validate_file_path = os.path.join(
                os.path.dirname(path), 'validate', os.path.basename(path))
            return validate_file_path

    def create_validation_file(self):
        """
        Временная копия файла для валидации
        """
        validate_file_path = self.validate_file_copy()
        self.file.save(validate_file_path, self.file, save=False)

    class Meta:
        db_table = "datasources"


class SettingNameChoices(DjangoChoices):
    """Типы подключения"""
    INDENT = ChoiceItem(1, 'indent')


class DatasourceSettings(models.Model):
    """
    Таблица настроек для источников
    """
    name = models.IntegerField(verbose_name='Название', db_index=True, choices=SettingNameChoices.choices)
    value = models.CharField(max_length=255, verbose_name='Значение')
    datasource = models.ForeignKey(Datasource, verbose_name='Источник', related_name='settings')

    class Meta:
        db_table = "datasources_settings"
        unique_together = ('name', 'datasource')

    def __str__(self):
        return "{name}({source}): {value}".format(name=self.name, source=self.datasource.name, value=self.value)

    def clean(self):
        if self.name == SettingNameChoices.INDENT and not self.datasource.file:
            raise ValidationError({'name': 'Отступ можно установить только для файловых источников'})


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


class DatasetStateChoices(DjangoChoices):
    """
    Cтатусы для Dataset
    """
    IDLE = ChoiceItem(1, 'В ожидании данных')
    FILLUP = ChoiceItem(2, 'Наполнение данных')
    FTW = ChoiceItem(6, 'Foreign Table')
    DIMCR = ChoiceItem(3, 'Создание размерностей')
    MSRCR = ChoiceItem(4, 'Создание мер')
    LOADED = ChoiceItem(5, 'Загрузка данных завершилась')


class Dataset(models.Model):
    """
    Модель
    """
    key = models.TextField(verbose_name='Ключ', unique=True)
    date_created = models.DateTimeField(
        verbose_name="Дата создания", auto_now_add=True, db_index=True)
    update_date = models.DateTimeField(
        verbose_name="Дата обновления", auto_now=True, db_index=True)
    context = JSONField(null=True, default='')
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

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        super(Dataset, self).save()


class DatasetSource(models.Model):
    """
    Связь источника и хранилища
    """

    dataset = models.ForeignKey(Dataset, name="Хранилище")
    source = models.ForeignKey(Datasource, name="Источник")

    class Meta:
        db_table = "dataset_source"


class ColumnTypeChoices(DjangoChoices):
    """
    Cтатусы для Dataset
    """
    STRING = ChoiceItem(1, 'text')
    INTEGER = ChoiceItem(2, 'integer')
    DOUBLE = ChoiceItem(3, 'double precision')
    TIMESTAMP = ChoiceItem(4, 'timestamp')
    BOOLEAN = ChoiceItem(5, 'bool')

    # FIXME доработать  BOOLEAN, вроде не работает

    @classmethod
    def get_type(cls, type_name):
        """
        Возвращает ключ по значению
        """
        for k, v in cls.choices:
            if v == type_name:
                return k
        raise Exception("No such type of ColumnChoices")

    @classmethod
    def filter_types(cls):
        """
        Список колонок для фильтров
        """
        return [cls.STRING, cls.TIMESTAMP, cls.BOOLEAN]

    @classmethod
    def measure_types(cls):
        """
        Список колонок для фильтров
        """
        return [cls.INTEGER, cls.DOUBLE]


class Columns(models.Model):
    """
    Колонки в кубе
    """
    name = models.CharField(verbose_name="Название", max_length=255)
    original_name = models.CharField(
        verbose_name="Название в источнике", max_length=255)
    original_table = models.CharField(
        verbose_name="Название таблицы в источнике",
        max_length=255, default='')
    source = models.ForeignKey(DatasetSource, verbose_name="Источник хранилища")
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
