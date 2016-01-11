# -*- coding: utf-8 -*-
import json
import logging

from django.contrib import admin
from psycopg2 import errorcodes
from django.db import models
from django.conf import settings
from core.models import DatasourceMetaKeys, QueueList, Datasource
from etl.constants import FIELD_NAME_SEP
from etl.helpers import TaskStatusEnum, DataSourceService, \
    RedisSourceService, TaskService, TaskErrorCodeEnum
from etl.services.middleware.base import datetime_now_str

logger = logging.getLogger(__name__)

type_match = {
    'text': ('TextField', [('blank', True), ('null', True)]),
    'integer': ('IntegerField', [('blank', True), ('null', True)]),
    'double precision': ('FloatField', [('blank', True), ('null', True)]),
    'timestamp': ('DateTimeField', [('blank', True), ('null', True)])
}


def get_field_settings(f_name, f_type):
    return dict(
        name=f_name,
        type=type_match[f_type][0],
        settings=type_match[f_type][1]
    )


def create_model(name, fields=None, app_label='',
                 module='', options=None, admin_opts=None):
    """
    Динамическое создание модели базы данных

    Args:
        name(unicode): Название
        fields(dict): Поля модели
        app_label(unicode): Название приложения
        module(unicode): Информации о модуле
        options(dict): Мета-данные
        admin_opts(dict): Мета-данные для административной панели

    Returns:
        `name`: Созданный класс-модель
    """

    # Настройки
    class Meta:
        pass

    if app_label:
        setattr(Meta, 'app_label', app_label)

    if options is not None:
        for key, value in options.iteritems():
            setattr(Meta, key, value)

    attrs = {'__module__': module, 'Meta': Meta}

    # Добавление полей
    if fields:
        attrs.update(fields)

    # Создание модели
    model = type(str(name), (models.Model,), attrs)

    # Добавление данных для админ. части
    if admin_opts is not None:
        class Admin(admin.ModelAdmin):
            pass
        for key, value in admin_opts:
            setattr(Admin, key, value)
        admin.site.register(model, Admin)

    return model


def install(model):
    """
    Создание соответсвующей для модели таблицы в базе данных

    Args:
        model: Синхронизируемая модель
    """
    from django.core.management import sql, color
    from django.db import connection

    # Standard syncdb expects models to be in reliable locations,
    # so dynamic models need to bypass django.core.management.syncdb.
    # On the plus side, this allows individual models to be installed
    # without installing the entire project structure.
    # On the other hand, this means that things like relationships and
    # indexes will have to be handled manually.
    # This installs only the basic table definition.

    # disable terminal colors in the sql statements
    style = color.no_style()

    cursor = connection.cursor()
    statements, pending = connection.creation.sql_create_model(model, style)
    for sql in statements:
        cursor.execute(sql)


def get_django_field(field):
    """
    """
    setting = [(s[0], s[1]) for s in field['settings']]

    return getattr(models, field['type'])(**dict(setting))


def get_django_model(name, fields_list, app_name, module_name, table_name=None):
    """
    Возращает Django модель основанная на текущих данных
    """
    fields = [(f['name'], get_django_field(f)) for f in fields_list]

    options = {'db_table': table_name} if table_name else None

    return create_model(
        name, dict(fields), app_name, module_name, options=options)