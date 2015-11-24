# -*- coding: utf-8 -*-

from django.contrib import admin
from django.db import models

type_match = {
    'text': 'CharField',
    'integer': 'IntegerField',
    'double precision': 'FloatField',
    'timestamp': 'DateTimeField',
}


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
