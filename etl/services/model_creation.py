# -*- coding: utf-8 -*-
import json

from django.contrib import admin
from django.core.exceptions import ValidationError
from django.db import models
from django.conf import settings
from core.models import DatasourceMetaKeys

type_match = {
    'text': ('CharField', [('max_length', 255), ('blank', True), ('null', True)]),
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


def is_valid_field(self, field_data, all_data):
    """Валидация типа поля"""
    if hasattr(models, field_data) and issubclass(
            getattr(models, field_data), models.Field):
        return
    raise ValidationError("This is not a valid field type.")


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


class DataStore(object):

    def __init__(self, key, module='biplatform.etl.models', app_name='sttm',
                 table_prefix='datasource'):
        self.module = module
        self.app_name = app_name
        self.key = key if not key.startswith('-') else '_%s' % key[1:]
        self.meta = DatasourceMetaKeys.objects.get(value=key).meta
        self.table_name = '{prefix}_{key}'.format(
                prefix=table_prefix, key=self.key)

        self.meta_data = json.loads(self.meta.fields)
        self.all_fields = self.get_fields_list()
        self.table_model = None

    def get_fields_list(self):
        """
        Получение списка полей таблицы
        """
        all_fields = []
        for table, fields in self.meta_data['columns'].iteritems():
            for field in fields:
                field['name'] = '%s--%s' % (table, field['name'])
                all_fields.append(field)
        return all_fields

    def get_table_model(self):
        f_list = []

        for field in self.all_fields:
            f_list.append(get_field_settings(field['name'], field['type']))
        return get_django_model(
            self.table_name, f_list, self.app_name, self.module)


class OlapEntityCreation(object):
    """
    Создание сущностей(рамерности, измерения) олап куба
    """

    actual_fields_type = []

    def __init__(self, source):
        self.source = source

    @property
    def actual_fields(self):
        """
        Фильтруем поля по необходимому нам типу

        Returns:
            list: Метаданные отфильтрованных полей
        """
        return [element for element in self.source.all_fields
                if element['type'] in self.actual_fields_type]

    def load_data(self, user_id, task_id):
        """
        Создание таблицы размерности/меры

        Args:
            user_id(int): id Пользователя
            task_id(int): id Задачи
        """

        columns = {}
        actual_fields = self.actual_fields
        for field in actual_fields:
            field_name = field['name']
            table_name = '%s_%s' % (field_name, self.source.key)
            f = get_field_settings(field_name, field['type'])

            model = get_django_model(
                table_name, [f], self.source.app_name,
                self.source.module, table_name)
            install(model)

            # Сохраняем метаданные
            self.save_meta_data(user_id, field)
            columns.update({field_name: model})
        self.save_fields(columns)

    def save_meta_data(self, user_id, field):
        """
        Сохранение метаинформации

        Args:
            user_id(int): id пользователя
            field(dict): данные о поле
            meta(DatasourceMeta): ссылка на метаданные хранилища
        """
        raise NotImplementedError

    def save_fields(self, field_models):
        """Заполняем таблицу данными

        Args:
            field_models(dict): Словарь с моделями новых таблиц
        """
        print field_models
        fields_name = field_models.keys()
        index = 0
        while True:
            index_to = index+settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
            # TODO: Каждый раз делаем `source.get_table_model()`
            data = self.source.get_table_model().objects.values(
                *fields_name)[index:index_to]
            print data[:2]
            if not data:
                break
            for field, column in field_models.iteritems():
                column_data = [column(
                    **{k: v for (k, v) in x.iteritems() if k == field})
                            for x in data]
                column.objects.bulk_create(column_data)
            index = index_to
            print index
