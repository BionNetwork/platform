# -*- coding: utf-8 -*-
import json

from django.contrib import admin
from django.core.exceptions import ValidationError
from django.db import models
from django.conf import settings
from core.models import DatasourceMetaKeys, QueueList, Datasource
from etl.constants import FIELD_NAME_SEP
from etl.helpers import get_table_name, TaskStatusEnum, DataSourceService

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


class OlapEntityCreation(object):
    """
    Создание сущностей(рамерности, измерения) олап куба
    """

    actual_fields_type = []

    def __init__(self, module='biplatform.etl.models', app_name='sttm'):
        self.module = module
        self.app_name = app_name
        self.source = None
        self.source_table_name = None
        self.meta = None
        self.meta_data = None
        self.actual_fields = None

    @staticmethod
    def get_fields_list(meta_data):
        """
        Получение списка полей таблицы

        Args:
            meta_data(dict): метаданные таблицы-источника
        """
        all_fields = []
        for table, fields in meta_data['columns'].iteritems():
            for field in fields:
                field['name'] = '{0}{1}{2}'.format(
                    table, FIELD_NAME_SEP, field['name'])
                all_fields.append(field)
        return all_fields

    def get_actual_fields(self):
        """
        Фильтруем поля по необходимому нам типу

        Returns:
            list: Метаданные отфильтрованных полей
        """
        all_fields = self.get_fields_list(self.meta_data)
        return [element for element in all_fields
                if element['type'] in self.actual_fields_type]

    def rows_query(self, fields):
        """
        Формируюм строку запроса

        Args:
            fields(list): Список именно необходимых имен

        Returns:
            str: Строка запроса
        """
        fields_str = '"'+'", "'.join(fields)+'"'
        query = "SELECT {0} FROM {1} LIMIT {2} OFFSET {3};"
        return query.format(
            fields_str, self.source_table_name, '{0}', '{1}')

    def load_data(self, task_id):
        """
        Создание таблицы размерности/меры

        Args:
            task_id(int): id Задачи
        """

        task = QueueList.objects.get(id=task_id)

        # обрабатываем таски со статусом 'В ожидании'
        if task.queue_status.title != TaskStatusEnum.IDLE:
            pass

        data = json.loads(task.arguments)

        self.source = Datasource.objects.get(id=data['datasource_id'])
        self.source_table_name = data['source_table']
        self.meta = DatasourceMetaKeys.objects.get(value=data['key']).meta
        self.meta_data = json.loads(self.meta.fields)
        self.actual_fields = self.get_actual_fields()

        f_list = []
        table_name = data['target_table']
        for field in self.actual_fields:
            field_name = field['name']

            f_list.append(get_field_settings(field_name, field['type']))

        model = get_django_model(
            table_name, f_list, self.app_name,
            self.module, table_name)
        install(model)

        # Сохраняем метаданные
        self.save_meta_data(
            data['user_id'], table_name, self.actual_fields)
        self.save_fields(model)

    def save_meta_data(self, user_id, table_name, fields):
        """
        Сохранение метаинформации

        Args:
            user_id(int): id пользователя
            table_name(str): Название создаваемой таблицы
            fields(dict): данные о полях
            meta(DatasourceMeta): ссылка на метаданные хранилища
        """
        raise NotImplementedError

    def save_fields(self, model):
        """Заполняем таблицу данными

        Args:
            model: Модель к целевой таблице
        """
        actual_fields_name = [field['name'] for field in self.actual_fields]
        offset = 0
        step = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT
        print 'load dim or measure'
        while True:
            rows_query = self.rows_query(actual_fields_name)
            index_to = offset+step
            connection = DataSourceService.get_local_instance().connection
            cursor = connection.cursor()

            cursor.execute(rows_query.format(index_to, offset))
            rows = cursor.fetchall()
            if not rows:
                break
            column_data = [model(
                **{actual_fields_name[i]: v for (i, v) in enumerate(x)})
                        for x in rows]
            model.objects.bulk_create(column_data)
            offset = index_to
