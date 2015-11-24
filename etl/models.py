# -*- coding: utf-8 -*-

from django.core.validators import ValidationError
from django.db import models

from etl.services.model_creation import create_model


class App(models.Model):
    """Приложение"""
    name = models.CharField(max_length=255)
    module = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class Model(models.Model):
    """Модель"""
    app = models.ForeignKey(App, related_name='models')
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

    def get_django_model(self, table_name=None):
        """
        Возращает Django модель основанная на текущих данных
        """
        fields = [(f.name, f.get_django_field()) for f in self.fields.all()]

        options = {'db_table': '%s_%s' % (self.app.name, table_name)} if table_name else None

        return create_model(
            self.name, dict(fields), self.app.name, self.app.module, options=options)

    class Meta:
        unique_together = (('app', 'name'),)


def is_valid_field(self, field_data, all_data):
    """Валидация типа поля"""
    if hasattr(models, field_data) and issubclass(
            getattr(models, field_data), models.Field):
        return
    raise ValidationError("This is not a valid field type.")


class Field(models.Model):
    """Поле модели"""
    model = models.ForeignKey(
        Model, verbose_name=u"Модель", related_name='fields')
    name = models.CharField(verbose_name=u'Название', max_length=255)
    type = models.CharField(
        verbose_name=u'Тип', max_length=255, validators=[is_valid_field])

    def get_django_field(self):
        settings = [(s.name, s.value) for s in self.settings.all()]

        return getattr(models, self.type)(**dict(settings))

    class Meta:
        unique_together = (('model', 'name'),)


class Setting(models.Model):
    """Настройки поля"""
    field = models.ForeignKey(
        Field, verbose_name=u'Поле', related_name='settings')
    name = models.CharField(verbose_name=u'Название', max_length=255)
    value = models.CharField(max_length=255)

    class Meta:
        unique_together = (('field', 'name'),)
