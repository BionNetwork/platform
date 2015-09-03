# -*- coding:utf-8 -*-
"""
Базовые модели приложения
"""
import datetime

from django.db import models
from django.utils import timezone


class Datasource(models.Model):
    """
    Источники данных содержат список всех подключений для сбора данных из разных источников
    """

    def __str__(self):
        return self.name

    def was_created_recently(self):
        return self.create_date >= timezone.now() - datetime.timedelta(days=1)

    name = models.CharField(max_length=255, help_text="название")
    host = models.CharField(max_length=255, help_text="имя хоста", db_index=True)
    port = models.IntegerField(help_text="Порт подключения")
    login = models.CharField(max_length=1024, null=True, help_text="логин")
    password = models.CharField(max_length=255, null=True, help_text="пароль")
    create_date = models.DateTimeField('create_date', help_text="дата создания", auto_now_add=True, db_index=True)
    user_id = models.IntegerField(help_text='идентификатор пользователя')

    class Meta:
        db_table = "datasources"


class DatasourceMeta(models.Model):
    """
    Мета информация для источников данных
    """

    database_name = models.CharField(max_length=255, help_text="название базы", db_index=True)
    collection_name = models.CharField(max_length=255, help_text="название коллекции")
    fields = models.TextField(help_text="мета-информация полей")
    stats = models.TextField(help_text="статистика", null=True)
    create_date = models.DateTimeField('create_date', help_text="дата создания", auto_now_add=True, db_index=True)
    update_date = models.DateTimeField('update_date', help_text="дата обновления", db_index=True)
    datasource = models.ForeignKey(Datasource, on_delete=models.CASCADE)

    class Meta:
        db_table = "datasources_meta"
