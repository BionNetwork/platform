# coding: utf-8

from __future__ import unicode_literals

from django.db import models
from django.contrib.auth.models import AbstractUser


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
    is_verify_email = models.BooleanField(default=False)

    class Meta:
        verbose_name = 'Пользователя'
        verbose_name_plural = 'Пользователи'
