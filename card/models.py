# coding: utf-8
from django.db import models


class Card(models.Model):
    """
    Карточка
    """
    creator = models.ForeignKey(
        'core.User', verbose_name=u'Создатель карточки', related_name='creator')


class UserCard(models.Model):
    """
    Карточка пользователя
    """

    card = models.ForeignKey(
        'card.Card', verbose_name=u'Карточка', related_name='card')
    user = models.ForeignKey(
        'core.User', verbose_name=u'Пользователь', related_name='user')
