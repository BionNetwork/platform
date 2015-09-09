# coding: utf-8
from __future__ import unicode_literals

from django.contrib.sessions.models import Session
from django.test import TestCase, Client
from django.core.urlresolvers import reverse
from core.models import User


class AuthenticationTest(TestCase):

    def setUp(self):
        self.client = Client()

    def test_login(self):
        user = User.objects.get()  # пароль 'test'
        sessions = Session.objects.all()

        self.assertEqual(sessions.count(), 0, 'Количество сессий непусто!')

        self.client.post(
            reverse('core:login'),
            {'username': user.username, 'password': 'test'}
        )
        self.assertEqual(sessions.count(), 1, 'Количество сессий неравно 1!')

    def test_registration(self):

        users = User.objects.all()

        self.assertEqual(users.count(), 1, 'Количество пользователей не 1!')

        self.client.post(
            reverse('core:registration'),
            {'login': 'NewLog', 'email': 'email@mail.ru', 'password': 'newpass'}
        )
        self.assertEqual(users.count(), 2, 'Количество пользователей не 2!')
