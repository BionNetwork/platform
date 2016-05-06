# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Card',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('creator', models.ForeignKey(related_name='creator', verbose_name='\u0421\u043e\u0437\u0434\u0430\u0442\u0435\u043b\u044c \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438', to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name='UserCard',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('card', models.ForeignKey(related_name='card', verbose_name='\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430', to='card.Card')),
                ('user', models.ForeignKey(related_name='user', verbose_name='\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c', to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]
