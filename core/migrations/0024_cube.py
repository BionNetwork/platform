# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.db.models.fields
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0023_auto_20151229_1231'),
    ]

    operations = [
        migrations.CreateModel(
            name='Cube',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024, verbose_name='\u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043a\u0443\u0431\u0430')),
                ('data', core.db.models.fields.XmlField(verbose_name='xml \u0441\u0445\u0435\u043c\u0430 \u043a\u0443\u0431\u0430')),
                ('create_date', models.DateTimeField(auto_now_add=True, verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', db_index=True)),
                ('update_date', models.DateTimeField(auto_now=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', db_index=True)),
                ('user', models.ForeignKey(verbose_name='\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'cubes',
            },
        ),
    ]
