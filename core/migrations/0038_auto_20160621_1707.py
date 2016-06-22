# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0037_merge'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='carddatasource',
            name='card',
        ),
        migrations.RemoveField(
            model_name='carddatasource',
            name='source',
        ),
        migrations.AlterField(
            model_name='datasourcesettings',
            name='value',
            field=models.CharField(max_length=255, verbose_name='\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435'),
        ),
        migrations.DeleteModel(
            name='CardDatasource',
        ),
    ]
