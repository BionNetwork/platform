# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0006_auto_20150909_1320'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasource',
            name='conn_type',
            field=models.SmallIntegerField(default=1, verbose_name='\u0422\u0438\u043f \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f', choices=[(1, 'Postgresql'), (2, 'Mysql')]),
        ),
    ]
