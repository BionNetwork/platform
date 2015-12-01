# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0018_auto_20151127_1619'),
    ]

    operations = [
        migrations.AlterField(
            model_name='measure',
            name='aggregator',
            field=models.CharField(max_length=50, null=True, verbose_name='\u0424\u0443\u043d\u043a\u0446\u0438\u044f \u0430\u0433\u0440\u0435\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f', choices=[('sum', 'sum')]),
        ),
    ]
