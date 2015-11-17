# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0012_auto_20151111_1744'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datasourcemeta',
            name='update_date',
            field=models.DateTimeField(auto_now=True, help_text='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', verbose_name='update_date', db_index=True),
        ),
    ]
