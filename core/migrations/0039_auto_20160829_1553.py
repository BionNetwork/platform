# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0038_columns_original_table'),
    ]

    operations = [
        migrations.AlterField(
            model_name='columns',
            name='type',
            field=models.CharField(default=1, max_length=20, verbose_name='\u0422\u0438\u043f', choices=[(1, 'text'), (2, 'integer'), (3, 'double precision'), (4, 'timestamp'), (5, 'bool')]),
        ),
    ]
