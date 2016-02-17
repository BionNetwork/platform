# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0022_datasourcesettings'),
    ]

    operations = [
        migrations.AlterField(
            model_name='measure',
            name='type',
            field=models.CharField(default='integer', max_length=50, verbose_name='\u0422\u0438\u043f \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f', choices=[('string', 'string'), ('integer', 'integer'), ('numeric', 'numeric'), ('boolean', 'boolean'), ('date', 'date'), ('time', 'time'), ('timestamp', 'timestamp'), ('bytea', 'bytea')]),
        ),
    ]
