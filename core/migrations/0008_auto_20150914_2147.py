# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0007_datasource_conn_type'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasource',
            name='db',
            field=models.CharField(default='test', help_text='\u0411\u0430\u0437\u0430 \u0434\u0430\u043d\u043d\u044b\u0445', max_length=255),
            preserve_default=False,
        ),
        migrations.AlterUniqueTogether(
            name='datasource',
            unique_together=set([('host', 'db')]),
        ),
        migrations.RemoveField(
            model_name='datasource',
            name='name',
        ),
    ]
