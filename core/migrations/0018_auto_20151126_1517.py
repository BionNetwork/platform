# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0017_measure'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='datasourcemetakeys',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='datasourcemetakeys',
            name='value',
        ),
        migrations.AddField(
            model_name='datasourcemetakeys',
            name='value',
            field=models.IntegerField(default=0, verbose_name='\u041a\u043b\u044e\u0447'),
            preserve_default=False,
        ),
        migrations.AlterUniqueTogether(
            name='datasourcemetakeys',
            unique_together=set([('meta', 'value')]),
        ),
    ]
