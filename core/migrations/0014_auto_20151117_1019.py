# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0013_auto_20151112_1119'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dimension',
            name='create_date',
            field=models.DateTimeField(auto_now_add=True, verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', db_index=True),
        ),
        migrations.AlterField(
            model_name='dimension',
            name='datasources_meta',
            field=models.ForeignKey(related_name='dimension', to='core.DatasourceMeta'),
        ),
        migrations.AlterField(
            model_name='dimension',
            name='type',
            field=models.CharField(default='SD', max_length=255, verbose_name='\u0442\u0438\u043f \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f', choices=[('SD', 'StandardDimension'), ('SD', 'TimeDimension')]),
        ),
        migrations.AlterField(
            model_name='dimension',
            name='update_date',
            field=models.DateTimeField(auto_now=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', db_index=True),
        ),
        migrations.AlterField(
            model_name='dimension',
            name='user',
            field=models.ForeignKey(verbose_name='\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c', to=settings.AUTH_USER_MODEL),
        ),
    ]
