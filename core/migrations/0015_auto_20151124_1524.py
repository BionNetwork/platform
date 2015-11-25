# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0014_auto_20151117_1019'),
    ]

    operations = [
        migrations.CreateModel(
            name='Measure',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043c\u0435\u0440\u044b', db_index=True)),
                ('title', models.CharField(max_length=255, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435')),
                ('type', models.SmallIntegerField(default=1, verbose_name='\u0422\u0438\u043f \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f', choices=[(1, 'String'), (2, 'Integer'), (3, 'Numeric'), (4, 'Boolean'), (5, 'Date'), (6, 'Time'), (7, 'Timestamp')])),
                ('aggregator', models.SmallIntegerField(default=1, verbose_name='\u0424\u0443\u043d\u043a\u0446\u0438\u044f \u0430\u0433\u0440\u0435\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f', choices=[(1, 'sum')])),
                ('format_string', models.CharField(max_length=255, null=True, verbose_name='\u0421\u0442\u0440\u043e\u043a\u0430 \u0444\u043e\u0440\u043c\u0430\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f', blank=True)),
                ('visible', models.BooleanField(default=True, verbose_name='\u0412\u0438\u0434\u0435\u043d')),
                ('create_date', models.DateTimeField(auto_now_add=True, verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', db_index=True)),
                ('update_date', models.DateTimeField(auto_now=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', db_index=True)),
                ('datasources_meta', models.ForeignKey(related_name='measure', to='core.DatasourceMeta')),
                ('user', models.ForeignKey(verbose_name='\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c', to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AlterModelOptions(
            name='dimension',
            options={'verbose_name': '\u0420\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u044c', 'verbose_name_plural': '\u0420\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u0438'},
        ),
    ]
