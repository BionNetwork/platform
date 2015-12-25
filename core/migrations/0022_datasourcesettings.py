# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0021_auto_20151211_1303'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasourceSettings',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435', db_index=True)),
                ('value', models.TextField(verbose_name='\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435')),
                ('datasource', models.ForeignKey(verbose_name='\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a', to='core.Datasource')),
            ],
            options={
                'db_table': 'datasources_settings',
            },
        ),
    ]
