# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0031_auto_20160301_1450'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasourcesTrigger',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435', db_index=True)),
                ('src', models.TextField(verbose_name='\u0442\u0435\u043a\u0441\u0442 \u0442\u0440\u0438\u0433\u0433\u0435\u0440\u0430')),
                ('collection_name', models.CharField(max_length=1024, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043a\u043e\u043b\u043b\u0435\u043a\u0446\u0438\u0438', db_index=True)),
                ('datasource', models.ForeignKey(verbose_name='\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a', to='core.Datasource')),
            ],
            options={
                'db_table': 'datasources_trigger',
            },
        ),
    ]
