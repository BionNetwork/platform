# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0036_auto_20160406_1644'),
    ]

    operations = [
        migrations.CreateModel(
            name='Columns',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435')),
                ('original_name', models.CharField(max_length=255, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0432 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0435')),
                ('type', models.CharField(default=1, max_length=20, verbose_name='\u0422\u0438\u043f', choices=[(1, 'string')])),
                ('format_string', models.CharField(max_length=20, verbose_name='\u0440\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u044c')),
                ('visible', models.BooleanField(default=True)),
                ('date_created', models.DateTimeField(auto_now_add=True, verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f')),
                ('date_updated', models.DateTimeField(auto_now=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f')),
                ('dataset', models.ForeignKey(verbose_name='\u0425\u0440\u0430\u043d\u0438\u043b\u0438\u0449\u0435', to='core.Dataset')),
                ('source', models.ForeignKey(verbose_name='\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a', to='core.Datasource')),
            ],
            options={
                'db_table': 'columns',
            },
        ),
        migrations.AlterField(
            model_name='datasourcesettings',
            name='value',
            field=models.CharField(max_length=255, verbose_name='\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435'),
        ),
    ]
