# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Datasource',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='\u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435', max_length=255)),
                ('host', models.CharField(help_text='\u0438\u043c\u044f \u0445\u043e\u0441\u0442\u0430', max_length=255, db_index=True)),
                ('port', models.IntegerField(help_text='\u041f\u043e\u0440\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f')),
                ('login', models.CharField(help_text='\u043b\u043e\u0433\u0438\u043d', max_length=1024, null=True)),
                ('password', models.CharField(help_text='\u043f\u0430\u0440\u043e\u043b\u044c', max_length=255, null=True)),
                ('create_date', models.DateTimeField(auto_now_add=True, help_text='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', verbose_name='create_date', db_index=True)),
                ('user_id', models.IntegerField(help_text='\u0438\u0434\u0435\u043d\u0442\u0438\u0444\u0438\u043a\u0430\u0442\u043e\u0440 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f')),
            ],
            options={
                'db_table': 'datasources',
            },
        ),
        migrations.CreateModel(
            name='DatasourceMeta',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('database_name', models.CharField(help_text='\u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0431\u0430\u0437\u044b', max_length=255, db_index=True)),
                ('collection_name', models.CharField(help_text='\u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043a\u043e\u043b\u043b\u0435\u043a\u0446\u0438\u0438', max_length=255)),
                ('fields', models.TextField(help_text='\u043c\u0435\u0442\u0430-\u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044f \u043f\u043e\u043b\u0435\u0439')),
                ('stats', models.TextField(help_text='\u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430', null=True)),
                ('create_date', models.DateTimeField(auto_now_add=True, help_text='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', verbose_name='create_date', db_index=True)),
                ('update_date', models.DateTimeField(help_text='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', verbose_name='update_date', db_index=True)),
                ('datasource', models.ForeignKey(to='core.Datasource')),
            ],
            options={
                'db_table': 'datasources_meta',
            },
        ),
        migrations.AlterModelOptions(
            name='user',
            options={'verbose_name': '\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c', 'verbose_name_plural': '\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438'},
        ),
    ]
