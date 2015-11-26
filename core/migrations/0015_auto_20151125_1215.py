# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0014_auto_20151117_1019'),
    ]

    operations = [
        migrations.CreateModel(
            name='Queue',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=50, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435')),
                ('interval', models.IntegerField(null=True, verbose_name='\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b', blank=True)),
                ('is_active', models.BooleanField(default=True, verbose_name='\u0410\u043a\u0442\u0438\u0432\u0435\u043d')),
            ],
            options={
                'db_table': 'queue',
            },
        ),
        migrations.CreateModel(
            name='QueueList',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('arguments', models.TextField(verbose_name='\u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b \u0437\u0430\u043f\u0443\u0441\u043a\u0430 \u0437\u0430\u0434\u0430\u0447\u0438')),
                ('app', models.CharField(max_length=50, verbose_name='\u043c\u043e\u0434\u0443\u043b\u044c/\u043f\u0440\u0438\u043b\u043e\u0436\u0435\u043d\u0438\u0435')),
                ('date_created', models.DateTimeField(auto_now_add=True, verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', db_index=True)),
                ('update_date', models.DateTimeField(null=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', blank=True)),
                ('comment', models.CharField(max_length=1024, null=True, verbose_name='\u043a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439', blank=True)),
                ('checksum', models.CharField(max_length=255, verbose_name='\u043a\u043e\u043d\u0442\u0440\u043e\u043b\u044c\u043d\u0430\u044f \u0441\u0443\u043c\u043c\u0430', db_index=True)),
                ('percent', models.FloatField(null=True, verbose_name='\u043f\u0440\u043e\u0446\u0435\u043d\u0442 \u0432\u044b\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f \u0437\u0430\u0434\u0430\u0447\u0438', blank=True)),
                ('queue', models.ForeignKey(verbose_name='\u0438\u0434 \u043e\u0447\u0435\u0440\u0435\u0434\u0438', to='core.Queue')),
            ],
            options={
                'db_table': 'queue_list',
            },
        ),
        migrations.CreateModel(
            name='QueueStatus',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('title', models.CharField(max_length=50, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435')),
                ('description', models.CharField(max_length=50, null=True, verbose_name='\u041e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u0435', blank=True)),
            ],
            options={
                'db_table': 'queue_status',
            },
        ),
        migrations.AlterModelOptions(
            name='dimension',
            options={'verbose_name': '\u0420\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u044c', 'verbose_name_plural': '\u0420\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u0438'},
        ),
        migrations.AddField(
            model_name='queuelist',
            name='queue_status',
            field=models.ForeignKey(verbose_name='\u0441\u0442\u0430\u0442\u0443\u0441 \u043e\u0447\u0435\u0440\u0435\u0434\u0438', to='core.QueueStatus'),
        ),
        migrations.AlterIndexTogether(
            name='queuelist',
            index_together=set([('queue', 'date_created', 'queue_status')]),
        ),
    ]
