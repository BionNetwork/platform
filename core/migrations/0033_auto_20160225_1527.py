# -*- coding: utf-8 -*-


from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0032_auto_20160224_1122'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasourcesJournal',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0442\u0430\u0431\u043b\u0438\u0446\u044b \u0442\u0440\u0438\u0433\u0433\u0435\u0440\u0430 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430', db_index=True)),
                ('collection_name', models.CharField(max_length=1024, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043a\u043e\u043b\u043b\u0435\u043a\u0446\u0438\u0438', db_index=True)),
                ('date_created', models.DateTimeField(auto_now_add=True, verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f')),
                ('date_updated', models.DateTimeField(auto_now=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f')),
                ('rows_read', models.IntegerField(default=0, verbose_name='\u0421\u0447\u0438\u0442\u0430\u043d\u043e')),
                ('rows_written', models.IntegerField(default=0, verbose_name='\u0417\u0430\u043f\u0438\u0441\u0430\u043d\u043e')),
                ('trigger', models.ForeignKey(verbose_name='\u0422\u0440\u0438\u0433\u0433\u0435\u0440', to='core.DatasourcesTrigger')),
            ],
            options={
                'db_table': 'datasources_journal',
            },
        ),
    ]
