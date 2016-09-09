# -*- coding: utf-8 -*-


from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0010_auto_20151109_1418'),
    ]

    operations = [
        migrations.CreateModel(
            name='Dimension',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255, verbose_name='\u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f', db_index=True)),
                ('title', models.CharField(max_length=255, verbose_name='\u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435')),
                ('type', models.CharField(max_length=255, verbose_name='\u0442\u0438\u043f \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f')),
                ('visible', models.BooleanField(default=True, verbose_name='\u0432\u0438\u0434\u0435\u043d')),
                ('high_cardinality', models.BooleanField(default=False, verbose_name='cardinality')),
                ('data', models.TextField(null=True, verbose_name='\u0438\u0435\u0440\u0430\u0440\u0445\u0438\u0438', blank=True)),
                ('create_date', models.DateTimeField(verbose_name='\u0434\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', db_index=True)),
                ('update_date', models.DateTimeField(verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', db_index=True)),
                ('datasources_meta', models.ForeignKey(to='core.DatasourceMeta')),
                ('user', models.ForeignKey(to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'dimensions',
                'verbose_name': '\u0420\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u0438',
                'verbose_name_plural': '\u0420\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u0438',
            },
        ),
    ]
