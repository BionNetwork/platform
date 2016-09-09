# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0035_auto_20160406_1154'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datasource',
            name='conn_type',
            field=models.SmallIntegerField(default=1, verbose_name='\u0422\u0438\u043f \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f', choices=[(1, 'Postgresql'), (2, 'Mysql'), (3, 'MsSql'), (4, 'Oracle'), (5, 'Excel'), (6, 'Csv'), (7, 'Text')]),
        ),
    ]
