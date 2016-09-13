# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0003_auto_20150903_1742'),
    ]

    operations = [
        migrations.AlterModelTable(
            name='user',
            table='users',
        ),
    ]
