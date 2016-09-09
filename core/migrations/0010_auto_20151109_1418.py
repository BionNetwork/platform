# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0009_remove_datasourcemeta_database_name'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='datasource',
            unique_together=set([('host', 'db', 'user_id')]),
        ),
    ]
