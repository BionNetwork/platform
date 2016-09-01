# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0039_auto_20160829_1553'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='datasource',
            unique_together=set([]),
        ),
    ]
