# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0004_auto_20150903_1743'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='user',
            name='is_verify_email',
        ),
        migrations.AddField(
            model_name='user',
            name='verify_email_uuid',
            field=models.CharField(max_length=50, null=True, blank=True),
        ),
    ]
