# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0030_auto_20160301_1450'),
    ]

    operations = [
        migrations.AddField(
            model_name='cube',
            name='structure',
            field=models.TextField(default='', verbose_name='\u0421\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0430 \u0434\u0435\u0440\u0435\u0432\u0430 \u0438 \u0434\u0436\u043e\u0439\u043d\u043e\u0432'),
        ),
    ]
