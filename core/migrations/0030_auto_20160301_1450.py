# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0029_merge'),
    ]

    operations = [
        migrations.AddField(
            model_name='cube',
            name='dataset',
            field=models.ForeignKey(default=1, verbose_name='\u0414\u0430\u0442\u0430\u0441\u0435\u0442', to='core.Dataset'),
            preserve_default=False,
        ),
    ]
