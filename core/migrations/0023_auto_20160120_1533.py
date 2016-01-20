# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0022_datasourcesettings'),
    ]

    operations = [
        migrations.AddField(
            model_name='user',
            name='big_image',
            field=models.ImageField(upload_to='users', null=True, verbose_name='\u0410\u0432\u0430\u0442\u0430\u0440\u043a\u0430', blank=True),
        ),
        migrations.AddField(
            model_name='user',
            name='small_image',
            field=models.ImageField(upload_to='users', null=True, verbose_name='\u0410\u0432\u0430\u0442\u0430\u0440\u043a\u0430', blank=True),
        ),
    ]
