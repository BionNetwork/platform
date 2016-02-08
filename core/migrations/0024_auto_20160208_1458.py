# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0023_auto_20160120_1533'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='user',
            name='big_image',
        ),
        migrations.RemoveField(
            model_name='user',
            name='small_image',
        ),
        migrations.AddField(
            model_name='user',
            name='avatar',
            field=models.ImageField(max_length=500, upload_to='users', null=True, verbose_name='\u0410\u0432\u0430\u0442\u0430\u0440', blank=True),
        ),
        migrations.AddField(
            model_name='user',
            name='avatar_small',
            field=models.ImageField(max_length=500, upload_to='users', null=True, verbose_name='\u0410\u0432\u0430\u0442\u0430\u0440 preview', blank=True),
        ),
    ]
