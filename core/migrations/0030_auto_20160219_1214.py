# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0029_merge'),
    ]

    operations = [
        migrations.AlterField(
            model_name='user',
            name='avatar',
            field=models.ImageField(max_length=500, upload_to=core.helpers.users_avatar_upload_path, null=True, verbose_name='\u0410\u0432\u0430\u0442\u0430\u0440', blank=True),
        ),
        migrations.AlterField(
            model_name='user',
            name='avatar_small',
            field=models.ImageField(max_length=500, upload_to=core.helpers.users_avatar_upload_path, null=True, verbose_name='\u0410\u0432\u0430\u0442\u0430\u0440 preview', blank=True),
        ),
    ]
