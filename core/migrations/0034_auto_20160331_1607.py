# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0033_auto_20160225_1527'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dimension',
            name='type',
            field=models.CharField(default='SD', max_length=255, verbose_name='\u0442\u0438\u043f \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f', choices=[('SD', 'OTHER'), ('TD', 'TIME')]),
        ),
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
