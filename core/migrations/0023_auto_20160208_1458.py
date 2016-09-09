# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0022_datasourcesettings'),
    ]

    operations = [
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
