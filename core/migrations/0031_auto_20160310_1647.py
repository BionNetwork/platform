# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0030_auto_20160303_1027'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datasourcesettings',
            name='datasource',
            field=models.ForeignKey(related_name='settings', verbose_name='\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a', to='core.Datasource'),
        ),
    ]
