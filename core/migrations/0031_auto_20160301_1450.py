# -*- coding: utf-8 -*-


from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0030_auto_20160219_1214'),
    ]

    operations = [
        migrations.AddField(
            model_name='cube',
            name='dataset',
            field=models.ForeignKey(default=1, verbose_name='\u0414\u0430\u0442\u0430\u0441\u0435\u0442', to='core.Dataset'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='datasourcesettings',
            name='datasource',
            field=models.ForeignKey(related_name='settings', verbose_name='\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a', to='core.Datasource'),
        ),
    ]
