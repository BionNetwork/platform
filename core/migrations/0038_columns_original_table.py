# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0037_auto_20160825_1453'),
    ]

    operations = [
        migrations.AddField(
            model_name='columns',
            name='original_table',
            field=models.CharField(default='', max_length=255, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0442\u0430\u0431\u043b\u0438\u0446\u044b \u0432 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0435'),
        ),
    ]
