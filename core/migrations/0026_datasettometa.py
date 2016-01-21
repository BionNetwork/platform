# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import core.model_helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0025_dataset'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasetToMeta',
            fields=[
                ('dataset', models.ForeignKey(primary_key=True, verbose_name='\u0414\u0430\u043d\u043d\u044b\u0435', serialize=False, to='core.Dataset')),
                ('meta', models.ForeignKey(verbose_name='\u041c\u0435\u0442\u0430 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430', to='core.DatasourceMeta')),
            ],
            options={
                'db_table': 'datasets_to_meta',
            },
            bases=(models.Model, core.model_helpers.MultiPrimaryKeyModel),
        ),
    ]
