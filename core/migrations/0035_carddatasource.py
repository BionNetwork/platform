# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('card', '0001_initial'),
        ('core', '0034_auto_20160331_1607'),
    ]

    operations = [
        migrations.CreateModel(
            name='CardDatasource',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('card', models.ForeignKey(related_name='card_datasource', verbose_name='\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430', to='card.Card')),
                ('source', models.ForeignKey(related_name='source', verbose_name='\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a', to='core.Datasource')),
            ],
        ),
    ]
