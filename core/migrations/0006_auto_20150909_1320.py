# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.core.management import call_command


def loadfixture(apps, schema_editor):
    call_command('loaddata', 'initial_data.json')


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0005_auto_20150903_1941'),
        ('core', '0023_auto_20160208_1458'),
    ]

    operations = [
        migrations.RunPython(loadfixture)
    ]

    def unapply(self, project_state, schema_editor, collect_sql=False):
        pass
