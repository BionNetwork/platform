# -*- coding: utf-8 -*-


from django.db import models, migrations
from django.core.management import call_command


def loadfixture(apps, schema_editor):
    call_command('loaddata', 'queue_data.json')


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0015_auto_20151125_1215'),
    ]

    operations = [
        # migrations.RunPython(loadfixture)
    ]

    def unapply(self, project_state, schema_editor, collect_sql=False):
        pass
