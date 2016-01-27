# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from etl.constants import *

# Список всех актуальных имен очередей к данному моменту
queue_names = [MONGODB_DATA_LOAD, DB_DATA_LOAD, MONGODB_DELTA_LOAD,
               DB_DETECT_REDUNDANT, DB_DELETE_REDUNDANT, GENERATE_DIMENSIONS,
               GENERATE_MEASURES, CREATE_TRIGGERS, CREATE_DATASET]


def update_task_names(apps, schema_editor):
    Queue = apps.get_model('core', 'Queue')
    current_queue_names = Queue.objects.all().values_list('name', flat=True)
    for each in queue_names:
        if each not in current_queue_names:
            Queue.objects.create(name=each)


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0027_auto_20160114_1046'),
    ]

    operations = [
        migrations.RunPython(update_task_names),
    ]

    def unapply(self, project_state, schema_editor, collect_sql=False):
        pass
