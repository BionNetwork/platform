# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


def update_date_in_queue_list(apps, schema_editor):
    """
    Обновляем поле update_date для пустых значений датой создания
    """
    QueueList = apps.get_model('core', 'QueueList')
    for record in QueueList.objects.filter(update_date__isnull=True):
        create_date = record.date_created
        record.update_date = create_date
        record.save()


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0019_auto_20151130_1643'),
    ]

    operations = [
        migrations.RunPython(update_date_in_queue_list),
        migrations.AlterField(
            model_name='queuelist',
            name='update_date',
            field=models.DateTimeField(auto_now=True, verbose_name='\u0434\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f'),
        ),
    ]

    def unapply(self, project_state, schema_editor, collect_sql=False):
        pass