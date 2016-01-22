# coding: utf-8
from django.core.management.base import BaseCommand
from core.models import Queue
from etl.constants import *

# Список всех актуальных имен очередей
queue_names = [MONGODB_DATA_LOAD, DB_DATA_LOAD, MONGODB_DELTA_LOAD,
               DB_DETECT_REDUNDANT, DB_DELETE_REDUNDANT, GENERATE_DIMENSIONS,
               GENERATE_MEASURES, CREATE_TRIGGERS, CREATE_DATASET]


class Command(BaseCommand):
    """
    Добавляет все недостающие имена очередей
    """
    args = '<>'
    help = "Add new queue names"

    def handle(self, *args, **options):
        current_queue_names = Queue.objects.all().values_list('name', flat=True)
        for each in queue_names:
            if each not in current_queue_names:
                Queue.objects.create(name=each)
        print 'Finish!'
