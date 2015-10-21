# coding: utf-8

import os
import sys
import brukva
import time

from django.conf import settings

from djcelery import celery

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

client = brukva.Client(host=settings.REDIS_HOST,
                       port=int(settings.REDIS_PORT),
                       selected_db=settings.REDIS_DB)
client.connect()


@celery.task(name='etl.tasks.load_data')
def load_data(user_id, task_id):
    for i in range(1, 11):
        client.publish('jobs:etl:extract:{0}:{1}'.format(user_id, task_id), i*10)
        time.sleep(3)

# write in console: python manage.py celery -A etl.tasks worker
