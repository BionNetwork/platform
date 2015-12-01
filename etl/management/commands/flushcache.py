# coding: utf-8
__author__ = 'miholeus'

from django.core.management.base import BaseCommand
from django.conf import settings

import storage


class Command(BaseCommand):
    args = '<>'
    help = "Сбрасывание кеша"

    def handle(self, *args, **options):
        r_server = storage.StrictRedis(
            host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
        r_server.flushdb()
        print 'Cleaned cache!'
