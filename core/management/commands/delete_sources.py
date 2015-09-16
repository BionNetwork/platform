# coding: utf-8

from django.core.management.base import BaseCommand, CommandError
from core.models import Datasource

class Command(BaseCommand):
    args = '<>'

    def handle(self, *args, **options):
        print args
        a, b = args[0].split('=')
        d = {a: b}
        sources = Datasource.objects.filter(**d)
        print sources
        sources.delete()
