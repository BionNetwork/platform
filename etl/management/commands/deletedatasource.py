# coding: utf-8

from django.core.management.base import BaseCommand
from core.models import Datasource

import json


class Command(BaseCommand):
    args = '<>'
    help = "Удаление источника данных"

    def handle(self, *args, **options):
        print("Searching datasources by args: %s" % json.dumps(args))
        filter_cond = dict()
        values = (arg.split('=') for arg in args if len(arg.split('=')) > 1)

        for value in values:
            filter_cond[value[0]] = value[1]
        delete = True
        ask_deletion = None

        if len(filter_cond) == 0:
            while ask_deletion not in ['Y', 'n']:
                ask_deletion = input("Вы действительно хотите удалить все источники? (Y/n) ")
                delete = ask_deletion == 'Y'

        if not delete:
            return False
        sources = Datasource.objects.filter(**filter_cond)

        if sources.exists():
            for source in sources:
                source.delete()
                print('Deleted datasource %d' % source['id'])
        else:
            print('No datasources found by condition')

