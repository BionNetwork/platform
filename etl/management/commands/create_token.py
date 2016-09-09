# coding: utf-8

__author__ = 'damir(GDR)'

from django.core.management.base import BaseCommand
from rest_framework.authtoken.models import Token


class Command(BaseCommand):

    def handle(self, *args, **options):

        token, created = Token.objects.get_or_create(user_id=1)
        print(token.key)
