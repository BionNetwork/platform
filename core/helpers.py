# coding: utf-8
from __future__ import unicode_literals

__author__ = 'damir'

import json
import datetime
import decimal
import time

from django.conf import settings
from django.db import OperationalError

"""
Хелпер настроек
"""


class Settings:
    @classmethod
    def get_host(cls, request):
        host = request.get_host()
        if request.is_secure():
            protocol = 'https'
        else:
            protocol = 'http'

        return "%s://%s" % (protocol, host)


def get_utf8_string(value):
    """
    Кодирование в utf-8 строки
    :param value: string
    :return: string
    """
    return unicode(value)


class CustomJsonEncoder(json.JSONEncoder):
    """
        Свой JsonEncoder заэнкодит все что угодно
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%d.%m.%Y')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%d.%m.%Y')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


def retry_query(func):
    """Перезапуск запроса"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except OperationalError, e:
            print 'exc'
            for i in range(0, settings.RETRY_COUNT):
                print 'step'
                time.sleep(settings.WAIT_TIMEOUT)
                try:
                    return func(*args, **kwargs)
                except OperationalError, e:
                    if i == settings.RETRY_COUNT-1:
                        raise OperationalError()
                    else:
                        continue
    return wrapper
