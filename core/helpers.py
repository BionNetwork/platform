# coding: utf-8
from __future__ import unicode_literals

import json
import datetime
import decimal
import time
import logging

from django.conf import settings

from redis.exceptions import LockError

logger = logging.getLogger(__name__)

"""
Хелпер настроек
"""


def convert_milliseconds_to_seconds(value):
    """миллисекунды -> секунды

    Args:
        value(int): миллисекундах

    Returns:
        float: секунды
    """
    return float(value)/1000


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
        elif isinstance(obj, buffer):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def check_redis_lock(func):
    """Повторный запрос при блокировке в редис"""
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except LockError:
            for i in range(0, settings.RETRY_COUNT):
                time.sleep(convert_milliseconds_to_seconds(
                    settings.REDIS_LOCK_TIMEOUT))
                try:
                    return func(*args, **kwargs)
                except LockError, e:
                    if i == settings.RETRY_COUNT-1:
                        logger.exception(e.message)
                        raise
                    else:
                        continue
    return wrap

