# coding: utf-8
from __future__ import unicode_literals

import json
import datetime
import decimal
import time
import logging

from django.conf import settings
from django.db import OperationalError

from django.db import models, connections
from django.db.models.sql import (
    Query, UpdateQuery, DeleteQuery, InsertQuery, AggregateQuery)
from django.db.models.sql.compiler import MULTI
from redis.exceptions import LockError

logger = logging.getLogger(__name__)

"""
Хелпер настроек
"""


RETRY_ERRORS = {
    'mysql': {
        'deadlock': (['1213'], settings.DEADLOCK_WAIT_TIMEOUT),
        'db_lock': (['1205'], settings.DATABASE_WAIT_TIMEOUT)
    },
    'postgresql_psycopg2': {
        'deadlock': (['40P01'], settings.DEADLOCK_WAIT_TIMEOUT),
        'db_lock': (['55P03'], settings.DATABASE_WAIT_TIMEOUT)
    }
}


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


def get_error(using, error_code):
    """Проверка типа ошибки
    """
    db_backend = settings.DATABASES[using]['ENGINE'].split('.')[-1]
    error_code = str(error_code)
    errs = RETRY_ERRORS[db_backend]
    for key, value in errs.iteritems():
        if error_code in errs[key][0]:
            return True, errs[key][1]
    return False, 0


def retry_query(using):
    def wrap(func):
        """Перезапуск запроса
        """
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except OperationalError, e:
                lock_error_check, timeout = get_error(using, e.args[0])
                if lock_error_check:
                    for i in range(0, settings.RETRY_COUNT):
                        time.sleep(timeout)
                        try:
                            return func(*args, **kwargs)
                        except OperationalError, e:
                            if i == settings.RETRY_COUNT-1:
                                logger.exception(e.message)
                                raise
                            else:
                                continue
        return wrapper
    return wrap


class RetryQueryMixin(object):
    """Расширение функцилона Query
    """
    def clone(self, klass=None, memo=None, **kwargs):
        obj = super(RetryQueryMixin, self).clone(
            klass=klass, memo=memo, **kwargs)
        if klass:
            obj.__class__ = query_class_according[klass]
        return obj

    def get_compiler(self, using=None, connection=None):
        """получение компилятора запроса
        """
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]
        compiler = connection.ops.compiler(self.compiler)

        class RetryCompiler(compiler):
            @retry_query(using=using)
            def execute_sql(self, result_type=MULTI):
                """Переопределяем выполение запроса"""
                return super(RetryCompiler, self).execute_sql(
                    result_type=result_type)

        return RetryCompiler(self, connection, using)


class RetryInsertQueryMixin(RetryQueryMixin):

    def clone(self, klass=None, **kwargs):
        extras = {
            'fields': self.fields[:],
            'objs': self.objs[:],
            'raw': self.raw,
        }
        extras.update(kwargs)
        return super(RetryInsertQueryMixin, self).clone(klass, **extras)


class RetryQuery(RetryQueryMixin, Query):
    pass


class RetryDeleteQuery(RetryQueryMixin, DeleteQuery):
    pass


class RetryUpdateQuery(RetryQueryMixin, UpdateQuery):
    pass


class RetryInsertQuery(RetryInsertQueryMixin, InsertQuery):
    pass


class RetryAggregateQuery(RetryQueryMixin, AggregateQuery):
    pass

# соответсвие классов запроса в базу ORM
query_class_according = {
    DeleteQuery: RetryDeleteQuery,
    UpdateQuery: RetryUpdateQuery,
    InsertQuery: RetryInsertQuery,
    AggregateQuery: RetryDeleteQuery
}


class RetryQueryset(models.QuerySet):

    def __init__(self, model=None, query=None, using=None, hints=None):
        super(RetryQueryset, self).__init__(
            model=model, query=query, using=using, hints=hints)
        self.query = query or RetryQuery(self.model)


def check_redis_lock(func):
    """Повторный запрос при блокировке в редис"""
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except LockError:
            for i in range(0, settings.RETRY_COUNT):
                time.sleep(settings.REDIS_LOCK_TIMEOUT)
                try:
                    return func(*args, **kwargs)
                except LockError, e:
                    if i == settings.RETRY_COUNT-1:
                        logger.exception(e.message)
                        raise
                    else:
                        continue
    return wrap

