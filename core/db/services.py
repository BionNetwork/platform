# -*- coding: utf-8 -*-
import time
import logging

from django.conf import settings

from django.db import OperationalError
from django.db import models, connections
from django.db.models.sql import (
    Query, UpdateQuery, DeleteQuery, InsertQuery, AggregateQuery)
from django.db.models.sql.compiler import MULTI

from core.helpers import convert_milliseconds_to_seconds

logger = logging.getLogger(__name__)


RETRY_ERRORS = {
    'mysql': {
        'deadlock': (['1213'], convert_milliseconds_to_seconds(
            settings.DEADLOCK_WAIT_TIMEOUT)),
        'db_lock': (['1205'], convert_milliseconds_to_seconds(
            settings.DATABASE_WAIT_TIMEOUT))
    },
    'postgresql_psycopg2': {
        'deadlock': (['40P01'], convert_milliseconds_to_seconds(
            settings.DEADLOCK_WAIT_TIMEOUT)),
        'db_lock': (['55P03'], convert_milliseconds_to_seconds(
            settings.DATABASE_WAIT_TIMEOUT))
    }
}


def get_error(using, error_code):
    """Проверка типа ошибки

    Args:
        error_code (str): Код ошибки
        using (str): Ключ базы данных

    Returns:
        {
            bool: True если ошибка найдена, иначе False
            int: Время задержки, с
        }
    """
    db_backend = settings.DATABASES[using]['ENGINE'].split('.')[-1]
    errs = RETRY_ERRORS[db_backend]
    for key, value in errs.iteritems():
        if error_code in errs[key][0]:
            return True, errs[key][1]
    return False, 0


def retry_query(using):
    """Перезапуск запроса, если схватили ошибку блокировки или deadlock'а

    Args:
        using (str): Ключ базы данных
    """
    def wrap(func):

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except OperationalError, e:

                lock_error_check, timeout = get_error(using, str(e.args[0]))
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
