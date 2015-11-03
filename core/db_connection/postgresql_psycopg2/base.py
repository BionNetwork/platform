# -*- coding: utf-8 -*-

from threading import local
from django.db.backends.postgresql_psycopg2.base import (
    DatabaseError, DatabaseWrapper as BaseDatabaseWrapper, IntegrityError)
from django.db.utils import (
    DatabaseErrorWrapper, DataError, InternalError, ProgrammingError,
    NotSupportedError, InterfaceError, Error, OperationalError)
from django.utils import six
from django.utils.functional import cached_property

thread_local = local()


class PostgresDatabaseErrorWrapper(DatabaseErrorWrapper):
    """Обработка ошибки
    """

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return
        for dj_exc_type in (
                DataError,
                OperationalError,
                IntegrityError,
                InternalError,
                ProgrammingError,
                NotSupportedError,
                DatabaseError,
                InterfaceError,
                Error,
        ):
            db_exc_type = getattr(self.wrapper.Database, dj_exc_type.__name__)
            if issubclass(exc_type, db_exc_type):
                #  Добавляем код ошибки
                dj_exc_value = dj_exc_type(
                    *exc_value.args + (exc_value.pgcode,))
                dj_exc_value.__cause__ = exc_value
                if dj_exc_type not in (DataError, IntegrityError):
                    self.wrapper.errors_occurred = True
                six.reraise(dj_exc_type, dj_exc_value, traceback)


class DatabaseWrapper(BaseDatabaseWrapper):

    @cached_property
    def wrap_database_errors(self):
        return PostgresDatabaseErrorWrapper(self)

    def is_alive(self, connection):
        try:  # Check if connection is alive
            connection.cursor().execute('SELECT 1')
        except (DatabaseError, OperationalError):
            return False
        else:
            return True

    @property
    def local_key(self):
        return '{}_{}'.format(self.settings_dict.get('NAME', 'db'), 'connection')

    @property
    def local_connection(self):
        return getattr(thread_local, self.local_key, None)

    @local_connection.setter
    def local_connection(self, connection):
        setattr(thread_local, self.local_key, connection)

    def _cursor(self, *args, **kwargs):
        if self.local_connection is not None and self.connection is None:
            if self.is_alive(self.local_connection):
                self.connection = self.local_connection
            else:
                self.local_connection = None
        elif self.connection is not None:
            if self.is_alive(self.connection):
                self.local_connection = self.connection
            else:
                self.connection.close()
                self.connection = None
                self.local_connection = None
        cursor = super(DatabaseWrapper, self)._cursor(*args, **kwargs)
        if self.local_connection is None and self.connection is not None:
            self.local_connection = self.connection
        return cursor

    def close(self):
        if self.connection is not None:
            self.connection.commit()
            self.connection = None