# coding: utf-8
__author__ = 'damir'

from django.conf import settings

from core.models import ConnectionChoices
from etl.services.db import mysql, postgresql


class FileService(object):
    """Сервис для источников данных на основе файлов"""

    @staticmethod
    def factory(**connection):
        """
        фабрика для инстанса бд

        Args:
            **connection(dict): словарь с информацией о подключении

        Returns:
            etl.services.db.interfaces.Database
        """
        conn_type = int(connection.get('conn_type', ''))
        del connection['conn_type']

        if conn_type == ConnectionChoices.POSTGRESQL:
            return postgresql.Postgresql(connection)
        elif conn_type == ConnectionChoices.MYSQL:
            return mysql.Mysql(connection)
        elif conn_type == ConnectionChoices.MS_SQL:
            import mssql
            return mssql.MsSql(connection)
        elif conn_type == ConnectionChoices.ORACLE:
            import oracle
            return oracle.Oracle(connection)
        else:
            raise ValueError("Неизвестный тип подключения!")