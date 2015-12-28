# coding: utf-8

from core.models import ConnectionChoices, DatasourceSettings
from etl.services.cdc.datasource import postgres, mysql
from etl.services.db.factory import DatabaseService


class CdcFactroy(object):
    """Фабрика для докачки
    Реализация для инстансов разных методик по докачке
    а) на основе триггеров
    б) на основе рассчета контрольных сумм для строк
    """

    @staticmethod
    def factory(source):
        """
        фабрика для инстанса докачки

        Args:
            source(`Datasource`): Объект источника
        """
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.POSTGRESQL:
            cdc_class = postgres.PostgresqlCdc
        elif conn_type == ConnectionChoices.MYSQL:
            cdc_class = mysql.MysqlCdc
        else:
            raise ValueError("Неизвестный тип подключения!")

        return cdc_class
