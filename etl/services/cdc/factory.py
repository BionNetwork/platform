# coding: utf-8

from core.models import ConnectionChoices
from etl.services.cdc.datasource import postgres, mysql


class CdcFactroy(object):
    """
    Фабрика для докачки
    """

    @staticmethod
    def factory(source):
        """
        фабрика для инстанса докачки

        Args:
            source(`Datasource`): Объект источника

        Returns:
            `TaskProcessing`: Класс реализации докачки с помощью триггеров для
            различных типов баз данных
        """
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.POSTGRESQL:
            cdc_class = postgres.PostgresqlTriggerCdc
        elif conn_type == ConnectionChoices.MYSQL:
            cdc_class = mysql.MysqlTriggerCdc
        else:
            raise ValueError("Неизвестный тип подключения!")

        return cdc_class
