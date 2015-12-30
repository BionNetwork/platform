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
            source: Datasource
        """
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.POSTGRESQL:
            cdc_instance = postgres.PostgresqlCdc()
        elif conn_type == ConnectionChoices.MYSQL:
            cdc_instance = mysql.MysqlCdc()
        else:
            raise ValueError("Неизвестный тип подключения!")

        cdc_instance.db_instance = DatabaseService.get_source_instance(source)

        return cdc_instance

    @classmethod
    def create_load_mechanism(cls, source, tables_info):

        cdc_instance = cls.factory(source)

        # непонятно как это обрабатывать на ошибки
        source_settings = DatasourceSettings.objects.get(
            datasource_id=source.id)

        if source_settings.value == DatasourceSettings.TRIGGERS:
            cdc_instance.apply_triggers(source, tables_info)

        elif source_settings.value == DatasourceSettings.CHECKSUM:
            cdc_instance.apply_checksum()
