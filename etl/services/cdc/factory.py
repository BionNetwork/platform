# coding: utf-8

from core.models import ConnectionChoices, DatasourceSettings
from etl.services.cdc.datasource import postgres, mysql
from etl.services.datasource.base import DataSourceService
from etl.services.db.factory import DatabaseService


class CdcFactroy(object):
    """Фабрика для докачки"""

    @staticmethod
    def factory(source):
        """
        фабрика для инстанса докачки
        """
        conn_type = source.conn_type

        if conn_type == ConnectionChoices.POSTGRESQL:
            cdc_instance = postgres.PostgresqlCdc()
        elif conn_type == ConnectionChoices.MYSQL:
            cdc_instance = mysql.MysqlCdc()
        else:
            raise ValueError("Неизвестный тип подключения!")

        return cdc_instance

    @classmethod
    def create_load_mechanism(cls, source, tables_info):

        cdc_instance = cls.factory(source)
        cdc_instance.bd_instance = DatabaseService.get_source_instance(source)

        # непонятно как это обрабатывать на ошибки
        source_settings = DatasourceSettings.objects.get(
            datasource_id=source.id)

        if source_settings.value == 'apply_triggers':
            cdc_instance.apply_triggers(tables_info)

        elif source_settings.value == 'apply_checksum':
            cdc_instance.apply_checksum()
