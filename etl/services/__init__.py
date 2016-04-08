# coding: utf-8

from core.models import ConnectionChoices as ConnType
from etl.services.db.factory import DatabaseService
from etl.services.file.factory import FileService


def get_datasource(source):
    """
    Определение типа источника данных

    Args:
        source(core.models.Datasource): источник данных

    Returns:
        etl.services.source.DatasourceApi: Сервис для работы с источником
    """

    connection_type = source.conn_type
    if connection_type in [
        ConnType.POSTGRESQL, ConnType.MYSQL, ConnType.MS_SQL, ConnType.ORACLE]:
        return DatabaseService(source)
    elif connection_type in [ConnType.CSV, ConnType.EXCEL, ConnType.TXT]:
        return FileService(source)
