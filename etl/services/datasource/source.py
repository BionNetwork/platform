# coding: utf-8


class SourceConvertTypes(object):
    """
    Типы для конвертации
    """
    INT = "integer"
    DOUBLE = "double precision"
    DATE = "timestamp"
    TEXT = "text"
    BOOL = "boolean"


class DatasourceApi(object):
    """
    Работа с источником данных
    """

    def __init__(self, source):
        self.source = source
        self.datasource = self.get_source_instance()

    def get_source_instance(self):
        """
        Получение экземпляра источника
        """
        raise NotImplementedError

    def get_source_rows(self, structure, cols, limit=None, offset=None):
        """
        Получение постраничных данных из базы пользователя
        """
        raise NotImplementedError

    # FIXME NO USAGE
    def get_rows(self, cols, structure):
        """
        Получение значений выбранных колонок из указанных таблиц и
        выбранного источника
        :type structure: dict
        :param cols: list
        :return:
        """
        return self.datasource.get_rows(cols, structure)
