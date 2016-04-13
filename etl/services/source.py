# coding: utf-8


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
