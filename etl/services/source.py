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

    def get_tables(self):
        raise NotImplementedError

    def get_separator(self):
        raise NotImplementedError
