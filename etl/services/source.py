# coding: utf-8


class DatasourceApi(object):
    """
    Работа с источником данных
    """

    def __init__(self, source):
        self.source = source
        self.datasource = self.factory(source)

    def factory(self, source):
        """
        Args:
            source(core.models.Datasource): источник данных

        Returns:

        """
        raise NotImplementedError

    def get_tables(self):
        raise NotImplementedError
