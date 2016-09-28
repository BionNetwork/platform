# coding: utf-8


from etl.services.datasource.nosql import Mongodb
from etl.services.datasource.source import BaseSourceService


class NoSqlService(BaseSourceService):

    def get_source_instance(self):

        return Mongodb(self.source)
