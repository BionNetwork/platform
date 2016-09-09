# coding: utf-8



from etl.services.nosql.mongodb import Mongodb
from etl.services.source import DatasourceApi


class NoSqlService(DatasourceApi):

    def get_source_instance(self):

        return Mongodb(self.source)
