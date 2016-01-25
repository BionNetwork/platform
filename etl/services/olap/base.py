# -*- coding: utf-8 -*-
import os
from olap.xmla import xmla
import easywebdav

from django.conf import settings

XMLA_URL = 'http://{host}:{port}/saiku/xmla'.format(
    host=settings.OLAP_SERVER_HOST, port=settings.OLAP_SERVER_PORT)
REPOSITORY_PATH = 'saiku/repository/default'


class OlapClient(object):
    """
    Клиент Saiku
    """
    def __init__(self, cude_id):
        """
        Args:
            cube_id(int): id куба
        """
        self.cube_id = cude_id
        self.connect = xmla.XMLAProvider().connect(location=XMLA_URL)
        self.webdav = easywebdav.connect(
            host=settings.OLAP_SERVER_HOST,
            port=settings.OLAP_SERVER_PORT,
            path=REPOSITORY_PATH,
            username=settings.OLAP_SERVER_USER,
            password=settings.OLAP_SERVER_PASS
        )

    def file_upload(self, file_name):
        self.webdav.upload(
            os.path.join(
                settings.BASE_DIR, 'data/resources/cubes/', '{0}/{1}'.format(
                    self.cube_id, file_name)),
            remote_path='datasources/{0}'.format(file_name))

    def file_delete(self, file_name):
        self.webdav.delete('datasources/{0}'.format(file_name))

    def execute(self, mdx=None):
        return self.connect.Execute(mdx, Catalog='cube_848272420')