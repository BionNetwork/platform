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


def send_xml(key, cube_id, xml):
    """
    Отправка файлов в mondrian-server

    Args:
        key(str): ключ
        cube_id(int): id куба
        xml(str): содержимое схемы
    """

    directory = os.path.join(
        settings.BASE_DIR, 'data/resources/cubes/{0}/'.format(cube_id))
    os.makedirs(directory)

    datasource_file_name = 'datasource_{0}.sds'.format(key)
    schema_name = 'cube_{0}.xml'.format(key)

    db_info = settings.DATABASES['default']
    settings_str = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <dataSource>
        <driver>mondrian.olap4j.MondrianOlap4jDriver</driver>
        <id>{schema_name}</id>
        <location>jdbc:mondrian:Jdbc=jdbc:postgresql://{host}/{db_name};Catalog=mondrian:///datasources/{schema_name}</location>
        <name>{schema_name}</name>
        <type>OLAP</type>
        <username>biplatform</username>
    </dataSource>""".format(schema_name=schema_name, host=db_info['HOST'], db_name=db_info['NAME'])

    with open(os.path.join(
            directory, datasource_file_name), 'w') as df:
        df.write(settings_str)

    with open(os.path.join(
            directory, schema_name), 'w') as sf:
        sf.write(xml)

    client = OlapClient(cube_id)
    try:
        client.file_delete(datasource_file_name)
        client.file_delete(schema_name)
    except easywebdav.OperationFailed as e:
        pass
    client.file_upload(datasource_file_name)
    client.file_upload(schema_name)
    # oc.connect.getDatasources()
