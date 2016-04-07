# -*- coding: utf-8 -*-
import json
import os
from olap.xmla import xmla
import easywebdav

from django.conf import settings
from requests import ConnectionError
from core.models import Cube


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
        self.connect = xmla.XMLAProvider().connect(location=settings.OLAP_XMLA_URL)
        self.webdav = easywebdav.connect(
            host=settings.OLAP_SERVER_HOST,
            port=settings.OLAP_SERVER_PORT,
            path=settings.OLAP_REPOSITORY_PATH,
            username=settings.OLAP_SERVER_USER,
            password=settings.OLAP_SERVER_PASS
        )

    def file_upload(self, file_name):
        """
        Загрузка файла

        Args:
            file_name(str): Название файла
        """
        self.webdav.upload(
            os.path.join(
                settings.BASE_DIR, 'data/resources/cubes/', '{0}/{1}'.format(
                    self.cube_id, file_name)),
            remote_path='datasources/{0}'.format(file_name))

    def file_delete(self, file_name):
        """
        Удаление файла

        Args:
            file_name(str): Название файла
        """
        self.webdav.delete('datasources/{0}'.format(file_name))


def send_xml(key, cube_id, xml):
    """
    Отправка файлов в mondrian-server

    Args:
        key(unicode): ключ
        cube_id(int): id куба
        xml(str): содержимое схемы
    """

    directory = os.path.join(
        settings.BASE_DIR, 'data/resources/cubes/{0}/'.format(cube_id))

    if not os.path.exists(directory):
        os.makedirs(directory)

    datasource_file_name = 'datasource_{0}.sds'.format(key)
    schema_name = '{0}.xml'.format(key)

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
    except ConnectionError as ce:
        raise OlapServerConnectionErrorException(ce.message)

    client.file_upload(datasource_file_name)
    client.file_upload(schema_name)
    # oc.connect.getDatasources()



def get_axis(data, axis):
    """
    Получение данных по осям

    Args:
        data(TupleFormatReader): Данные
        axis(str): Название оси. 'Axis1' или 'Axis2'

    Returns:
        list: Данные по оси
    """
    try:
        return data.getAxisTuple(axis)
    except IndexError:
        return None


def mdx_execute(cube_name, mdx=None):
    """
    executing mdx request

    Args:
        mdx(unicode): Строка запроса
        cube_name(unicode): Название куба

    Returns:
        list: Данные mdx-запроса
    """

    cube_id = Cube.objects.get(name=cube_name).id
    # cube_id = 68
    client = OlapClient(cube_id)
    res = client.connect.Execute(mdx, Catalog=cube_name)

    axis_0 = get_axis(res, 'Axis0')
    axis_1 = get_axis(res, 'Axis1')

    data = []

    header = [{
        'value': '(All)',
        'type': 'ROW_HEADER_HEADER',
        'properties': {}
    }]
    if not axis_0:
        cell = res.getSlice()
        data.append({
            'value': cell['FmtValue'],
            'type': 'DATA_CELL',
            'properties': {
                'raw': cell['Value'],
                'cell_ordinal': cell['_CellOrdinal'],
            }
        })
        return data
    for column in axis_0:
        column_header = {
            'value': column['Caption'],
            'type': 'COLUMN_HEADER',
            'properties': {
                'uniquename': column['UName'],
                'hierarchy': column['_Hierarchy'],
                'dimension': 'Measures',
                'level': column['LName']
            },
        }
        if not axis_1:
            column_header['display_value'] = column['DisplayInfo']
        header.append(column_header)

    data.append(header)

    if not axis_1:
        return data

    row_names = []
    for row_name in axis_1:
        row_names.append({
            'value': row_name['Caption'],
            'type': 'ROW_HEADER',
            'properties': {
                'uniquename': row_name['UName'],
                'hierarchy': row_name['_Hierarchy'],
                'level': row_name['LName']
            }
        })

    cellmap = [[dict(x) for x in row] for row in res.getSlice()]
    data_cells = []
    for row in cellmap:
        row_data = []
        for cell in row:
            row_data.append({
                'value': cell['FmtValue'],
                'type': 'DATA_CELL',
                'properties': {
                    'raw': cell['Value'],
                    'cell_ordinal': cell['_CellOrdinal'],
                }
            })
            data_cells.append(row_data)

    for row_name, row_data in zip(row_names, data_cells):
        row = [row_name]
        row.extend(row_data)
        data.append(row)

    return data


class OlapServerConnectionErrorException(Exception):
    """
    Исключение при ошибке коннекта к olap серверу
    """
    pass

