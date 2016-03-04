# coding: utf-8
from __future__ import unicode_literals
import json

import xmltodict
import logging

from core.models import Cube
from core.views import BaseViewNoLogin
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException, \
    mdx_execute
from django.db import transaction


logger = logging.getLogger(__name__)


class ImportSchemaView(BaseViewNoLogin):
    """
    Импортирование схемы куба
    """

    def post(self, request, *args, **kwargs):
        post = request.POST
        key = post.get('key')
        data = post.get('data')

        try:
            with transaction.atomic():
                try:
                    cube = Cube.objects.get(
                        name=key,
                        user_id=int(post.get('user_id')),
                    )
                    cube.data = data
                    cube.save()
                except Cube.DoesNotExist:
                    cube = Cube.objects.create(
                        name=key,
                        user_id=int(post.get('user_id')),
                        data=data,
                    )

                send_xml(key, cube.id, data)

                return self.json_response({'id': cube.id, 'status': 'success'})

        except OlapServerConnectionErrorException as e:
            message_to_log = "Can't connect to OLAP Server!\n" + e.message + "\nCube data:\n" + data
            logger.error(message_to_log)
            message = e.message
        except Exception as e:
            message_to_log = "Error creating cube by key" + key + "\n" + e.message + "\nCube data:\n" + data
            logger.error(message_to_log)
            message = e.message

        return self.json_response({'status': 'error', 'message': message})


class ExecuteQueryView(BaseViewNoLogin):
    """
    Выполнение mdx запроса к данным
    """

    def get(self, request, *args, **kwargs):
        mdx_request_info = request.POST('mdx_info')

        # Тестовый запрос
        # mdx_request_info = """{
# "cube":
#     {"name":"cube__2002624614230133262"},
# "mdx": "SELECT {[Measures].[promotion__promotion_id], [Measures].[promotion__promotion_district_id]} ON COLUMNS, NON EMPTY {[Dim Table].[promotion__media_type].Members} ON ROWS FROM [cube__2002624614230133262]",
# "name": "BEC1E7D7-12DC-8F5A-A1C3-6CFC636041E2",
# "queryType": "OLAP",
# "type":"QUERYMODEL"
# }"""
        mdx_info = json.loads(mdx_request_info)
        mdx = mdx_info['mdx']
        cube_name = mdx_info['cube']['name']
        mdx_response = mdx_execute(cube_name, mdx)
        result = {
            'cellset': mdx_response,
            'query': {
                'cube:': {'name': cube_name},
                'mdx': mdx,
                'queryType': mdx_info['queryType'],
                'type': mdx_info['type'],
            }
        }
        return self.json_response(result)


class SchemasListView(BaseViewNoLogin):
    """
    Список доступных кубов
    """

    def get(self, request, *args, **kwargs):

        cubes = map(lambda x: {
            'id': x.id,
            'user_id': x.user_id,
            'create_date': (x.create_date.strftime("%Y-%m-%d %H:%M:%S")
                            if x.create_date else ''),
            'name': x.name,
        }, Cube.objects.all())

        return self.json_response(cubes)


class GetSchemaView(BaseViewNoLogin):
    """
    Получение информации по кубу
    """

    def get(self, request, *args, **kwargs):
        cube_id = kwargs.get('id')

        try:
            cube = Cube.objects.get(id=cube_id)
        except Cube.DoesNotExist:
            return self.json_response(
                {'status': 'error', 'message': 'No such schema!'})

        cube_dict = xmltodict.parse(cube.data)

        return self.json_response(cube_dict)
