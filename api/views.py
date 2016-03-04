# coding: utf-8
from __future__ import unicode_literals

import xmltodict
import logging

from core.models import Cube
from core.views import BaseViewNoLogin
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
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
                        user_id=post.get('user_id'),
                        dataset_id=post.get('dataset_id'),
                    )
                except Cube.DoesNotExist:
                    cube = Cube(
                        name=key,
                        user_id=post.get('user_id'),
                        dataset_id=post.get('dataset_id'),
                    )
                cube.data = data
                cube.structure = post.get('tree_structure')
                cube.save()

                send_xml(key, cube.id, data)

                return self.json_response({'id': cube.id, 'status': 'success'})

        except OlapServerConnectionErrorException as e:
            logger.error("Can't connect to OLAP Server!")
            logger.error(e.message)
            message = e.message
        except Exception as e:
            logger.error("Error creating cube by key" + key)
            logger.error(e.message)
            message = e.message

        return self.json_response({'status': 'error', 'message': message})


class ExecuteQueryView(BaseViewNoLogin):
    """
    Выполнение mdx запроса к данным
    """

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': 'success'})


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
