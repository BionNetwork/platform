# coding: utf-8
from __future__ import unicode_literals

import xmltodict
import logging

from core.models import Cube, Dimension, Measure
from core.views import BaseViewNoLogin
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
from django.db import transaction


logger = logging.getLogger(__name__)


SUCCESS = 'success'
ERROR = 'error'


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
                cube, created = Cube.objects.get_or_create(
                    name=key,
                    user_id=post.get('user_id'),
                )
                cube.data = data
                cube.save()

                send_xml(key, cube.id, data)

                return self.json_response({'id': cube.id, 'status': SUCCESS})

        except OlapServerConnectionErrorException as e:
            logger.error("Can't connect to OLAP Server!")
            logger.error(e.message)
            message = e.message
        except Exception as e:
            logger.error("Error creating cube by key" + key)
            logger.error(e.message)
            message = e.message

        return self.json_response({'status': ERROR, 'message': message})


class ExecuteQueryView(BaseViewNoLogin):
    """
    Выполнение mdx запроса к данным
    """

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': SUCCESS})


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


class GetMeasureDataView(BaseViewNoLogin):
    """
    Получение информации меры
    """

    def get(self, request, *args, **kwargs):
        measure_id = kwargs.get('id')
        try:
            measure = Measure.objects.get(id=measure_id)
        except Measure.DoesNotExist:
            return self.json_response({
                'status': ERROR,
                'message': "No measure with id={0}".format(measure_id)
            })

        data = {
            "id": measure_id,
            "name": measure.name,
            "title": measure.title,
            "type": measure.type,
            "aggregator": measure.aggregator,
            "format_string": measure.format_string,
            "visible": measure.visible,
        }

        return self.json_response({'data': data, })


class GetDimensionDataView(BaseViewNoLogin):
    """
    Получение информации размерности
    """

    def get(self, request, *args, **kwargs):
        dimension_id = kwargs.get('id')
        try:
            dimension = Dimension.objects.get(id=dimension_id)
        except Dimension.DoesNotExist:
            return self.json_response({
                'status': ERROR,
                'message': "No dimension with id={0}".format(dimension_id)
            })

        data = {
            "id": dimension_id,
            "name": dimension.name,
            "title": dimension.title,
            "type": dimension.get_dimension_type(),
            "visible": dimension.visible,
            "high_cardinality": dimension.high_cardinality,
            "data": dimension.data,
        }

        return self.json_response({'data': data, })
