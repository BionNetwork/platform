# coding: utf-8
from __future__ import unicode_literals

import logging
from rest_framework import serializers

from rest_framework.response import Response
from rest_framework import viewsets, generics, mixins
from rest_framework.views import APIView

from api.serializers import (
    UserSerializer, DatasourceSerializer, SchemasListSerializer,
    SchemasRetreviewSerializer, CardDatasourceSerializer, TaskSerializer, tasks,
    TableSerializer, NodeSerializer, TreeSerializer, TreeSerializerRequest)

from core.models import (Cube, User, Datasource, Dimension, Measure,
                         DatasourceMetaKeys, CardDatasource)
from core.views import BaseViewNoLogin
from etl.services.datasource.base import DataSourceService
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
from django.db import transaction

from rest_framework.decorators import detail_route, list_route


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
        user_id = post.get('user_id')

        try:
            with transaction.atomic():
                try:
                    cube = Cube.objects.get(
                        name=key,
                        user_id=int(user_id),
                        dataset_id=post.get('dataset_id'),
                    )
                except Cube.DoesNotExist:
                    cube = Cube(
                        name=key,
                        user_id=int(user_id),
                        dataset_id=post.get('dataset_id'),
                    )
                cube.data = data
                cube.save()

                send_xml(key, cube.id, data)

                return self.json_response({'id': cube.id, 'status': SUCCESS})

        except OlapServerConnectionErrorException as e:
            message_to_log = "Can't connect to OLAP Server!\n" + e.message + "\nCube data:\n" + data
            logger.error(message_to_log)
            message = e.message
        except Exception as e:
            message_to_log = "Error creating cube by key" + key + "\n" + e.message + "\nCube data:\n" + data
            logger.error(message_to_log)
            message = e.message

        return self.json_response({'status': ERROR, 'message': message})


class ExecuteQueryView(BaseViewNoLogin):
    """
    Выполнение mdx запроса к данным
    """

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': SUCCESS})


class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class DatasourceViewSet(viewsets.ModelViewSet):
    model = Datasource
    serializer_class = DatasourceSerializer

    def get_queryset(self):
        return self.model.objects.filter(user_id=self.request.user.id)

    def create(self, request, *args, **kwargs):
        request.data.update({'user_id': request.user.id})
        return super(DatasourceViewSet, self).create(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        source = self.get_object()
        DataSourceService.delete_datasource(source)
        DataSourceService.tree_full_clean(source)
        return super(DatasourceViewSet, self).destroy(request, *args, **kwargs)

    @detail_route(methods=['get'])
    def tables(self, request, pk=None):
        source = Datasource.objects.get(id=pk)

        service = DataSourceService.get_source_service(source)
        data = service.get_tables()
        return Response(data)


class CardDataSourceViewSet(viewsets.ModelViewSet):
    """
    Источкник в карточке
    """
    model = CardDatasource
    serializer_class = CardDatasourceSerializer

    def get_queryset(self):
        return self.model.objects.filter(source__user_id=self.request.user.id)

    def create(self, request, *args, **kwargs):
        pass


class SchemasListView(mixins.ListModelMixin,
                      mixins.CreateModelMixin,
                      generics.GenericAPIView):
    queryset = Cube.objects.all()
    serializer_class = SchemasListSerializer

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)


class GetSchemaView(mixins.RetrieveModelMixin,
                    generics.GenericAPIView):
    queryset = Cube.objects.all()
    serializer_class = SchemasRetreviewSerializer

    def get(self, request, *args, **kwargs):
        return self.retrieve(request, *args, **kwargs)


class GetMeasureDataView(BaseViewNoLogin):
    """
    Получение информации о мерах
    """

    def get(self, request, *args, **kwargs):
        cube_id = kwargs.get('id')

        try:
            cube = Cube.objects.get(id=cube_id)
        except Cube.DoesNotExist:
            return self.json_response({
                'status': ERROR,
                'message': "No cube with id={0}".format(cube_id)
            })

        key = cube.name.split('cube_')[1]

        meta_ids = DatasourceMetaKeys.objects.filter(
            value=key).values_list('meta_id', flat=True)

        measures = Measure.objects.filter(datasources_meta_id__in=meta_ids)

        data = map(lambda measure:{
            "id": measure.id,
            "name": measure.name,
            "title": measure.title,
            "type": measure.type,
            "aggregator": measure.aggregator,
            "format_string": measure.format_string,
            "visible": measure.visible,
        }, measures)

        return self.json_response({'data': data, })


class GetDimensionDataView(BaseViewNoLogin):
    """
    Получение информации размерности
    """

    def get(self, request, *args, **kwargs):
        cube_id = kwargs.get('id')

        try:
            cube = Cube.objects.get(id=cube_id)
        except Cube.DoesNotExist:
            return self.json_response({
                'status': ERROR,
                'message': "No cube with id={0}".format(cube_id)
            })

        key = cube.name.split('cube_')[1]

        meta_ids = DatasourceMetaKeys.objects.filter(
            value=key).values_list('meta_id', flat=True)

        dimensions = Dimension.objects.filter(datasources_meta_id__in=meta_ids)

        data = map(lambda dimension: {
            "id": dimension.id,
            "name": dimension.name,
            "title": dimension.title,
            "type": dimension.get_dimension_type(),
            "visible": dimension.visible,
            "high_cardinality": dimension.high_cardinality,
            "data": dimension.data,
        }, dimensions)

        return self.json_response({'data': data, })


# @api_view()
def build_tree(request):
    return Response({'message': 'Hello, world!'})


class TaskViewSet(viewsets.ViewSet):
    # Required for the Browsable API renderer to have a nice form.
    serializer_class = TaskSerializer

    def list(self, request):
        serializer = TaskSerializer(
            instance=tasks.values(), many=True)
        return Response(serializer.data)

    def retrieve(self, request, pk=None):
        return Response(tasks[int(pk)].__dict__)

    def update(self, request, pk=None):
        return Response(tasks[int(pk)].__dict__)

    def create(self, request):
        return Response()


class TablesDataView(APIView):

    def get(self, request, source_id, table_name):
        """
        Получение данных о таблице
        """
        source = Datasource.objects.get(id=source_id)

        service = DataSourceService.get_source_service(source)
        data = service.fetch_tables_columns([table_name])
        return Response(data)


class CardViewSet(viewsets.ViewSet):
    """
    Реализация методов карточки
    """

    serializer_class = TreeSerializer

    @detail_route(['post'], serializer_class=TreeSerializerRequest)
    def create_tree(self, request, pk=None):

        card_id = pk
        data = request.data

        data = [
            # {"source_id": 2, "table_name": u'auth_group', },
            # {"source_id": 2, "table_name": u'auth_group_permissions', },
            # {"source_id": 2, "table_name": u'auth_permission', },
            # {"source_id": 2, "table_name": u'card_card', },
            # {"source_id": 1, "table_name": u'Лист1', },
            # {"source_id": 1, "table_name": u'List3', },
            # {"source_id": 1, "table_name": u'Лист2', },
        ]

        serializer = self.serializer_class(data=data, many=True)
        info = []

        if serializer.is_valid():
            for each in data:
                ds = Datasource.objects.get(id=each['source_id'])
                info = DataSourceService.process_tree_info(
                    card_id, ds, each['table_name'])

        return Response(info)


class Node(object):
    def __init__(self, **kwargs):
        for field in ('dist', 'is_root', 'source_id', 't_name', 'without_bind'):
            setattr(self, field, kwargs.get(field, None))

nodes = {
    1: Node(id=1, dest='auth_group', is_root=True,
            source_id=1, t_name='auth_group', is_bind=False),
    2: Node(id=1, dest='auth_group', is_root=True,
            source_id=1, t_name='auth_group_permission', is_bind=False),
}


class NodeViewSet(viewsets.ViewSet):
    """
    Предстваление для работы с узлами для дерева
    """

    serializer_class = NodeSerializer

    def list(self, request, card_pk):

        data = DataSourceService.get_tree_api(card_pk)
        d = []
        for index, node in enumerate(data):
            d.append({
                'id': index,
                'source_id': node['source_id'],
                'table_name': node['tname'],
                'dest': node['dest'],
                'is_root': node['is_root'],
                'is_remain': False,
                'is_bind': not node['without_bind']
            })
        s = NodeSerializer(data=d, many=True)
        # raise serializers.ValidationError('This field must be an even number.')
        if s.is_valid():
            return Response(data=d)

    def create(self, request):
        data = request.POST
        source = Datasource.objects.get(id=4)
        table = 'tname'
        info = DataSourceService.get_tree_info(
            source, table)
        return

    def update(self, request, card_pk=None, pk=None):
        a = 3
        return Response(data={
                'id': 1,
                'source_id': 1,
                'table_name': 'cubes',
                'dest': 'abc',
                'is_root': True,
                'is_remain': False,
                'is_bind': True
            })

    def retrieve(self, request, card_pk=None, pk=None):
        return Response(data={
                'id': 1,
                'source_id': 1,
                'table_name': 'cubes',
                'dest': 'abc',
                'is_root': True,
                'is_remain': False,
                'is_bind': True
            })


# [{"source_id":1,"table_name":"cubes"},{"source_id":1,"table_name":"datasets"}]

    @detail_route(methods=['post'])
    def change_destination(self):
        pass
