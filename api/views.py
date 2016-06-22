# coding: utf-8
from __future__ import unicode_literals

import logging

from django.db import transaction

from rest_framework import serializers
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework import viewsets, generics, mixins
from rest_framework.views import APIView

from api.serializers import (
    UserSerializer, DatasourceSerializer, SchemasListSerializer,
    SchemasRetreviewSerializer, NodeSerializer, TreeSerializer,
    TreeSerializerRequest, ParentIdSerializer)

from core.models import (Cube, User, Datasource, Dimension, Measure,
                         DatasourceMetaKeys)
from core.views import BaseViewNoLogin
from etl.services.datasource.base import DataSourceService
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
from etl.views import LoadDataView

from rest_framework.decorators import detail_route

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

    def list(self, request, *args, **kwargs):
        return super(DatasourceViewSet, self).list(request, *args, **kwargs)

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


class TablesDataView(APIView):

    def get(self, request, source_id, table_name):
        """
        Получение данных о таблице
        """
        source = Datasource.objects.get(id=source_id)

        service = DataSourceService.get_source_service(source)
        data = service.fetch_tables_columns([table_name])
        return Response(data)


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


class CardViewSet(viewsets.ViewSet):
    """
    Реализация методов карточки
    """

    serializer_class = TreeSerializer

    @detail_route(['post'], serializer_class=TreeSerializerRequest)
    def create_tree(self, request, pk):

        data = request.data

        card_id = pk

        data = [
            {"source_id": 2, "table_name": u'auth_group', },
            {"source_id": 2, "table_name": u'auth_group_permissions', },
            {"source_id": 2, "table_name": u'auth_permission', },
            {"source_id": 2, "table_name": u'card_card', },
            {"source_id": 1, "table_name": u'Лист1', },
            {"source_id": 1, "table_name": u'List3', },
            {"source_id": 1, "table_name": u'Лист2', },

        #     # {"source_id": 1, "table_name": u"auth_group", },
        #     # {"source_id": 1, "table_name": u"auth_group_permissions", },
        #     # {"source_id": 1, "table_name": u"auth_permission", },
        #     # {"source_id": 1, "table_name": u"card_card", },
        #     # {"source_id": 4, "table_name": u"list1", },
        #     # {"source_id": 4, "table_name": u"List3", },
        #     # {"source_id": 4, "table_name": u"Лист2", },
        ]

        info = []

        serializer = self.serializer_class(data=data, many=True)
        if serializer.is_valid():
            for each in data:
                node_id = DataSourceService.cache_columns(
                    card_id, each['source_id'], each['table_name'])

                info = DataSourceService.add_randomly_from_remains(
                    card_id, node_id)

        return Response(info)

    @detail_route(['post', ])
    def load_data(self, request, pk):
        """
        Начачло загрузки данных
        """
        load_view = LoadDataView.as_view()
        load_view(request, card_id=pk)
        return Response('OK')


def check_parent(func):
    """
    Проверка ID родителя на существование
    """
    def inner(*args, **kwargs):
        request = args[1]
        card_id = int(kwargs['card_pk'])
        parent_id = int(request.data.get('parent_id'))

        if not DataSourceService.check_node_id_in_builder(
                card_id, parent_id, in_remain=False):
            raise APIException("No such node id in builder!")

        return func(*args, **kwargs)
    return inner


def check_child(in_remain=True):
    """
    Проверка ID ребенка на существование, если in_remain=True,
    то проверяет в остатках, иначе в активных
    """
    def inner(func):
        def inner(*args, **kwargs):

            node_id = int(kwargs['pk'])
            card_id = int(kwargs['card_pk'])

            # проверка узла родителя
            if not DataSourceService.check_node_id_in_builder(
                    card_id, node_id, in_remain):
                raise APIException("No such node id in builder!")

            return func(*args, **kwargs)
        return inner
    return inner


class NodeViewSet(viewsets.ViewSet):
    """
    Предстваление для работы с узлами для дерева
    """

    serializer_class = NodeSerializer

    def list(self, request, card_pk):
        """
        Список узлов дерева и остаткa
        """
        data = DataSourceService.get_tree_api(card_pk)

        # FIXME доделать валидатор
        # serializer = NodeSerializer(data=data, many=True)
        # if serializer.is_valid():
        return Response(data=data)

    def retrieve(self, request, card_pk=None, pk=None):
        """
        Инфа ноды
        """
        data = DataSourceService.get_node(card_pk, pk)

        return Response(data=data)

    @detail_route(methods=['post'], serializer_class=ParentIdSerializer)
    @check_child(in_remain=False)
    @check_parent
    def reparent(self, request, card_pk, pk):
        """
        Изменение родительского узла, перенос узла с одного места на другое

        Args:
            card_pk(int): id карточки
            pk(int): id узла

        Returns:
            dict: Информацию об связанных/не связанных узлах дерева и остатка
        """

        # fixme serializer проверить
        # serializer = self.serializer_class(data=request.data, many=True)
        # if serializer.is_valid(raise_exception=True):
        # try:
        info = DataSourceService.reparent(
                card_pk,
                request.data['parent_id'],
                pk)
        return Response(info)
        # except Exception as ex:
        #     raise APIException(ex.message)

    @detail_route(methods=['post'])
    @check_child(in_remain=False)
    def to_remain(self, request, card_pk, pk):
        """
        Добавлеине узла дерева в остатки
        """
        node_id = pk
        info = DataSourceService.send_nodes_to_remains(
            card_pk, node_id)

        return Response(info)

    @detail_route(methods=['post'])
    def change_source(self):
        """
        Измение источника для узла
        """

    @detail_route(methods=['post'])
    @check_child
    def remain_whatever(self, request, card_pk, pk):
        """
        Перенос узла из остатков в основное дерево
        в произвольное место
        """
        node_id = pk
        info = DataSourceService.add_randomly_from_remains(
                    card_pk, node_id)
        return Response(info)

    @detail_route(methods=['post'], serializer_class=ParentIdSerializer)
    @check_child
    @check_parent
    def remain_current(self, request, card_pk, pk):
        """
        Перенос узла из остатков в основное дерево
        в определенный узел

        Args:
            card_pk(int): id карточки
            pk(int): id узла

        Returns:
            dict: Информацию об связанных/не связанных узлах дерева и остатка
        """
        # fixme serializer проверить
        # serializer = self.serializer_class(data=request.data, many=True)
        # if serializer.is_valid(raise_exception=True):
        try:
            parent_id = request.data['parent_id']
            info = DataSourceService.from_remain_to_certain(
                card_pk, parent_id, pk)
            return Response(info)
        except Exception as ex:
            raise APIException(ex.message)


class JoinViewSet(viewsets.ViewSet):
    """
    Представление для связей таблиц
    """

    def retrieve(self, request, card_pk=None, node_pk=None, pk=None):
        """
        Получение информации о соединение узлов (join)
        ---
        Args:
            card_pk(int): id карточки
            node_pk(int): id родительского узла
            pk(int): id дочернего узла

        Returns:
        """

        data = DataSourceService.get_columns_and_joins(
            card_pk, int(node_pk), int(pk))

        return Response(data=data)

    def update(self, request, card_pk=None, node_pk=None, pk=None):
        """
        Обновление связи между узлами

        Args:
            card_pk(int): id карточки
            node_pk(int): id родительского узла
            pk(int): id узла-потомка

        {
        "joins":
        [{
            "right": "group_id",
            "join": "eq",
            "left": "id"
        }, {...}]
}
        """

        right_data = DataSourceService.get_node(card_pk, pk)
        left_data = DataSourceService.get_node(card_pk, node_pk)
        parent_sid = left_data['sid']
        child_sid = right_data['sid']

        parent_table = left_data['value']
        child_table = right_data['value']

        join_type = 'inner'

        joins = []
        for each in request.data['joins']:
            joins.append([each['left'], each['join'], each['right']])

        data = DataSourceService.save_new_joins_NEW(
            card_pk, parent_table, parent_sid, child_table,
            child_sid, pk, join_type, joins)

        return Response(data=data)


# {"joins": [
#         {
#             "right": {
#                 "column": "group_id",
#                 "table": "auth_group_permissions",
#                 "sid": 1
#             },
#             "join": {
#                 "type": "inner",
#                 "value": "eq"
#             },
#             "left": {
#                 "column": "id",
#                 "table": "auth_group",
#                 "sid": 1
#             }
#         }
#     ]}