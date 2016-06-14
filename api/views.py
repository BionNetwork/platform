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
from etl.models import TableTreeRepository
from etl.services.datasource.base import DataSourceService, RedisSS
from etl.services.datasource.repository.storage import RKeys
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
    def create_tree(self, request, pk):

        data = request.data

        card_id = pk

        data = [
            {"source_id": 2, "table_name": u'auth_group', },
            # {"source_id": 2, "table_name": u'auth_group_permissions', },
            # {"source_id": 2, "table_name": u'auth_permission', },
            # {"source_id": 2, "table_name": u'card_card', },
            {"source_id": 1, "table_name": u'Лист1', },
            {"source_id": 1, "table_name": u'List3', },
            {"source_id": 1, "table_name": u'Лист2', },
        ]

        serializer = self.serializer_class(data=data, many=True)
        info = []

        if serializer.is_valid():
            for each in data:

                table = each['table_name']
                source_id = each['source_id']
                source = Datasource.objects.get(id=source_id)

                info = DataSourceService.try_tree_restruct(
                    card_id, source, table)

        return Response(info)


class NodeViewSet(viewsets.ViewSet):
    """
    Предстваление для работы с узлами для дерева
    """

    serializer_class = NodeSerializer

    def list(self, request, card_pk):
        """
        Cписок узлов дерева и остаткa
        """
        data = DataSourceService.get_tree_api(card_pk)

        # FIXME доделать валидатор
        # serializer = NodeSerializer(data=data, many=True)
        # if serializer.is_valid():
        return Response(data=data)

    # def create(self, request):
    #     data = request.POST
    #     source = Datasource.objects.get(id=4)
    #     table = 'tname'
    #     info = DataSourceService.get_tree_info(
    #         source, table)
    #     return
    #
    # def update(self, request, card_pk=None, pk=None):
    #     return Response(data={
    #             'id': 1,
    #             'source_id': 1,
    #             'table_name': 'cubes',
    #             'dest': 'abc',
    #             'is_root': True,
    #             'is_remain': False,
    #             'is_bind': True
    #         })

    def retrieve(self, request, card_pk=None, pk=None):
        # достаем структуру дерева из редиса
        # FIXME: Перенести в сервис Datasource
        structure = RedisSS.get_active_tree_structure_NEW(card_pk)
        sel_tree = TableTreeRepository.build_tree_by_structure(structure)
        node_info = sel_tree.get_node_info(pk)
        card_key = RKeys.get_user_card_key(card_pk)
        actives = RedisSS.get_card_actives_data(card_key)
        data = RedisSS.get_node_info(actives, node_info)

        data = DataSourceService.get_node(card_pk, pk)

        return Response(data={
                'id': pk,
                'source_id': data['sid'],
                'table_name': data['value'],
                'dest': data['parent_id'],
                'is_root': data['is_root'],
                'is_remain': False,
                'is_bind': not data['without_bind']
            })
    # [{"source_id":1,"table_name":"cubes"},{"source_id":1,"table_name":"datasets"}]

    @detail_route(methods=['get'])
    def reparent(self, request, card_pk, pk):
        """
        Изменение родительского узла, перенос ноды с одгного места на другое
        """
        node_id = pk
        parent_id = request.data['parent_id']

        info = DataSourceService.reparent(card_pk, node_id, parent_id)
        return Response(info)

    @detail_route(methods=['post'])
    def to_remain(self):
        """
        Добавлеине узла дерева в остатки
        """

    @detail_route(methods=['post'])
    def change_source(self):
        """
        Измение источника для узла
        """

    @detail_route(methods=['post'])
    def remain_whatever(self, request, card_pk, pk):
        """
        Перенос узла из остатков в основное дерево
        в произвольное место
        """
        info = DataSourceService.from_remain_to_whatever(card_pk, pk)
        return Response(info)

    @detail_route(methods=['post'])
    def remain_current(self, request, card_pk, pk):
        """
        Перенос узла из остатков в основное дерево
        в определенный узел
        """
        node_id = pk
        parent_id = request.data['parent_id']

        info = DataSourceService.from_remain_to_certain(
            card_pk, node_id, parent_id)

        return Response(info)


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
        right_data = DataSourceService.get_node(card_pk, node_pk)
        left_data = DataSourceService.get_node(card_pk, pk)
        parent_sid = right_data['source_id']
        child_sid = left_data['source_id']

        parent_table = right_data['tname']
        child_table = left_data['tname']

        data = DataSourceService.get_columns_and_joins(
            request.user.id, parent_table, parent_sid, child_table, child_sid)

        return Response(data=data)

    def update(self, request, card_pk=None, node_pk=None, pk=None):
        """
        {
        "joins":
        {
            "right": "group_id",
            "join": "eq",
            "left": "id"
        }
}
        """

        right_data = DataSourceService.get_node(card_pk, node_pk)
        left_data = DataSourceService.get_node(card_pk, pk)
        parent_sid = right_data['source_id']
        child_sid = left_data['source_id']

        parent_table = right_data['tname']
        child_table = left_data['tname']

        join_type = 'inner'

        joins = []
        for each in request.data['joins']:
            joins.append([each['right'], each['join'], each['left']])

        data = DataSourceService.save_new_joins_NEW(
            card_pk, parent_table, parent_sid, child_table,
            child_sid, pk, join_type, joins)

        return Response(data=data)


# {"good_joins": [
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