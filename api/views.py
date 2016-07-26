# coding: utf-8
from __future__ import unicode_literals

import json
import logging

from django.db import transaction

from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework import viewsets, generics, mixins
from rest_framework.views import APIView

from api.serializers import (
    UserSerializer, DatasourceSerializer, SchemasListSerializer,
    SchemasRetreviewSerializer, NodeSerializer, TreeSerializer,
    TreeSerializerRequest, ParentIdSerializer, IndentSerializer,
    LoadDataSerializer)

from core.models import (Cube, User, Datasource, Dimension, Measure,
                         DatasourceMetaKeys)
from core.views import BaseViewNoLogin
from etl.multitask import create_dataset_multi
from etl.services.datasource.base import DataSourceService
from etl.services.datasource.repository.storage import (
    RedisSourceService, CacheService)
from etl.services.middleware.base import (
    extract_tables_info, generate_cube_key, generate_columns_string_NEW)
from etl.helpers import group_by_source
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
from etl.services.queue.base import get_single_task

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
        # DataSourceService.tree_full_clean(source)
        return super(DatasourceViewSet, self).destroy(request, *args, **kwargs)

    @detail_route(methods=['get'])
    def tables(self, request, pk=None):
        source = Datasource.objects.get(id=pk)

        service = DataSourceService.get_source_service(source)
        data = service.get_tables()
        return Response(data)

    # FIXME Maybe needed decorator for source_id
    @detail_route(methods=['post'], serializer_class=IndentSerializer)
    def set_indent(self, request, pk):
        """
        Отступ в соурсах, предположительно в файлах
        """
        post = request.data

        source_id = pk
        sheet = post.get('sheet', None)
        if sheet is None:
            raise APIException("Sheet name is needed!")
        indent = post.get('indent', None)

        if indent is None:
            raise APIException("Indent is needed!")
        try:
            indent = int(indent)
        except Exception:
            raise APIException("Indent is incorrect!")

        DataSourceService.insert_source_indentation(source_id, sheet, indent)

        return Response('Setted!')


class TablesView(APIView):

    def get(self, request, source_id, table_name):
        """
        Получение данных о таблице
        """
        source = Datasource.objects.get(id=source_id)
        service = DataSourceService.get_source_service(source)

        indents = DataSourceService.extract_source_indentation(source_id)

        data = service.fetch_tables_columns([table_name], indents)
        return Response(data)


class TablesDataView(APIView):

    def get(self, request, source_id, table_name):

        source = Datasource.objects.get(id=source_id)
        source_service = DataSourceService.get_source_service(source)

        indents = DataSourceService.extract_source_indentation(source_id)

        data = source_service.get_source_table_rows(
            table_name, limit=1000, offset=0, indents=indents)

        return Response(data=data)


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

        data = [
            {"source_id": 2, "table_name": u'auth_group', },
            {"source_id": 2, "table_name": u'auth_group_permissions', },
            # {"source_id": 2, "table_name": u'auth_permission', },
            # {"source_id": 2, "table_name": u'auth_permission2', },
            # {"source_id": 2, "table_name": u'card_card', },
            # {"source_id": 1, "table_name": u'Лист1', },
            # {"source_id": 1, "table_name": u'List3', },
            # {"source_id": 1, "table_name": u'Лист2', },

            # {"source_id": 31, "table_name": 'kladr_kladrgeo', },
        ]

        card_id = pk

        info = []

        serializer = self.serializer_class(data=data, many=True)
        if serializer.is_valid():

            cache = CacheService(card_id)

            for each in data:
                sid, table = each['source_id'], each['table_name']
                node_id = cache.get_table_id(sid, table)

                if node_id is None:
                    node_id = DataSourceService.cache_columns(
                        card_id, sid, table)

                    info = DataSourceService.add_randomly_from_remains(
                        card_id, node_id)

        return Response(info)

    @detail_route(['post', ], serializer_class=LoadDataSerializer)
    def load_data(self, request, pk):
        """
        Начачло загрузки данных
        """

        if pk is None:
            raise Exception("Card ID is None!")

        # columns = json.loads(post.get('columns'))

        columns_info = {
            # '8':
            #     {
            #         "mrk_reference": ["pubmedid", "creation_date"],
            #     },
            # '1':
            #     {
            #         "Лист1": [
            #             "name2", "пол", "auth_group_id", "Date", "Floata", ],
            #     },
            # '2':
            #     {
            #         "auth_group": ["id", "name", ],
            #         "auth_group_permissions": [
            #             "id", "group_id", "permission_id",],
            #         # 'asdf': ['asdf'],
            #     },
            # '31':
            #     {
            #         "kladr_kladrgeo": [
            #             "id", "parent_id", "name", "socr", "code", "zipcode",
            #             "gni", "uno", "okato", "status", "level",
            #         ],
            #     },
        }

        if not columns_info:
            return Response(
                {"message": "Data is empty!"})

        worker = DataSourceService(card_id=pk)

        # проверка на пришедшие колонки, лежат ли они в редисе,
        # убираем ненужные типы (бинари)

        # группируем по соурс id на всякий
        columns_info = group_by_source(columns_info)

        # проверяем наличие соурс id в кэше
        uncached = worker.check_sids_exist(columns_info.keys())
        if uncached:
            return Response(
                {"message": "Uncached source IDs: {0}!".format(uncached)})

        # проверяем наличие ключей, таблиц, колонок в кэше
        uncached_tables, uncached_keys, uncached_columns = (
            worker.check_tables_columns_exist(columns_info))

        if any([uncached_tables, uncached_keys, uncached_columns]):
            message = ""
            if uncached_tables:
                message += "Uncached tables: {0}! ".format(uncached_tables)
            if uncached_keys:
                message += "No keys for: {0}! ".format(uncached_keys)
            if uncached_columns:
                message += "Uncached columns: {0}! ".format(uncached_columns)

            return Response({"message": message})

        # проверка наличия всех таблиц из дерева в пришедших нам
        range_ = worker.check_tables_with_tree_structure(columns_info)
        if range_:
            return Response({
                "message": "Tables {0} in tree, but didn't come! ".format(
                range_)})

        # убираем колонки с ненужными типами, например бинари
        columns_info = worker.exclude_types(columns_info)

        cols_str = generate_columns_string_NEW(columns_info)
        cube_key = generate_cube_key(cols_str, pk)

        cache = worker.cache

        tree_structure = cache.active_tree_structure

        tables = extract_tables_info(columns_info)
        # достаем инфу колонок (статистика, типы, )
        meta_tables_info = cache.tables_info_for_metasource(tables)

        sub_trees = DataSourceService.prepare_sub_trees(
            tree_structure, columns_info, pk, meta_tables_info)

        relations = DataSourceService.prepare_relations(sub_trees)

        cols_type = {}
        for tree in sub_trees:
            for k, v in tree['columns_types'].iteritems():
                cols_type.update({k: v})

        # Параметры для задач
        load_args = {
            'meta_info': json.dumps(meta_tables_info),
            'card_id': pk,
            'cols_type': json.dumps(cols_type),
            'is_update': False,
            'tree_structure': tree_structure,
            'sub_trees': sub_trees,
            'cube_key': cube_key,
            "relations": relations,
        }

        get_single_task(create_dataset_multi, load_args)

        return Response({"message": "Loading is started!"})


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


def check_child(in_remain):
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
        try:
            data = DataSourceService.get_node_info(card_pk, pk)
        except TypeError:
            raise APIException("Узел должен быть частью дерева(не остаток)")

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

    @detail_route(methods=['get'])
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

    @detail_route(methods=['get'])
    @check_child(in_remain=True)
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
    @check_child(in_remain=True)
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

        left_node = DataSourceService.get_node(card_pk, node_pk)
        right_node = DataSourceService.get_node(card_pk, pk)

        join_type = 'inner'

        joins = []
        for each in request.data['joins']:
            joins.append([each['left'], each['join'], each['right']])

        data = DataSourceService.save_new_joins(
            card_pk, left_node, right_node, join_type, joins)

        return Response(data=data)
