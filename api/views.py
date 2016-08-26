# coding: utf-8
from __future__ import unicode_literals

import json
import logging

import requests
from django.db import transaction
from django.http.response import HttpResponse

from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework import viewsets, generics, mixins
from rest_framework.views import APIView
from rest_framework.authtoken.models import Token

from api.serializers import (
    UserSerializer, DatasourceSerializer, SchemasListSerializer,
    SchemasRetreviewSerializer, NodeSerializer, TreeSerializer,
    TreeSerializerRequest, ParentIdSerializer, IndentSerializer,
    LoadDataSerializer)

from core.models import (Cube, User, Datasource, Dimension, Measure,
                         DatasourceMetaKeys)
from core.views import BaseViewNoLogin
from etl.multitask import load_data
from etl.services.datasource.base import DataSourceService
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
from etl.helpers import group_by_source
from etl.services.excepts import SheetExcept

from rest_framework.decorators import detail_route

logger = logging.getLogger(__name__)


SUCCESS = 'success'
ERROR = 'error'


class ErrorCodes(object):
    KEY_CODE = 4000

    values = {
        KEY_CODE: "Invalid key!"
    }


ERRC = ErrorCodes


# FIXME потом унаследовать всех от этого класса
class TokenRequired(APIView):

    def dispatch(self, *args, **kwargs):

        request = args[0]
        data = getattr(request, request.method)

        key = data.get('api_auth_token', '')
        token = Token.objects.get()

        if key == token.key:
            return super(TokenRequired, self).dispatch(*args, **kwargs)

        return HttpResponse(json.dumps({
            'code': ERRC.KEY_CODE,
            'message': ERRC.values[ERRC.KEY_CODE],
        }))


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

        try:
            data = service.fetch_tables_columns([table_name], indents)
        except SheetExcept as e:
            raise APIException(e)

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


def send(data, settings=None, stream=False):
    """
    """
    for query in data:
        r = requests.post('http://localhost:8123/', data=query, stream=stream)
        if r.status_code != 200:
            raise Exception(r.text)


class CardViewSet(viewsets.ViewSet):
    """
    Реализация методов карточки
    """

    serializer_class = TreeSerializer

    @detail_route(methods=['post'])
    def clear_cache(self, request, pk):
        """
        Очистка инфы карточки из редиса
        """
        worker = DataSourceService(card_id=pk)
        worker.cache.clear_card_cache()

        return Response()

    @detail_route(['post'], serializer_class=TreeSerializerRequest)
    def create_tree(self, request, pk):

        data = request.data

        data = [
            # for server
            # {"source_id": 4, "table_name": u'auth_group', },
            # {"source_id": 4, "table_name": u'auth_group_permissions', },
            # {"source_id": 2, "table_name": u'auth_group', },
            # {"source_id": 2, "table_name": u'auth_group_permissions', },
            # {"source_id": 2, "table_name": u'auth_permission', },
            # {"source_id": 2, "table_name": u'auth_permission2', },
            # {"source_id": 2, "table_name": u'card_card', },
            # {"source_id": 1, "table_name": u'Лист1', },
            # {"source_id": 1, "table_name": u'List3', },
            # {"source_id": 1, "table_name": u'Лист2', },

            # {"source_id": 31, "table_name": 'kladr_kladrgeo', },
            {"source_id": 36, "table_name": 'auth_group', },
        ]

        info = []

        serializer = self.serializer_class(data=data, many=True)
        if serializer.is_valid():

            worker = DataSourceService(card_id=pk)

            for each in data:
                sid, table = each['source_id'], each['table_name']
                node_id = worker.cache.get_table_id(sid, table)

                if node_id is None:
                    node_id = worker.cache_columns(sid, table)
                    info = worker.add_randomly_from_remains(node_id)

        return Response(info)

    @detail_route(['post'])
    def exchange_file(self, request, pk):
        pass

    @detail_route(['post'] )
    def update_data(self, request, pk):
        """
        Обновление данных
        Args:
            request:
            pk: id карточки

        """
        return Response()

    @detail_route(['post'])
    def query(self, request, pk):
        """
        Формирование запроса
        Args:
            request:
            pk:

            ::
[{
"Y": {"field_name":"price","aggregation": "sum"},
"X": {"field_name":"time","period":[1, 3],"discrete":"week"},
"filters": [{"field_name":"company","value": ["etton"]}, {"field_name":"color","value": ["blue"]}]
}]

        Returns:

        """
        table = ''
        data = request.data[0]

        x_field = 'toRelativeWeekNum({field}) AS {field}_week'.format(field=data['X']['field_name'])
        y_field = '{aggregation}({field})'.format(aggregation=data['Y']['aggregation'], field=data['Y']['field_name'])

        fields = ', '.join([x_field, y_field])

        x_field_name = '{field}_week'.format(field=data['X']['field_name'])

        condition = ' AND '.join(['{field} IN {resolve_fields}'.format(
                field=fltr['field_name'],
                resolve_fields=repr(fltr['value']).replace('[', '(').replace(']', ')')) for fltr in data['filters']])
        # Если есть переодичность, то добавяем секцию BETWEEN
        if data['X']['period']:
            condition += ' AND ({field} BETWEEN {left} AND {right})'.format(
                field=data['X']['field_name'], left=data['X']['period'][0], right=data['X']['period'][1])

        query = "SELECT {fields} FROM {table} WHERE {condition} GROUP BY {group_by_field}".format(
            fields=fields, table=table, condition=condition, group_by_field=x_field_name)

        send([query])

    @detail_route(['post', ], serializer_class=LoadDataSerializer)
    def load_data(self, request, pk):
        """
        Начачло загрузки данных
        """

        if pk is None:
            raise Exception("Card ID is None!")

        # columns = json.loads(post.get('columns'))

        sources_info = {
            # '5':
            #     {
            #         "Таблица1": ['name', 'gender', 'age'],
            #         "Таблица2": ['name', 'country2']
            #     },
            # '3':
            #     {
            #         'shops': ['name']
            #     }
            # '8':
            #     {
            #         "mrk_reference": ["pubmedid", "creation_date"],
            #     },
            # '4':
            #     {
            #         "auth_group": ["name", "id", ],
            #         "auth_group_permissions": [
            #             "id", "group_id", "permission_id",],
            #     },
            # '1':
            #     {
            #         "Лист1": [
            #             "name2", "пол", "auth_group_id", "Floata", ],
            #         "List3": ["name2", "some_id", ],
            #     },
            # '2':
            #     {
            #         "auth_group": ["name", "id", ],
            #         "auth_group_permissions": [
            #             "id", "group_id", "permission_id",],
            #     },
            # '31':
            #     {
            #         "kladr_kladrgeo": [
            #             "id", "parent_id", "name", "socr", "code", "zipcode",
            #             "gni", "uno", "okato", "status", "level",
            #         ],
            #     },
            '36':
                {
                    "auth_group": ["num", "name2", ],
                },
        }

        # TODO возможно валидацию перенести в отдельный файл
        if not sources_info:
            return Response(
                {"message": "Data is empty!"})

        worker = DataSourceService(card_id=pk)

        # проверка на пришедшие колонки, лежат ли они в редисе,
        # убираем ненужные типы (бинари)

        # группируем по соурс id на всякий
        sources_info = group_by_source(sources_info)

        # проверяем наличие соурс id в кэше
        uncached = worker.check_sids_exist(sources_info.keys())
        if uncached:
            return Response(
                {"message": "Uncached source IDs: {0}!".format(uncached)})

        # проверяем наличие ключей, таблиц, колонок в кэше
        uncached_tables, uncached_keys, uncached_columns = (
            worker.check_cached_data(sources_info))

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
        range_ = worker.check_tables_with_tree_structure(sources_info)
        if range_:
            return Response({
                "message": "Tables {0} in tree, but didn't come! ".format(
                 range_)})

        # TODO проверить пришедшие точно лежат в активных, не в остатках

        # cols_str = generate_columns_string_NEW(columns_info)
        # cube_key = generate_cube_key(cols_str, pk)

        # список структур со всей инфой для загрузки
        sub_trees = worker.prepare_sub_trees(sources_info)

        # строим связи между таблицами
        relations = worker.prepare_relations(sub_trees)

        # Параметры для задач
        load_args = {
            'card_id': pk,
            'is_update': False,
            'sub_trees': sub_trees,
            # TODO решить как должен выглядеть cube_key, пока card_id
            'cube_key': pk,
            "relations": relations,
        }

        load_data(load_args)

        return Response({"message": "Loading is started!"})


def check_parent(func):
    """
    Проверка ID родителя на существование
    """
    def inner(*args, **kwargs):
        request = args[1]
        card_id = int(kwargs['card_pk'])
        parent_id = int(request.data.get('parent_id'))

        worker = DataSourceService(card_id=card_id)

        if not worker.check_node_id_in_builder(
                parent_id, in_remain=False):
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

            worker = DataSourceService(card_id=card_id)

            # проверка узла родителя
            if not worker.check_node_id_in_builder(
                    node_id, in_remain):
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
        worker = DataSourceService(card_id=card_pk)
        data = worker.get_tree_api()

        # FIXME доделать валидатор
        # serializer = NodeSerializer(data=data, many=True)
        # if serializer.is_valid():
        return Response(data=data)

    def retrieve(self, request, card_pk=None, pk=None):
        """
        Инфа ноды
        """
        try:
            worker = DataSourceService(card_id=card_pk)
            data = worker.get_node_info(pk)
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
        worker = DataSourceService(card_id=card_pk)
        info = worker.reparent(request.data['parent_id'], pk)

        return Response(info)
        # except Exception as ex:
        #     raise APIException(ex.message)

    @detail_route(methods=['get'])
    @check_child(in_remain=False)
    def to_remain(self, request, card_pk, pk):
        """
        Добавлеине узла дерева в остатки
        """
        worker = DataSourceService(card_id=card_pk)
        info = worker.send_nodes_to_remains(node_id=pk)

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
        worker = DataSourceService(card_id=card_pk)

        info = worker.add_randomly_from_remains(node_id)
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
            worker = DataSourceService(card_id=card_pk)
            info = worker.from_remain_to_certain(parent_id, pk)

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
        worker = DataSourceService(card_id=card_pk)
        data = worker.get_columns_and_joins(int(node_pk), int(pk))

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

        worker = DataSourceService(card_id=card_pk)

        left_node = worker.get_node(node_pk)
        right_node = worker.get_node(pk)

        join_type = 'inner'

        joins = []
        for each in request.data['joins']:
            joins.append([each['left'], each['join'], each['right']])

        data = worker.save_new_joins(
            left_node, right_node, join_type, joins)

        return Response(data=data)
