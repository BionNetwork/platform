# coding: utf-8

import json
import logging

import requests
from django.http.response import HttpResponse

from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework import viewsets
from rest_framework.views import APIView
from rest_framework.authtoken.models import Token
from rest_framework import status

from api.serializers import (
    DatasourceSerializer, NodeSerializer,
    TreeSerializerRequest, ParentIdSerializer, IndentSerializer,
    LoadDataSerializer, DatasetSerializer, ColumnsSerializer, SettingsSerializer
)

from core.models import (
    Datasource, Dataset, EmptyEnum, DatasourceSettings,
    ColumnTypeChoices as CTC
)
from etl.tasks import load_data
from etl.services.datasource.base import (DataSourceService, DataCubeService)
from etl.services.exceptions import *
from etl.helpers import group_by_source, DatasetContext, ContextError
from etl.services.exceptions import SheetException


from rest_framework.decorators import detail_route

from query.generate import QueryGenerate

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


# TODO decorator for existing Datasource pk
# TODO replace validation of args
class DatasourceViewSet(viewsets.ModelViewSet):
    model = Datasource
    serializer_class = DatasourceSerializer

    def get_queryset(self):
        return self.model.objects.all()

    def list(self, request, *args, **kwargs):
        return super(DatasourceViewSet, self).list(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        return super(DatasourceViewSet, self).create(request, *args, **kwargs)

    # TODO
    def destroy(self, request, *args, **kwargs):
        source = self.get_object()
        DataSourceService.delete_datasource(source)
        # DataSourceService.tree_full_clean(source)
        return super(DatasourceViewSet, self).destroy(request, *args, **kwargs)

    @detail_route(methods=['get'])
    def tables(self, request, pk=None):
        """
        Список таблиц или листов
        """
        worker = DataSourceService(source_id=pk)
        tables = worker.get_source_tables()
        return Response(tables)

    @detail_route(methods=['post'], serializer_class=IndentSerializer)
    def indent(self, request, pk):
        """
        Отступ в соурсах, предположительно в файлах
        """
        post = request.data

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

        header = post.get('header', True)

        assert isinstance(header, bool), "Header isn't bool!"

        worker = DataSourceService(source_id=pk)
        worker.set_indentation(sheet, indent, header)

        return Response(status=status.HTTP_200_OK)


class TablesView(APIView):

    def get(self, request, source_id, table_name):
        """
        Получение данных о таблице
        """
        worker = DataSourceService(source_id)
        try:
            data = worker.get_source_columns(table_name)
        except SheetException as e:
            raise APIException(e)

        return Response(data)


class TablesDataView(APIView):

    def get(self, request, source_id, table_name):
        """
        Preview for tables and sheets
        """
        worker = DataSourceService(source_id)
        try:
            data = worker.get_source_rows(table_name)
        except SheetException as e:
            raise APIException(e)

        return Response(data=data)


def send(data, settings=None, stream=False):
    """
    """
    for query in data:
        r = requests.post('http://localhost:8123/', data=query, stream=stream)
        if r.status_code != 200:
            raise Exception(r.text)
        try:
            j = r.json()
        except Exception as e:
            # print e.message
            pass
        else:
            return j


class SettingsViewSet(viewsets.ModelViewSet):

    model = DatasourceSettings
    serializer_class = SettingsSerializer

    def get_queryset(self):
        return DatasourceSettings.objects.filter(datasource=self.kwargs['datasource_pk'])

    def list(self, request, *args, **kwargs):
        return super(SettingsViewSet, self).list(request, *args, **kwargs)


# TODO decorator for existing Cube pk
class CubeViewSet(viewsets.ModelViewSet):
    """
    Реализация методов карточки
    """
    model = Dataset
    serializer_class = DatasetSerializer

    def get_queryset(self):
        return self.model.objects.all()

    def list(self, request, *args, **kwargs):
        return super(CubeViewSet, self).list(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        return super(CubeViewSet, self).create(request, *args, **kwargs)

    # FIXME подумать передавать ли сюда список колонок,
    # FIXME все колонки источника нам не нужны
    @detail_route(['post'])
    def tree(self, request, pk):

        # data = json.loads(request.data.get('data'))
        # data = request.data

        # data = [
        #     {"source_id": 92, "original_table": 'Таблица1', },
        #     {"source_id": 91, "original_table": 'Лист1', },
        #     {"source_id": 92, "original_table": 'Таблица3', },
        #     {"source_id": 91, "original_table": 'Лист2', },
        #     {"source_id": 90, "original_table": 'TDSheet', },
        #     {"source_id": 89, "original_table": 'Sheet1', },
        # ]
        # serializer = self.serializer_class(data=data, many=True)
        # if serializer.is_valid():

        worker = DataCubeService(cube_id=pk)
        data = worker.data_for_tree_creation()

        for each in data:
            sid, table = each['source_id'], each['original_table']
            node_id = worker.cache.get_table_id(sid, table)

            if node_id is None:
                node_id = worker.cache_columns(sid, table)
                worker.add_randomly_from_remains(node_id)

        info = worker.get_tree_api()

        return Response(info)

    @detail_route(['post'])
    def exchange_file(self, request, pk):
        pass

    @detail_route(['post'])
    def validate_col(self, request, pk):
        """
        Проверка значений колонки на определенный тип
        """
        post = request.data

        sid = int(post.get('source_id'))
        table = post.get('table')
        column = post.get('column')
        param = post.get('param')
        typ = post.get('type')

        if not all([sid, table, column, param, typ]):
            raise APIException("Invalid args for validate_col!")

        worker = DataCubeService(cube_id=pk)
        result = worker.validate_column(sid, table, column, param, typ)

        return Response({
            "cube_id": pk,
            "source_id": sid,
            "table": table,
            "column": column,
            "param": param,
            "type": typ,
            "errors": result['errors'],
            "nulls": result['nulls'],
        })

    @detail_route(['post'])
    def set_default(self, request, pk):
        """
        Установка дефолтного значения для пустых значений колонки,
        либо удаление таких строк
        """
        post = request.data

        sid = int(post.get('source_id'))
        table = post.get('table')
        column = post.get('column')
        default = int(post.get('default', 0))

        if not all([sid, table, column, default]):
            raise APIException("Invalid args for column_default!")

        if not default in [EmptyEnum.ZERO, EmptyEnum.REMOVE]:
            raise APIException("Invalid value for default!")

        worker = DataCubeService(cube_id=pk)
        worker.set_column_default(sid, table, column, default)

        return Response('Setted!')

    @detail_route(['post'])
    def update_data(self, request, pk):
        """
        Обновление данных
        Args:
            request:
            pk: id карточки

        """
        return Response()

    @detail_route(['post'])
    def delete_source(self, request, pk):
        """
        Удаление источника куба
        """
        post = request.data
        sid = int(post.get('source_id'))

        worker = DataCubeService(cube_id=pk)
        worker.delete_source(source_id=sid)

        return Response()

    @detail_route(['post'])
    def query(self, request, pk):
        """
        Формирование запроса
        Args:
            pk:

        :: {
            "dims": [
            {
                "name": "d_name",
                "type": "DateDim",
                "field": "d",
                "filters": {
                    "range": ["2016-02-28", "2016-03-04"]
                },
                "interval": "toMonday",

            }, {
                "name": "org",
                "type": "TextDim",
                "field": "org",
                "order": "desc",
                "visible": True,
                "filters": {
                    "match": ['Эттон-Центр', 'Эттон'],
                    },
            }, {
                "name": "project",
                "type": "TextDim",
                "field": "project",
                "filters": {
                    "match": ['Татмедиа'],
                },
            }
            ],
            "measures": [
            {
                "name": "val_sum",
                "type": "sum",
                "field": "val",
                "visible": True,
                "filters": {
                    "lte": 3000
                },
            }]
        }
        """
        try:
            data = request.data

            data = QueryGenerate(pk, data).parse()
            return Response(json.loads(data))
        except Exception as e:
            raise APIException("Ошибка обработки запроса")

    @detail_route(['post', ], serializer_class=LoadDataSerializer)
    def data(self, request, pk):
        """
        Начачло загрузки данных

        {"21":
                {
                    "TDSheet": [
                        "Дата",
                        "Организация",
                        "Остаток",
                        "Контрагент"
                    ]
                }
}
        """
        # try:
        if pk is None:
            raise APIException("Cube ID is None!")

        sources_info = json.loads(request.data.get('data'))

        # TODO возможно валидацию перенести в отдельный файл
        if not sources_info:
            raise APIException("Data is empty!")

        worker = DataCubeService(cube_id=pk)

        # проверка на пришедшие колонки, лежат ли они в редисе,
        # убираем ненужные типы (бинари)

        # группируем по соурс id на всякий
        sources_info = group_by_source(sources_info)

        # проверяем наличие соурс id в кэше
        uncached = worker.check_sids_exist(list(sources_info.keys()))
        if uncached:
            raise APIException("Uncached source IDs: {0}!".format(uncached))

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

            raise APIException(message)

        # проверка наличия всех таблиц из дерева в пришедших нам
        range_ = worker.check_tables_with_tree_structure(sources_info)
        if range_:
            raise APIException("Tables {0} in tree, but didn't come! ".format(
                 range_))

        dc = DatasetContext(cube_id=pk, sources_info=sources_info)
        try:
            dc.create_dataset()
        except ContextError:
            raise APIException("Подобная карточка уже существует")
        load_d = load_data(dc.context)

        return Response(load_d)
        # except Exception as err:
        #     raise APIException(err)

    @detail_route(['get', ], serializer_class=LoadDataSerializer)
    def get_filters(self, request, pk):
        """
        Список значений для фильтров куба
        """
        worker = DataCubeService(cube_id=pk)

        meta = worker.get_cube_columns()
        return Response(meta)


class ColumnsViewSet(viewsets.ViewSet):
    """
    Колонки куба
    """

    serializer_class = ColumnsSerializer

    def list(self, request, cube_pk):
        """
        Список узлов дерева и остаткa
        """
        worker = DataCubeService(cube_id=cube_pk)
        columns = worker.cube_columns()

        data = map(lambda c: {
            "id": c.id,
            "cube_id": cube_pk,
            "source_id": c.source_id,
            "param": c.name,
            "table": c.original_table,
            "column": c.original_name,
            "type": CTC.values.get(int(c.type)),
            "default": c.default_val,
        }, columns)

        return Response(data=data)

    def get(self, request, cube_pk, pk):
        """
        Колонка
        """
        worker = DataCubeService(cube_id=cube_pk)
        column = worker.get_column(pk)

        data = {
            "id": column.id,
            "cube_id": cube_pk,
            "source_id": column.source_id,
            "param": column.name,
            "table": column.original_table,
            "column": column.original_name,
            "type": CTC.values.get(int(column.type)),
            "default": column.default_val,
        }

        return Response(data)

    def create(self, request, cube_pk):
        """
        Создание колонки
        """
        post = request.data

        sid = int(post.get('source_id'))
        table = post.get('table')
        column_name = post.get('column')
        param = post.get('param')
        type = post.get('type')
        default = post.get('default', None)

        info = {
            "cube_id": cube_pk,
            "source_id": sid,
            "table": table,
            "column": column_name,
            "param": param,
            "type": type,
        }

        if not all([sid, table, column_name, param]):
            raise APIException("Empty args for validate_col!")

        if type not in CTC.values_list():
            raise APIException("Invalid type!")

        worker = DataCubeService(cube_id=cube_pk)
        result = worker.validate_column(sid, table, column_name, type)

        if result['errors']:
            info["type_errors"] = result['errors']
            return Response(info)

        if default:
            default = int(default)
            if default not in EmptyEnum.keys():
                raise APIException("Invalid default!")
            info["default"] = default

        elif result['nulls']:
            info["nulls"] = result['nulls']
            return Response(info)

        int_type = CTC.get_int_type(type)
        try:
            column = worker.create_column(
                sid, table, column_name, param, int_type, default)
        except BaseExcept as ex:
            raise APIException(ex.message)

        info["id"] = column.id

        return Response(info)

    def update(self, request, cube_pk, pk):
        """
        """
        column_id = pk

        data = request.data

        sid = int(data.get('source_id'))
        table = data.get('table')
        column_name = data.get('column')
        param = data.get('param')
        type = data.get('type', None)
        default = data.get('default', None)

        info = {
            "id": column_id,
            "cube_id": cube_pk,
            "source_id": sid,
            "table": table,
            "column": column_name,
            "param": param,
        }

        worker = DataCubeService(cube_id=cube_pk)

        if not all([sid, table, column_name, param]):
            raise APIException("Empty args for validate_col!")

        if type:
            if type not in CTC.values_list():
                raise APIException("Invalid type!")

            result = worker.validate_column(sid, table, column_name, type)
            if result['errors']:
                info["type"] = type
                info["type_errors"] = result['errors']
                return Response(info)

        if default:
            default = int(default)
            if default not in EmptyEnum.keys():
                raise APIException("Invalid default!")

        int_type = CTC.get_int_type(type or None)
        default = default or None

        try:
            column = worker.update_column(
                column_id, sid, table, column_name, param, int_type, default)
        except BaseExcept as ex:
            raise APIException(ex.message)

        return Response(info)

    def delete(self, request, cube_pk, pk):
        """
        Удаление колонки
        """
        worker = DataCubeService(cube_id=cube_pk)
        try:
            worker.delete_column(pk)
        except BaseExcept as ex:
            raise APIException(ex.message)
        return Response("Deleted!")


def check_parent(func):
    """
    Проверка ID родителя на существование
    """
    def inner(*args, **kwargs):
        request = args[1]
        cube_id = int(kwargs['cube_pk'])
        parent_id = int(request.data.get('parent_id'))

        worker = DataCubeService(cube_id=cube_id)

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
            cube_id = int(kwargs['cube_pk'])

            worker = DataCubeService(cube_id=cube_id)

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
    #
    # [{
    #     "val": "TDSheet",
    #     "is_bind": "false",
    #     "is_root": "true",
    #     "source_id": 21
    # }]

    serializer_class = NodeSerializer

    def list(self, request, cube_pk):
        """
        Список узлов дерева и остаткa
        """
        worker = DataCubeService(cube_id=cube_pk)
        data = worker.get_tree_api()

        return Response(data=data)

    def create(self, request, cube_pk):
        data = json.loads(request.data.get('data'))
        serializer = NodeSerializer(data=data, many=True)
        if serializer.is_valid():
            worker = DataCubeService(cube_id=cube_pk)

            for each in data:
                sid, table = each['source_id'], each['table_name']
                node_id = worker.cache.get_table_id(sid, table)

                if node_id is None:
                    node_id = worker.cache_columns(sid, table)
                    worker.add_randomly_from_remains(node_id)

            info = worker.get_tree_api()

            return Response(info)
        raise APIException("Invalid args!")

    def retrieve(self, request, cube_pk=None, pk=None):
        """
        Инфа ноды
        """
        try:
            worker = DataCubeService(cube_id=cube_pk)
            data = worker.get_node_info(pk)
        except TypeError:
            raise APIException("Узел должен быть частью дерева(не остаток)")

        return Response(data=data)

    @detail_route(methods=['post'], serializer_class=ParentIdSerializer)
    @check_child(in_remain=False)
    @check_parent
    def reparent(self, request, cube_pk, pk):
        """
        Изменение родительского узла, перенос узла с одного места на другое
        Args:
            cube_pk(int): id карточки
            pk(int): id узла
        Returns:
            dict: Информацию об связанных/не связанных узлах дерева и остатка
        """
        # fixme serializer проверить
        # serializer = self.serializer_class(data=request.data, many=True)
        # if serializer.is_valid(raise_exception=True):
        # try:
        worker = DataCubeService(cube_id=cube_pk)
        info = worker.reparent(request.data['parent_id'], pk)

        return Response(info)
        # except Exception as ex:
        #     raise APIException(ex.message)

    @detail_route(methods=['get'])
    @check_child(in_remain=False)
    def to_remain(self, request, cube_pk, pk):
        """
        Добавлеине узла дерева в остатки
        """
        worker = DataCubeService(cube_id=cube_pk)
        info = worker.send_nodes_to_remains(node_id=pk)

        return Response(info)

    @detail_route(methods=['post'])
    def change_source(self):
        """
        Измение источника для узла
        """

    @detail_route(methods=['get'])
    @check_child(in_remain=True)
    def remain_whatever(self, request, cube_pk, pk):
        """
        Перенос узла из остатков в основное дерево
        в произвольное место
        """
        node_id = pk
        worker = DataCubeService(cube_id=cube_pk)

        info = worker.add_randomly_from_remains(node_id)
        return Response(info)

    @detail_route(methods=['post'], serializer_class=ParentIdSerializer)
    @check_child(in_remain=True)
    @check_parent
    def remain_current(self, request, cube_pk, pk):
        """
        Перенос узла из остатков в основное дерево
        в определенный узел

        Args:
            cube_pk(int): id карточки
            pk(int): id узла

        Returns:
            dict: Информацию об связанных/не связанных узлах дерева и остатка
        """
        # fixme serializer проверить
        # serializer = self.serializer_class(data=request.data, many=True)
        # if serializer.is_valid(raise_exception=True):
        try:
            parent_id = request.data['parent_id']
            worker = DataCubeService(cube_id=cube_pk)
            info = worker.from_remain_to_certain(parent_id, pk)

            return Response(info)
        except Exception as ex:
            raise APIException(ex.message)


class JoinViewSet(viewsets.ViewSet):
    """
    Представление для связей таблиц
    """

    def retrieve(self, request, cube_pk=None, node_pk=None, pk=None):
        """
        Получение информации о соединение узлов (join)
        ---
        Args:
            cube_pk(int): id карточки
            node_pk(int): id родительского узла
            pk(int): id дочернего узла

        Returns:
        """
        worker = DataCubeService(cube_id=cube_pk)
        data = worker.get_columns_and_joins(int(node_pk), int(pk))

        return Response(data=data)

    def update(self, request, cube_pk=None, node_pk=None, pk=None):
        """
        Обновление связи между узлами

        Args:
            cube_pk(int): id карточки
            node_pk(int): id родительского узла
            pk(int): id узла-потомка

        {
        "join_type": "inner",
        "joins":
        [{
            "child": "group_id",
            "operator": "eq",
            "parent": "id"
        }, {...}]
}
        """

        worker = DataCubeService(cube_id=cube_pk)

        left_node = worker.get_node(node_pk)
        right_node = worker.get_node(pk)
        data = json.loads(request.data.get('data'))

        join_type = data['type']

        joins = []
        for each in request.data['joins']:
            joins.append([each['parent'], each['operator'], each['child']])

        data = worker.save_new_joins(
            left_node, right_node, join_type, joins)

        return Response(data=data)


d = \
    {
    "22":
        {
            "TDSheet": [
                "Дата",
                "Организация",
                "Выручка",
                "ВыручкаБезНДС",
                "НоменклатурнаяГруппа",
                "Контрагент",
                "ДоговорКонтрагента",
                "Проект"
            ]
        }
}
