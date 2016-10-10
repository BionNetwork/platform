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

from api.serializers import (
    DatasourceSerializer, NodeSerializer, TreeSerializer,
    TreeSerializerRequest, ParentIdSerializer, IndentSerializer,
    LoadDataSerializer, ColumnValidationSeria, DatasetSerializer)

from core.models import (Datasource, Dataset, DatasetStateChoices)
from etl.tasks import load_data
from etl.services.datasource.base import (DataSourceService, DataCubeService)
from etl.helpers import group_by_source
from etl.services.exceptions import SheetException
from etl.services.datasource.source import EmptyEnum

from etl.constants import *

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
    def set_indent(self, request, pk):
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

        return Response('Setted!')


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


# TODO decorator for existing Cube pk
class CubeViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Реализация методов карточки
    """
    model = Dataset
    serializer_class = DatasetSerializer

    def get_queryset(self):
        return self.model.objects.all()

    def list(self, request, *args, **kwargs):
        return super(CubeViewSet, self).list(request, *args, **kwargs)


    # @detail_route(methods=['post'])
    # def clear_cache(self, request, pk):
    #     """
    #     Очистка инфы карточки из редиса
    #     """
    #     worker = DataCubeService(cube_id=pk)
    #     worker.cache.clear_cube_cache()
    #
    #     return Response()

    # FIXME подумать передавать ли сюда список колонок,
    # FIXME все колонки источника нам не нужны
    @detail_route(['post'], serializer_class=TreeSerializerRequest)
    def tree(self, request, pk):

        data = json.loads(request.data.get('data'))

        # data = [
        #     {"source_id": 92, "table_name": 'Таблица1', },
        #     {"source_id": 91, "table_name": 'Лист1', },
        #     {"source_id": 92, "table_name": 'Таблица3', },
        #     {"source_id": 91, "table_name": 'Лист2', },
        #     {"source_id": 90, "table_name": 'TDSheet', },
        #     {"source_id": 89, "table_name": 'Sheet1', },
        # ]

        serializer = self.serializer_class(data=data, many=True)
        if serializer.is_valid():
            worker = DataCubeService(cube_id=pk)

            for each in data:
                sid, table = each['source_id'], each['table_name']
                node_id = worker.cache.get_table_id(sid, table)

                if node_id is None:
                    node_id = worker.cache_columns(sid, table)
                    worker.add_randomly_from_remains(node_id)

            info = worker.get_tree_api()

            return Response(info)

        raise APIException("Invalid args!")

    @detail_route(['post'])
    def exchange_file(self, request, pk):
        pass

    @detail_route(['post'], serializer_class=ColumnValidationSeria)
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

    @detail_route(['post'], serializer_class=ColumnValidationSeria)
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
        """
        d = {
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

        QueryGenerate(pk, d).parse()

    @detail_route(['post', ], serializer_class=LoadDataSerializer)
    def data(self, request, pk):
        """
        Начачло загрузки данных
        """
        if pk is None:
            raise Exception("Cube ID is None!")

        sources_info = json.loads(request.data.get('data'))

        # sources_info = {
        #     '90':
        #         {
        #             "TDSheet": [
        #                 "Дата",
        #                 "Организация",
        #                 # "Остаток",
        #                 # "Дебетовый остаток",
        #                 "Выручка",
        #                 "ВыручкаБезНДС",
        #                 "НоменклатурнаяГруппа",
        #                 "Контрагент",
        #                 "ДоговорКонтрагента",
        #                 # "Регистратор",
        #                 "Проект",
        #             ],
        #         },
            # '89':
            #     {
            #         "Sheet1": [
            #             "d.1",
            #             "Unnamed: 2",
            #             # 4,
            #             # "d",
            #         ]
            #     }
        # }

        # TODO возможно валидацию перенести в отдельный файл
        if not sources_info:
            return Response(
                {"message": "Data is empty!"})

        worker = DataCubeService(cube_id=pk)

        # проверка на пришедшие колонки, лежат ли они в редисе,
        # убираем ненужные типы (бинари)

        # группируем по соурс id на всякий
        sources_info = group_by_source(sources_info)

        # проверяем наличие соурс id в кэше
        uncached = worker.check_sids_exist(list(sources_info.keys()))
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

        dc = DatasetContext(cube_id=pk, sources_info=sources_info)
        try:
            dc.create_dataset()
        except ContextError:
            return Response({"message": "Подобная карточка уже существует"})
        load_d = load_data(dc.context)

        return Response(load_d)
        # return Response({"message": "Loading is started!"})

    @detail_route(['get', ], serializer_class=LoadDataSerializer)
    def get_filters(self, request, pk):
        """
        Список значений для фильтров куба
        """
        worker = DataCubeService(cube_id=pk)

        meta = worker.get_cube_columns()
        return Response(meta)


# TODO: Перенести куда-нибудь
class DatasetContext(object):
    """
    Контекст выполениния задач
    """

    def __init__(self, cube_id, is_update=False, sources_info=None):
        """

        Args:
            cube_id(int): id Карточки
            is_update(bool): Обновление?
            sources_info(dict): Данные загрузки
            ::
            ['<source_1>':
                {
                    "shops": ['<column_1>, <column_2>, <column_3>]
                },
            '<source_2>':
                {
                    "<table_1>": [<column_1>, <column_2>],
                    "<table_2": [<column_1>, <column_2>]
                }
            ...

        ]
        """
        self.cube_id = cube_id
        self.is_update = is_update
        self.sources_info = sources_info
        self.cube_service = DataCubeService(cube_id=cube_id)

        try:
            self.dataset = Dataset.objects.get(key=cube_id)
            self.is_new = False
        except Dataset.DoesNotExist:
            self.dataset = Dataset()
            self.is_new = True

    @property
    def context(self):
        """
        Контекста выполнения задач

        Returns:
            dict:
        """
        if self.is_new:
            sub_trees = self.cube_service.prepare_sub_trees(self.sources_info)
            for sub_tree in sub_trees:
                sub_tree.update(
                    view_name='{type}{view_hash}'.format(
                        type=VIEW, view_hash=sub_tree['collection_hash']),
                    table_name='{type}{view_hash}'.format(
                        type=STTM, view_hash=sub_tree['collection_hash']))
                for column in sub_tree['columns']:
                    column.update(click_column=CLICK_COLUMN.format(column['hash']))

            relations = self.cube_service.prepare_relations(sub_trees)
            self.is_new = False
            return {
                'warehouse': CLICK_TABLE.format(self.cube_id),
                'cube_id': self.cube_id,
                'is_update': False,
                'sub_trees': sub_trees,
                "relations": relations,
            }
        else:
            return self.dataset.context

    def create_dataset(self):
        if self.is_new:
            self.dataset.key = self.cube_id
            self.dataset.context = self.context
            self.dataset.state = DatasetStateChoices.IDLE
            self.dataset.save()
        else:
            raise ContextError('Dataset already exist')

    @property
    def state(self):
        return self.dataset.state

    @state.setter
    def state(self, value):
        self.dataset.state = value
        self.dataset.save()


class ContextError(Exception):
    pass


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

    serializer_class = NodeSerializer

    def list(self, request, cube_pk):
        """
        Список узлов дерева и остаткa
        """
        worker = DataCubeService(cube_id=cube_pk)
        data = worker.get_tree_api()

        # FIXME доделать валидатор
        # serializer = NodeSerializer(data=data, many=True)
        # if serializer.is_valid():
        return Response(data=data)

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
        "joins":
        [{
            "right": "group_id",
            "join": "eq",
            "left": "id"
        }, {...}]
}
        """

        worker = DataCubeService(cube_id=cube_pk)

        left_node = worker.get_node(node_pk)
        right_node = worker.get_node(pk)

        join_type = 'inner'

        joins = []
        for each in request.data['joins']:
            joins.append([each['left'], each['join'], each['right']])

        data = worker.save_new_joins(
            left_node, right_node, join_type, joins)

        return Response(data=data)
