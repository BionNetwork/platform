# coding: utf-8
from __future__ import unicode_literals

import json
from itertools import groupby

from django.db.models import Q
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404
from django.core.urlresolvers import reverse
from django.conf import settings
from django.db import transaction
from django.http import HttpResponse

from core.exceptions import ResponseError, ValidationError, ExceptionCode, TaskError
from core.helpers import CustomJsonEncoder
from core.views import BaseView, BaseTemplateView, JSONMixin
from core.models import (
    Datasource, Queue, QueueList, QueueStatus,
    DatasourceSettings as SourceSettings, DatasourceSettings,
    DatasourceMetaKeys)
from . import forms as etl_forms
import logging

from etl.constants import *
from etl.models import TableTreeRepository
from etl.services.datasource.base import DataSourceService
from etl.services.datasource.repository.storage import RedisSourceService
# from etl.tasks import create_dataset
from .services.queue.base import TaskStatusEnum
from .services.middleware.base import (
    generate_columns_string, generate_columns_string_NEW,
    generate_table_name_key, generate_cube_key)

logger = logging.getLogger(__name__)


SUCCESS = 'success'
ERROR = 'error'


class BaseEtlView(BaseView):

    @staticmethod
    def try_to_get_source(request):
        method = request.GET if request.method == 'GET' else request.POST
        d = {
            "user_id": request.user.id,
            "id": method.get('sourceId'),
        }
        return Datasource.objects.get(**d)

    def get(self, request, *args, **kwargs):
        # try:
            return self.request_method(request, *args, **kwargs)
        # # TODO: Надо убрать Exception
        # except Exception as e:
        #     return self.json_response({'status': 'error', 'message': e.message})

    def post(self, request, *args, **kwargs):
        # try:
            return self.request_method(request, *args, **kwargs)
        # # TODO: Надо убрать Exception
        # except Exception as e:
        #     return self.json_response({'status': 'error', 'message': e.message})

    def request_method(self, request, *args, **kwargs):
        """
        Выполнение запроса с проверкой на существование источника и
        обработкой ошибок при действии
        :param request: WSGIRequest
        :param args: list
        :param kwargs: dict
        :return: string
        """
        # try:
        source = self.try_to_get_source(request)
        if request.method == 'GET':
            data = self.start_get_action(request, source)
        else:
            data = self.start_post_action(request, source)
        return self.json_response(
                {'status': SUCCESS, 'data': data, 'message': ''})

    def start_get_action(self, request, source):
        """
        for get
        :type source: Datasource
        """
        return []

    def start_post_action(self, request, source):
        """
        for post
        :type source: Datasource
        """
        return []


# FIXME: Удалить
class GetDataRowsView(BaseEtlView):

    def start_post_action(self, request, source):
        """

        :type source: Datasource
        """
        data = request.POST.get('cols', '')

        if len(data) == 0:
            raise ValueError("Неверный запрос")

        cols = json.loads(data)
        table_names = []
        col_names = []

        for t_name, col_group in groupby(cols, lambda x: x["table"]):
            table_names.append(t_name)
            col_names += col_group

        data = DataSourceService.get_rows_info(
            source, col_names)

        return data

    def json_response(self, context, **response_kwargs):
        response_kwargs['content_type'] = 'application/json'

        # стараемся отобразить бинарные данные
        new_context_data = []

        for item in context['data']:
            new_list = list(item)
            for i, item in enumerate(new_list):
                try:
                    json.dumps(item, cls=CustomJsonEncoder)
                except Exception:
                    new_list[i] = 'binary'

            new_context_data.append(new_list)

        context['data'] = new_context_data
        return HttpResponse(
            json.dumps(context, cls=CustomJsonEncoder), **response_kwargs)


# FIXME: Удалить
class LoadDataViewMono(BaseEtlView):

    def start_post_action(self, request, source):
        """
        Постановка задачи в очередь на загрузку данных в хранилище
        """
        data = request.POST

        # генерируем название новой таблицы и
        # проверяем на существование дубликатов
        cols = json.loads(data.get('cols'))
        cols_str = generate_columns_string(cols)
        table_key = generate_table_name_key(source, cols_str)

        # берем начальные типы очередей
        queue_ids = Queue.objects.filter(
            name__in=[MONGODB_DATA_LOAD, MONGODB_DELTA_LOAD]).values_list(
            'id', flat=True)
        # берем статусы (В ожидании, В обработке)
        queue_status_ids = QueueStatus.objects.filter(
            title__in=(TaskStatusEnum.IDLE, TaskStatusEnum.PROCESSING)
        ).values_list('id', flat=True)

        queues_list = QueueList.objects.filter(
            checksum__isnull=False,
            checksum=table_key,
            queue_id__in=queue_ids,
            queue_status_id__in=queue_status_ids,
        )

        if queues_list.exists():
            raise ResponseError(u'Данная задача уже находится в обработке!', ExceptionCode.ERR_TASK_ALREADY_IN_QUEUE)

        tables = json.loads(data.get('tables'))

        collections_names = DataSourceService.get_collections_names(
            source, tables)

        # достаем инфу колонок (статистика, типы, )
        # tables_info_for_meta = DataSourceService.tables_info_for_metasource(
        #   source, tables)

        try:
            cdc_type = DatasourceSettings.objects.get(
                    datasource_id=source.id, name=DatasourceSettings.SETTING_CDC_NAME).value
        except DatasourceSettings.DoesNotExist:
            raise ResponseError(u'Не определен тип дозагрузки данных', ExceptionCode.ERR_CDC_TYPE_IS_NOT_SET)

        user_id = request.user.id
        # Параметры для задач
        load_args = {
            'cdc_type': cdc_type,
            'cols': data.get('cols'),
            'tables': data.get('tables'),
            # 'col_types': json.dumps(
            #     DataSourceService.get_columns_types(source, tables)),
            # 'meta_info': json.dumps(tables_info_for_meta),
            # 'tree': RedisSourceService.get_active_tree_structure(source),
            'source': source.get_connection_dict(),
            'user_id': user_id,
            'db_update': False,
            'collections_names': collections_names,
            'checksum': table_key,
            'tables_info': RedisSourceService.get_ddl_tables_info(
                    source, tables),
        }
        db_update = False
        meta_key = DatasourceMetaKeys.objects.filter(value=table_key)
        if meta_key:
            db_update = True
            try:
                meta_stats = json.loads(
                    meta_key[0].meta.stats)['tables_stat']['last_row']['cdc_key']
            except:
                pass
        load_args.update({'db_update': db_update})

        # try:
        #     task, channels = get_single_task(
        #         CREATE_DATASET, create_dataset, load_args)
        # except TaskError as e:
        #     raise ResponseError(e.message)
        #
        # return {'channels': channels}
