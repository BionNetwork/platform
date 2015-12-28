# coding: utf-8
from __future__ import unicode_literals

import json
from itertools import groupby

from django.db.models import Q
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404
from django.core.urlresolvers import reverse
from django.conf import settings

from core.exceptions import ResponseError
from core.views import BaseView, BaseTemplateView
from core.models import Datasource, Queue, QueueList, QueueStatus, DatasourceMetaKeys
from . import forms as etl_forms
import logging

from . import helpers
from . import services
from . import tasks
from etl.constants import *
from etl.services.queue.base import run_task
from etl.tasks import UpdateMongodb, LoadDb, DetectRedundant, DeleteRedundant, \
    LoadMongodb, LoadDimensions, LoadMeasures
from .services.queue.base import TaskStatusEnum
from .services.middleware.base import (generate_columns_string,
                                       generate_table_name_key)

logger = logging.getLogger(__name__)


SUCCESS = 'success'
ERROR = 'error'


class SourcesListView(BaseTemplateView):

    template_name = 'etl/datasources/index.html'

    def get(self, request, *args, **kwargs):

        get = request.GET
        or_cond = Q(user_id=request.user.id)

        search = get.get('search', None)

        if search:
            for field in ('id', 'db', 'login', 'host', 'port', 'password'):
                or_cond |= Q(
                    **{"%s__icontains" % field: search}
                )

        sub_url = '?search={0};page='.format(search or '')

        sources = Datasource.objects.filter(or_cond)
        count = 20

        paginator = Paginator(sources, count)
        page_count = paginator.num_pages

        page = int(get.get('page', 0))
        if page not in xrange(page_count):
            page = 0

        sources = paginator.page(page + 1)

        return self.render_to_response(
            {
                'sources': sources,
                'url': reverse('etl:datasources.index') + sub_url,
                'range': range(page_count),
                'page': page,
                'search': search or ''
            }
        )


class NewSourceView(BaseTemplateView):
    template_name = 'etl/datasources/add.html'

    def get(self, request, *args, **kwargs):
        form = etl_forms.SourceForm()
        return render(request, self.template_name, {'form': form, })

    def post(self, request, *args, **kwargs):
        post = request.POST
        form = etl_forms.SourceForm(post)

        if not form.is_valid():
            return self.render_to_response({'form': form, })

        source = form.save(commit=False)
        source.user_id = request.user.id
        source.save()

        return self.redirect('etl:datasources.index')


class EditSourceView(BaseTemplateView):
    template_name = 'etl/datasources/edit.html'

    def get(self, request, *args, **kwargs):

        source = get_object_or_404(Datasource, pk=kwargs.get('id'))

        form = etl_forms.SourceForm(instance=source)
        return self.render_to_response({'form': form, })

    def post(self, request, *args, **kwargs):

        post = request.POST
        source = get_object_or_404(Datasource, pk=kwargs.get('id'))
        form = etl_forms.SourceForm(post, instance=source)

        if not form.is_valid():
            return self.render_to_response({'form': form, })

        if form.has_changed() and settings.USE_REDIS_CACHE:
            # если что-то изменилось, то удаляем инфу о датасорсе
            helpers.DataSourceService.delete_datasource(source)
            # дополнительно удаляем инфу о таблицах, джоинах, дереве
            helpers.DataSourceService.tree_full_clean(source)
        form.save()

        return self.redirect('etl:datasources.index')


class RemoveSourceView(BaseView):

    def post(self, request, *args, **kwargs):
        source = get_object_or_404(Datasource, pk=kwargs.get('id'))
        source.delete()

        # удаляем инфу о датасорсе
        helpers.DataSourceService.delete_datasource(source)
        # дополнительно удаляем инфу о таблицах, джоинах, дереве
        helpers.DataSourceService.tree_full_clean(source)

        return self.json_response({'redirect_url': reverse('etl:datasources.index')})


class CheckConnectionView(BaseView):

    def post(self, request, *args, **kwargs):

        try:
            helpers.DataSourceService.check_connection(request.POST)
            return self.json_response(
                {'status': SUCCESS,
                 'message': 'Проверка соединения прошла успешно'})
        except ValueError as e:
            return self.json_response({'status': ERROR, 'message': e.message})
        except Exception as e:
            logger.exception(e.message)
            return self.json_response(
                {'status': ERROR,
                 'message': 'Ошибка во время проверки соединения'}
            )


class GetConnectionDataView(BaseView):

    def get(self, request, *args, **kwargs):
        source = get_object_or_404(Datasource, pk=kwargs.get('id'))

        # очищаем из редиса инфу дерева перед созданием нового
        helpers.DataSourceService.tree_full_clean(source)

        try:
            db = helpers.DataSourceService.get_database_info(source)
        except ValueError as err:
            return self.json_response({'status': ERROR, 'message': err.message})

        return self.json_response({'data': db, 'status': SUCCESS})


class BaseEtlView(BaseView):

    @staticmethod
    def try_to_get_source(request):
        method = request.GET if request.method == 'GET' else request.POST
        d = {
            "user_id": request.user.id,
            "host": method.get('host', ''),
            "db": method.get('db', '')
        }
        return Datasource.objects.get(**d)

    def get(self, request, *args, **kwargs):
        try:
            return self.request_method(request, *args, **kwargs)
        except Exception as e:
            return self.json_response({'status': 'error', 'message': e.message})

    def post(self, request, *args, **kwargs):
        try:
            return self.request_method(request, *args, **kwargs)
        except Exception as e:
            return self.json_response({'status': 'error', 'message': e.message})

    def request_method(self, request, *args, **kwargs):
        """
        Выполнение запроса с проверкой на существование источника и обработкой ошибок при действии
        :param request: WSGIRequest
        :param args: list
        :param kwargs: dict
        :return: string
        """
        try:
            source = self.try_to_get_source(request)
            if request.method == 'GET':
                data = self.start_get_action(request, source)
            else:
                data = self.start_post_action(request, source)
            return self.json_response(
                    {'status': SUCCESS, 'data': data, 'message': ''})
        except (Datasource.DoesNotExist, ResponseError) as err:
            return self.json_response(
                {'status': err.code, 'message': err.message})
        except Exception as e:
            logger.exception(e.message)
            return self.json_response({
                'status': ERROR,
                'message': 'Произошла непредвиденная ошибка'})

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


class GetColumnsView(BaseEtlView):

    def start_get_action(self, request, source):
        tables = json.loads(request.GET.get('tables', ''))
        columns = helpers.DataSourceService.get_columns_info(
            source, tables)
        return columns


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

        data = helpers.DataSourceService.get_rows_info(
            source, col_names)
        return data


class RemoveTablesView(BaseEtlView):

    def start_get_action(self, request, source):
        tables = json.loads(request.GET.get('tables'))
        helpers.DataSourceService.remove_tables_from_tree(
            source, tables)
        return []


class RemoveAllTablesView(BaseEtlView):

    def start_get_action(self, request, source):
        helpers.RedisSourceService.tree_full_clean(source)
        return []


class GetColumnsForChoicesView(BaseEtlView):

    def start_get_action(self, request, source):
        parent_table = request.GET.get('parent')
        child_table = request.GET.get('child_bind')
        has_warning = json.loads(request.GET.get('has_warning'))

        data = helpers.DataSourceService.get_columns_and_joins_for_join_window(
            source, parent_table, child_table, has_warning)

        return data


class SaveNewJoinsView(BaseEtlView):

    def start_get_action(self, request, source):
        get = request.GET
        left_table = get.get('left')
        right_table = get.get('right')
        join_type = get.get('joinType')
        joins = json.loads(get.get('joins'))

        data = helpers.DataSourceService.save_new_joins(
            source, left_table, right_table, join_type, joins)

        return data


class LoadDataView(BaseEtlView):

    def start_post_action(self, request, source):
        """
        Постановка задачи в очередь на загрузку данных в хранилище
        :type request: WSGIRequest
        :type source: Datasource
        """

        # подключение к источнику данных
        source_conn = helpers.DataSourceService.get_source_connection(source)
        if not source_conn:
            raise ResponseError(u'Не удалось подключиться к источнику данных!')

        # копия, чтобы могли добавлять
        data = request.POST.copy()

        # генерируем название новой таблицы и
        # проверяем на существование дубликатов
        cols = json.loads(data['cols'])
        cols_str = generate_columns_string(cols)
        table_key = generate_table_name_key(source, cols_str)

        # берем все типы очередей
        queue_ids = Queue.objects.values_list('id', flat=True)
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
            raise ResponseError(u'Данная задача уже находится в обработке!')

        tables = json.loads(data.get('tables'))

        collections_names = helpers.DataSourceService.get_collections_names(
            source, tables)
        # достаем типы колонок
        col_types = helpers.DataSourceService.get_columns_types(source, tables)
        data.appendlist('col_types', json.dumps(col_types))

        # достаем инфу колонок (статистика, типы, )
        tables_info_for_meta = helpers.DataSourceService.tables_info_for_metasource(
            source, tables)
        data.appendlist('meta_info', json.dumps(tables_info_for_meta))

        structure = helpers.RedisSourceService.get_active_tree_structure(source)
        conn_dict = source.get_connection_dict()

        user_id = request.user.id
        arguments = {
            'cols': data['cols'],
            'tables': data['tables'],
            'col_types': data['col_types'],
            'meta_info': data['meta_info'],
            'tree': structure,
            'source': conn_dict,
            'user_id': user_id,
            'db_update': False,
            'collections_names': collections_names,
        }
        dim_measure_args = {
                'user_id': user_id,
                'datasource_id': source.id,
        }

        redundant_args = {
                'user_id': user_id,
            }

        create_tasks = [
            (MONGODB_DATA_LOAD, LoadMongodb, arguments),
            (DB_DATA_LOAD, LoadDb, arguments),
            [
                (GENERATE_DIMENSIONS, LoadDimensions, dim_measure_args),
                (GENERATE_MEASURES, LoadMeasures, dim_measure_args),
            ]
        ]
        update_tasks = [
            # (MONGODB_DELTA_LOAD, UpdateMongodb, arguments),
            # (DB_DATA_LOAD, LoadDb, arguments),
            (DB_DETECT_REDUNDANT, DetectRedundant, redundant_args),
            (DB_DELETE_REDUNDANT, DeleteRedundant, redundant_args),
        ]

        # to_update = DatasourceMetaKeys.objects.filter(value=table_key)
        to_update = True
        channels = []
        try:
            if not to_update:
                for task_info in create_tasks:
                    channels.extend(run_task(task_info, table_key))
            else:
                arguments.update(db_update=True)
                for task_info in update_tasks:
                    channels.extend(run_task(task_info, table_key))
        except Exception as e:
            print e

        return {'channels': channels}


class GetUserTasksView(BaseView):
    """
        Cписок юзеровских тасков
    """
    def get(self, request, *args, **kwargs):
        # берем 10 последних инфо каналов юзера
        channels_info = helpers.RedisSourceService.get_user_subscribers(
            request.user.id)[-10:]

        # сами каналы
        channels = []
        for ch in channels_info:
            channels.append(ch['channel'])

        return self.json_response({'channels': channels})
