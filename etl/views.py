# coding: utf-8
from __future__ import unicode_literals

import json
from itertools import groupby

from django.db.models import Q
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404
from django.core.urlresolvers import reverse
from django.conf import settings

from core.views import BaseView, BaseTemplateView
from core.models import Datasource
from . import forms as etl_forms
import logging

from . import helpers
from . import tasks


logger = logging.getLogger(__name__)


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
            result = helpers.DataSourceService.check_connection(request.POST)
            return self.json_response(
                {'status': 'error' if not result else 'success',
                 'message': ('Проверка соединения прошла успешно'
                             if result else 'Проверка подключения не удалась')})
        except Exception as e:
            logger.exception(e.message)
            return self.json_response(
                {'status': 'error', 'message': 'Ошибка во время проверки соединения'}
            )


class GetConnectionDataView(BaseView):

    def get(self, request, *args, **kwargs):
        source = get_object_or_404(Datasource, pk=kwargs.get('id'))

        # очищаем из редиса инфу дерева перед созданием нового
        helpers.DataSourceService.tree_full_clean(source)

        try:
            db = helpers.DataSourceService.get_database_info(source)
        except ValueError as err:
            return self.json_response({'status': 'error', 'message': err.message})

        return self.json_response({'data': db, 'status': 'success'})


class ErrorResponse(object):
    """
        Класс ответа с ошибкой
    """
    def __init__(self, msg):
        self.message = msg


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
        except Datasource.DoesNotExist:
            err_mess = 'Такого источника не найдено!'
        else:
            try:
                if request.method == 'GET':
                    data = self.start_get_action(request, source)
                else:
                    data = self.start_post_action(request, source)

                # возвращаем свое сообщение
                if isinstance(data, ErrorResponse):
                    return self.json_response({'status': 'error', 'message': data.message})

                return self.json_response({'status': 'ok', 'data': data, 'message': ''})
            except Exception as e:
                logger.exception(e.message)
                err_mess = "Произошла непредвиденная ошибка"

        if err_mess:
            return self.json_response({'status': 'error', 'message': err_mess})

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
            col_names += [x["table"] + "." + x["col"] for x in col_group]

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
        is_without_bind = json.loads(request.GET.get('is_without_bind'))

        data = helpers.DataSourceService.get_columns_for_choices(
            source, parent_table, child_table, is_without_bind)

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
            return ErrorResponse('Не удалось подключиться к источнику данных!')

        # копия, чтобы могли добавлять
        data = request.POST.copy()

        tables = json.loads(data.get('tables'))
        # cols = json.loads(data.get('cols'))

        # достаем типы колонок
        col_types = helpers.DataSourceService.get_columns_types(source, tables)
        data.appendlist('col_types', json.dumps(col_types))

        # достаем инфу колонок (статистика, типы, )
        tables_info_for_meta = helpers.DataSourceService.tables_info_for_metasource(
            source, tables)
        data.appendlist('tables_info_for_meta', json.dumps(tables_info_for_meta))

        structure = helpers.RedisSourceService.get_active_tree_structure(source)
        conn_dict = source.get_connection_dict()

        # добавляем задачу mongo в очередь
        task = helpers.TaskService('etl:load_data:mongo')
        task_id1 = task.add_task(request.user.id, data, structure, conn_dict)
        tasks.load_data.apply_async((request.user.id, task_id1),)

        # добавляем задачу database в очередь
        task = helpers.TaskService('etl:load_data:database')
        task_id2 = task.add_task(request.user.id, data, structure, conn_dict)
        tasks.load_data.apply_async((request.user.id, task_id2),)

        # FIXME double task_id, need task_ids

        return {'task_id': task_id2, }


class GetUserTasksView(BaseView):
    """
        Cписок юзеровских тасков
    """
    def get(self, request, *args, **kwargs):
        user_tasks = helpers.RedisSourceService.get_user_tasks(request.user.id)
        return self.json_response({'userId': request.user.id, 'tasks': user_tasks})
