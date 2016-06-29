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
from etl.tasks import create_dataset
from etl.multitask import create_dataset_multi
from .services.queue.base import TaskStatusEnum, get_single_task
from .services.middleware.base import (
    generate_columns_string, generate_columns_string_NEW,
    generate_table_name_key, extract_tables_info, generate_cube_key)

logger = logging.getLogger(__name__)


SUCCESS = 'success'
ERROR = 'error'


class NewSourceView(JSONMixin, BaseTemplateView):
    template_name = 'etl/datasources/add.html'

    def get(self, request, *args, **kwargs):
        form = etl_forms.SourceForm()
        settings_form = etl_forms.SettingsForm()
        # return render(request, self.template_name, {
        #     'form': form, 'settings_form': settings_form})
        return self.render_to_response(
                {'form': form, 'settings_form': settings_form})

    @transaction.atomic()
    def post(self, request, *args, **kwargs):
        try:
            post = request.POST

            cdc_value = post.get('cdc_type')

            if cdc_value not in [SourceSettings.CHECKSUM, SourceSettings.TRIGGERS]:
                raise ValidationError("Неверное значение для метода докачки данных")

            form = etl_forms.SourceForm(post)
            if not form.is_valid():
                raise ValidationError('Поля формы заполнены неверно')

            if Datasource.objects.filter(
                host=post.get('host'), db=post.get('db'), user_id=request.user.id
            ).exists():
                raise ValidationError("Данный источник уже имеется в системе")

            source = form.save(commit=False)
            source.user_id = request.user.id
            source.save()

            # сохраняем настройки докачки
            SourceSettings.objects.create(
                name='cdc_type',
                value=cdc_value,
                datasource=source,
            )

            return self.json_response(
                {'status': SUCCESS, 'redirect_url': reverse('etl:datasources.index')})
        except ValidationError as e:
            return self.json_response(
                {'status': ERROR, 'message': e.message})
        except Exception as e:
            logger.exception(e.message)
            return self.json_response(
                {'status': ERROR,
                 'message': 'Произошла непредвиденная ошибка!'}
            )


class EditSourceView(BaseTemplateView):
    template_name = 'etl/datasources/edit.html'

    def get(self, request, *args, **kwargs):

        source = get_object_or_404(Datasource, pk=kwargs.get('id'))
        try:
            cdc_value = source.settings.get(name='cdc_type').value
        except DatasourceSettings.DoesNotExist:
            cdc_value = SourceSettings.CHECKSUM

        form = etl_forms.SourceForm(instance=source)
        settings_form = etl_forms.SettingsForm(initial={
            'cdc_type_field': cdc_value})
        return self.render_to_response(
            {'form': form, 'settings_form': settings_form,
             'datasource_id': kwargs.get('id')})

    def post(self, request, *args, **kwargs):

        post = request.POST
        source = get_object_or_404(Datasource, pk=kwargs.get('id'))
        form = etl_forms.SourceForm(post, instance=source)

        cdc_value = post.get('cdc_type')
        if cdc_value not in [SourceSettings.CHECKSUM, SourceSettings.TRIGGERS]:
            return self.json_response(
                {'status': ERROR, 'message': 'Неверное значение выбора закачки!'})
        # сохраняем настройки докачки
        source_settings, create = SourceSettings.objects.get_or_create(
            name='cdc_type',
            datasource=source,
        )
        source_settings.value = cdc_value
        source_settings.save()

        if not form.is_valid():
            return self.render_to_response({'form': form, })

        if form.has_changed() and settings.USE_REDIS_CACHE:
            # если что-то изменилось, то удаляем инфу о датасорсе
            DataSourceService.delete_datasource(source)
            # дополнительно удаляем инфу о таблицах, джоинах, дереве
            DataSourceService.tree_full_clean(source)
        form.save()

        return self.json_response(
                {'status': SUCCESS, 'redirect_url': reverse('etl:datasources.index')})


class CheckConnectionView(BaseView):

    def post(self, request, *args, **kwargs):

        try:
            DataSourceService.check_connection(request.POST)
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

        # FIXME в зависимости от типа источника, определить как очищать редис
        # очищаем из редиса инфу дерева перед созданием нового
        DataSourceService.tree_full_clean(source)

        try:
            db_tables = DataSourceService.get_source_tables(source)
        except ValueError as err:
            return self.json_response({'status': ERROR, 'message': err.message})

        return self.json_response({'data': db_tables, 'status': SUCCESS})


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
        # except (Datasource.DoesNotExist, ResponseError) as err:
        #     return self.json_response(
        #         {'status': ERROR, 'code': err.code, 'message': err.message})
        # except Exception as e:
        #     logger.exception(e.message)
        #     return self.json_response({
        #         'status': ERROR,
        #         'message': 'Произошла непредвиденная ошибка'})

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

class RetitleColumnView(BaseEtlView):

    def start_post_action(self, request, source):
        post = request.POST
        table = post.get('table')
        column = post.get('column')
        title = post.get('title')

        DataSourceService.retitle_table_column(
            source, table, column, title)
        return []


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


class RemoveAllTablesView(BaseEtlView):

    def start_get_action(self, request, source):
        delete_ddl = request.GET.get('delete_ddl') == 'true'
        RedisSourceService.tree_full_clean(source, delete_ddl)
        return []


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
        tables_info_for_meta = DataSourceService.tables_info_for_metasource(
            source, tables)

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
            'meta_info': json.dumps(tables_info_for_meta),
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

        try:
            task, channels = get_single_task(
                CREATE_DATASET, create_dataset, load_args)
        except TaskError as e:
            raise ResponseError(e.message)

        return {'channels': channels}


class LoadDataView(BaseView):

    def post(self, request, card_id=None):
        """
        Постановка задачи в очередь на загрузку данных в хранилище
        """
        post = request.POST

        if card_id is None:
            raise Exception("Card ID is None!")

        # columns = json.loads(post.get('columns'))
        columns_info = {
            # '1':
            '2':
                {
                "auth_group": ["id", "name", ],
                "auth_group_permissions": ["id", "group_id", ],
                "auth_permission": ["id", "name", ],
            },
            # '4':
            '1':
                {
                "Лист1": ["auth_group_id", "name2", "пол"],
                "Лист2": ["name2", "join_to_list3", "name"],
                "List3": ["join_to_list3", "some_id", "name2"],
            },
        }

        cols_str = generate_columns_string_NEW(columns_info)
        cube_key = generate_cube_key(cols_str, card_id)

        tree_structure = (
            RedisSourceService.get_active_tree_structure(card_id))

        tables = extract_tables_info(columns_info)
        # достаем инфу колонок (статистика, типы, )
        meta_tables_info = RedisSourceService.tables_info_for_metasource_NEW(
            tables, card_id)

        sub_trees = DataSourceService.prepare_sub_trees(
            tree_structure, columns_info, card_id, meta_tables_info)

        relations = DataSourceService.prepare_relations(sub_trees)

        print sub_trees

        cols_type = {
            'auth_group__id': {
                'type': 'int',
            },
            'auth_group__name': {
                'type': 'text',
                'max_length': 255,
            },
            'auth_group_permissions__id': {
                'type': 'int'
            },
            'auth_group_permissions__group_id': {
                'type': 'int'
            }
        }
        # Параметры для задач
        load_args = {
            'cols_type': json.dumps(cols_type),
            'card_id': card_id,
            'is_update': False,
            'tree_structure': tree_structure,
            'sub_trees': sub_trees,
            'cube_key': cube_key,
            "relations": relations,
        }

        # get_single_task(
        #     CREATE_DATASET_MULTI, create_dataset_multi, load_args)


class GetUserTasksView(BaseView):
    """
        Список юзеровских тасков
    """
    def get(self, request, *args, **kwargs):
        # берем 10 последних инфо каналов юзера
        subscribes = RedisSourceService.get_user_subscribers(
            request.user.id)
        if subscribes:
            user_subscribes = json.loads(RedisSourceService.get_user_subscribers(
                request.user.id))[-10:]
        else:
            user_subscribes = []

        # сами каналы
        channels = []
        for ch in user_subscribes:
            channels.append(ch['channel'])

        return self.json_response({'channels': channels})
