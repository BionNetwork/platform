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
from . import r_server


logger = logging.getLogger(__name__)


class SourcesListView(BaseTemplateView):

    template_name = 'etl/datasources/index.html'

    def get(self, request, *args, **kwargs):

        get = request.GET
        or_cond = Q()

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
        source_id = kwargs.get('id')
        source = get_object_or_404(Datasource, pk=source_id)
        form = etl_forms.SourceForm(post, instance=source)

        if not form.is_valid():
            return self.render_to_response({'form': form, })

        if form.has_changed() and settings.USE_REDIS_CACHE:
            # отыскиваем ключи для удаления
            user_db_key = helpers.RedisCacheKeys.get_user_databases(request.user.id)
            user_datasource_key = helpers.RedisCacheKeys.get_user_datasource(request.user.id, source_id)

            r_server.lrem(user_db_key, 1, source_id)
            r_server.delete(user_datasource_key)

        form.save()

        return self.redirect('etl:datasources.index')


class RemoveSourceView(BaseView):

    def post(self, request, *args, **kwargs):
        user = get_object_or_404(Datasource, pk=kwargs.get('id'))
        user.delete()

        return self.json_response({'redirect_url': reverse('etl:datasources.index')})


class CheckConnectionView(BaseView):

    def post(self, request, *args, **kwargs):
        try:
            result = helpers.Database.check_connection(request.POST)
            return self.json_response(
                {'status': 'error' if not result else 'success',
                 'message': 'Проверка соединения прошла успешно' if result else 'Проверка подключения не удалась'})
        except Exception as e:
            logger.exception(e.message)
            return self.json_response(
                {'status': 'error', 'message': 'Ошибка во время проверки соединения'}
            )


class GetConnectionDataView(BaseView):

    def get(self, request, *args, **kwargs):
        source = get_object_or_404(Datasource, pk=kwargs.get('id'))
        try:
            db = helpers.Database.get_db_info(request.user.id, source)
        except ValueError as err:
            return self.json_response({'status': 'error', 'message': err.message})

        return self.json_response({'data': db, 'status': 'success'})


class GetColumnsView(BaseView):

    def get(self, request, *args, **kwargs):
        get = request.GET
        d = {
            "user_id": request.user.id,
            "host": get.get('host', ''),
            "db": get.get('db', '')
        }
        tables = json.loads(get.get('tables', ''))
        try:
            source = Datasource.objects.get(**d)
        except Datasource.DoesNotExists:
            err_mess = 'Такого источника не найдено!'
        else:
            try:
                columns = helpers.Database.get_columns_info(source, tables)

                return self.json_response({'status': 'ok', 'data': columns, 'message': ''})
            except ValueError as err:
                err_mess = err.message

        if err_mess:
            return self.json_response({'status': 'error', 'message': err_mess})


class GetDataRowsView(BaseView):
    def get(self, request, *args, **kwargs):
        get = request.GET

        d = {
            "user_id": request.user.id,
            "host": get.get('host', ''),
            "db": get.get('db', '')
        }

        try:
            source = Datasource.objects.get(**d)
        except Datasource.DoesNotExists:
            err_mess = 'Такого источника не найдено!'
        else:
            cols = json.loads(get.get('cols', ''))

            table_names = []
            col_names = []

            for t_name, col_group in groupby(cols, lambda x: x["table"]):
                table_names.append(t_name)
                col_names += [x["table"] + "." + x["col"] for x in col_group]

            try:
                data = helpers.Database.get_rows_info(source, table_names, col_names)

                if len(data) > 0:
                    data = helpers.DecimalEncoder.encode(data)

                data = zip(*data)
                new_data = []

                for i in xrange(len(col_names)):
                    t, c = col_names[i].split('.')
                    new_data.append({
                        "table": t,
                        "col": c,
                        "cols": data[i] if data else []
                    })

                return self.json_response({'status': 'ok', 'data': new_data})
            except ValueError as err:
                err_mess = err.message
            except Exception as e:
                logger.exception(e.message)
                err_mess = "Произошла системная ошибка"

        if err_mess:
            return self.json_response({'status': 'error', 'message': err_mess})

