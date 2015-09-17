# coding: utf-8
from __future__ import unicode_literals

from django.db.models import Q
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404
from django.core.urlresolvers import reverse
from django.conf import settings

from core.views import BaseView, BaseTemplateView
from core.models import Datasource
import forms as etl_forms
import logging

import helpers
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

        result = helpers.check_connection(post)
        if not result:
            return self.render_to_response(
                {'form': form,
                 'error_message': 'Подключение не удалось! Подключение не сохранено!'})

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

        result = helpers.check_connection(post)
        if not result:
            return self.render_to_response(
                {'form': form,
                 'error_message': 'Подключение не удалось! Подключение не сохранено!'})

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
            result = helpers.check_connection(request.POST)
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
            dbs = helpers.get_db_info(request.user.id, source)
        except ValueError as err:
            return self.json_response({'status': 'error', 'message': err.message})

        return self.json_response({'data': dbs, 'status': 'success'})
