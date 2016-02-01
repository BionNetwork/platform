# coding: utf-8
from __future__ import unicode_literals

import xmltodict

from core.models import Cube
from core.views import BaseView


class ImportSchemaView(BaseView):

    def post(self, request, *args, **kwargs):
        # для отправки надо
        # cookies = {'csrftoken': '4gNfNzeAXhNuNZrqnLvw5dvJaNrmOHJ0'}
        # data = {'csrfmiddlewaretoken':'4gNfNzeAXhNuNZrqnLvw5dvJaNrmOHJ0'}

        post = request.POST

        cube = Cube.objects.create(
            name=post.get('cube_key'),
            data=post.get('cube_string'),
            user_id=post.get('user_id'),
        )

        return self.json_response({'id': cube.id})

    def put(self, request, *args, **kwargs):
        # FIXME не посылается на PUTб нужен django_rest_framewЁrk
        return self.json_response({'id': 'some_cube_id'})


class ExexcuteQueryView(BaseView):

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': 'success'})


class SchemasListView(BaseView):

    def get(self, request, *args, **kwargs):

        user_id = request.GET.get('user_id', None)

        # достаем все схемы
        if user_id is None:
            cubes = Cube.objects.filter(user_id=user_id)
        # достаем схемы юзера
        else:
            cubes = Cube.objects.all()

        cubes = map(lambda x: {
            'user_id': x.user_id,
            'create_date': (x.create_date.strftime("%Y-%m-%d %H:%M:%S")
                            if x.create_date else ''),
            'name': x.name,
        }, cubes)

        return self.json_response(cubes)


class GetSchemaView(BaseView):

    def get(self, request, *args, **kwargs):
        cube_id = kwargs.get('id')

        try:
            cube = Cube.objects.get(id=cube_id)
        except Cube.DoesNotExist:
            return self.json_response(
                {'status': 'error', 'message': 'No such schema!'})

        cube_dict = xmltodict.parse(cube.data)

        return self.json_response(cube_dict)
