# coding: utf-8
from __future__ import unicode_literals

import xmltodict

from core.models import Cube
from core.views import BaseViewNoLogin


class ImportSchemaView(BaseViewNoLogin):

    def post(self, request, *args, **kwargs):
        post = request.POST

        cube = Cube.objects.create(
            name=post.get('cube_key'),
            data=post.get('cube_string'),
            user_id=post.get('user_id'),
        )

        return self.json_response({'id': cube.id})


class ExecuteQueryView(BaseViewNoLogin):

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': 'success'})


class SchemasListView(BaseViewNoLogin):

    def get(self, request, *args, **kwargs):

        cubes = map(lambda x: {
            'user_id': x.user_id,
            'create_date': (x.create_date.strftime("%Y-%m-%d %H:%M:%S")
                            if x.create_date else ''),
            'name': x.name,
        }, Cube.objects.all())

        return self.json_response(cubes)


class GetSchemaView(BaseViewNoLogin):

    def get(self, request, *args, **kwargs):
        cube_id = kwargs.get('id')

        try:
            cube = Cube.objects.get(id=cube_id)
        except Cube.DoesNotExist:
            return self.json_response(
                {'status': 'error', 'message': 'No such schema!'})

        cube_dict = xmltodict.parse(cube.data)

        return self.json_response(cube_dict)
