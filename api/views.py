# coding: utf-8
from __future__ import unicode_literals


from core.views import BaseView


class ImportSchemaView(BaseView):

    def put(self, request, *args, **kwargs):
        return self.json_response({'id': 'some_cube_id'})


class ExexcuteQueryView(BaseView):

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': 'success'})


class SchemasListView(BaseView):

    def get(self, request, *args, **kwargs):
        return self.json_response([
                {'name': 'Schecma1',
                 'create_date': '2012-12-12',
                 'user_id': 12},
            ])


class GetSchemaView(BaseView):

    def get(self, request, *args, **kwargs):
        cube_id = kwargs.get('id')
        return self.json_response({'cube_id': cube_id})
