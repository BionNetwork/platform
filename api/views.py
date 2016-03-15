# coding: utf-8
from __future__ import unicode_literals
from rest_framework import viewsets, generics, mixins

import logging
from api.serializers import UserSerializer, DatasourceSerializer, \
    SchemasListSerializer, SchemasRetreviewSerializer

from core.models import Cube, User, Datasource
from core.views import BaseViewNoLogin
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException
from django.db import transaction


logger = logging.getLogger(__name__)


class ImportSchemaView(BaseViewNoLogin):
    """
    Импортирование схемы куба
    """

    def post(self, request, *args, **kwargs):
        post = request.POST
        key = post.get('key')
        data = post.get('data')
        user_id = post.get('user_id')

        try:
            with transaction.atomic():
                try:
                    cube = Cube.objects.get(
                        name=key,
                        user_id=int(user_id),
                    )
                    cube.data = data
                    cube.save()
                except Cube.DoesNotExist:
                    cube = Cube.objects.create(
                        name=key,
                        user_id=int(user_id),
                        data=data,
                    )

                send_xml(key, cube.id, data)

                return self.json_response({'id': cube.id, 'status': 'success'})

        except OlapServerConnectionErrorException as e:
            message_to_log = "Can't connect to OLAP Server!\n" + e.message + "\nCube data:\n" + data
            logger.error(message_to_log)
            message = e.message
        except Exception as e:
            message_to_log = "Error creating cube by key" + key + "\n" + e.message + "\nCube data:\n" + data
            logger.error(message_to_log)
            message = e.message

        return self.json_response({'status': 'error', 'message': message})


class ExecuteQueryView(BaseViewNoLogin):
    """
    Выполнение mdx запроса к данным
    """

    def post(self, request, *args, **kwargs):
        return self.json_response({'status': 'success'})


class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class DatasourceViewSet(viewsets.ModelViewSet):
    queryset = Datasource.objects.all()
    serializer_class = DatasourceSerializer


class SchemasListView(mixins.ListModelMixin,
                  mixins.CreateModelMixin,
                  generics.GenericAPIView):
    queryset = Cube.objects.all()
    serializer_class = SchemasListSerializer

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)


class GetSchemaView(mixins.RetrieveModelMixin,
                    generics.GenericAPIView):
    queryset = Cube.objects.all()
    serializer_class = SchemasRetreviewSerializer

    def get(self, request, *args, **kwargs):
        return self.retrieve(request, *args, **kwargs)


