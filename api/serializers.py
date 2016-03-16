import logging
import json
from django.db import transaction
from rest_framework import serializers
from rest_framework.exceptions import APIException
import xmltodict
from core.models import User, Datasource, DatasourceSettings, Cube
from etl.services.olap.base import send_xml, OlapServerConnectionErrorException

logger = logging.getLogger(__name__)


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = (
            'id', 'username', 'last_name', 'is_active', 'is_superuser',
            'is_staff', 'phone', 'skype', 'site', 'city', 'middle_name',
            'birth_date', 'verify_email_uuid', 'avatar_small', 'avatar')
        depth = 1


class SettingsField(serializers.RelatedField):
    def to_representation(self, value):
        return json.dumps({'name': value.name, 'value': value.value})

    def to_internal_value(self, data):
        return DatasourceSettings(**json.loads(data))


class DatasourceSerializer(serializers.ModelSerializer):
    """"""
    settings = SettingsField(
        many=True, queryset=DatasourceSettings.objects.all())

    class Meta:
        model = Datasource
        fields = ('id', 'user_id', 'db', 'host', 'port', 'login', 'password',
                  'conn_type', 'settings')


class SchemasListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cube
        fields = ('id', 'user', 'create_date', 'name', 'data')

    def save(self, **kwargs):
        name = self.data['name']
        user = self.data['user']
        data = self.data['data']
        try:
            with transaction.atomic():
                try:
                    cube = Cube.objects.get(
                        name=name,
                        user_id=user,
                    )
                    cube.data = data
                    cube.save()
                except Cube.DoesNotExist:
                    cube = Cube.objects.create(
                        name=name,
                        user_id=user,
                        data=data,
                    )

                send_xml(name, cube.id, data)
        except OlapServerConnectionErrorException as e:
            logger.error("Can't connect to OLAP Server!\n" + e.message +
                         "\nCube data:\n" + data)
            raise APIException(e.message)
        except Exception as e:
            logger.error("Error creating cube by key" + name + "\n" +
                         e.message + "\nCube data:\n" + data)
            raise APIException(e.message)
        return cube


class SchemasRetreviewSerializer(serializers.BaseSerializer):
    def to_representation(self, obj):
        return xmltodict.parse(obj.data)
