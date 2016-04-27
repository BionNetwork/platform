# coding: utf-8
import logging
import json
from django.conf import settings
from django.db import transaction
from rest_framework import serializers
from rest_framework.exceptions import APIException
import xmltodict
from core.models import User, Datasource, DatasourceSettings, Cube, \
    CardDatasource
from etl import helpers
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
        return value.value

    def to_internal_value(self, data):

        return DatasourceSettings(**{'name': 'cdc_key', 'value': data})


class DatasourceSerializer(serializers.ModelSerializer):
    """
    Серилизатор для источника
    """
    settings = SettingsField(
        many=True, queryset=DatasourceSettings.objects.distinct('name', 'value'))

    class Meta:
        model = Datasource
        fields = ('id', 'db', 'host', 'port', 'login', 'password',
                  'conn_type', 'user_id', 'settings')

    def update(self, instance, validated_data):
        instance = super(DatasourceSerializer, self).update(
            instance, validated_data)
        if settings.USE_REDIS_CACHE:
            helpers.DataSourceService.delete_datasource(instance)
            helpers.DataSourceService.tree_full_clean(instance)
        return instance


class CardDatasourceSerializer(serializers.ModelSerializer):
    # card_id = serializers.RelatedField(source='card')
    # source_id = serializers.RelatedField(source='source')

    class Meta:
        model = CardDatasource
        fields = ('card', 'source')


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
