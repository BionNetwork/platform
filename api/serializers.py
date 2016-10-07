# coding: utf-8
import logging

from django.core.exceptions import ValidationError
from rest_framework import serializers
from core.models import User, Datasource, DatasourceSettings, ConnectionChoices, SettingNameChoices

logger = logging.getLogger(__name__)


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = (
            'id', 'username', 'last_name', 'is_active', 'is_superuser',
            'is_staff', 'phone', 'skype', 'site', 'city', 'middle_name',
            'birth_date', 'verify_email_uuid', 'avatar_small', 'avatar')
        depth = 1


class ChoicesField(serializers.Field):
    def __init__(self, choices, **kwargs):
        self._choices = choices
        super(ChoicesField, self).__init__(**kwargs)

    def to_representation(self, obj):
        return self._choices.values[obj]

    def to_internal_value(self, data):
        return getattr(self._choices, data.upper())


class SettingsSerializer(serializers.ModelSerializer):
    name = ChoicesField(choices=SettingNameChoices)

    class Meta:
        model = DatasourceSettings
        fields = ('name', 'value')


class DatasourceSerializer(serializers.ModelSerializer):
    """
    Серилизатор для источника
    """
    settings = SettingsSerializer(many=True)
    conn_type = ChoicesField(choices=ConnectionChoices)

    class Meta:
        model = Datasource
        fields = ('id', 'name', 'db', 'host', 'port', 'login', 'password',
                  'conn_type', 'user_id', 'settings', 'file')

    def update(self, instance, validated_data):
        instance = super(DatasourceSerializer, self).update(
            instance, validated_data)
        return instance

    def create(self, validated_data):
        settings_data = validated_data.pop('settings')
        datasource = Datasource.objects.create(**validated_data)
        for setting_data in settings_data:
            try:
                ds = DatasourceSettings.objects.create(datasource=datasource, **setting_data)
                ds.full_clean()
            except ValidationError:
                raise serializers.ValidationError

        return datasource


class IndentSerializer(serializers.Serializer):

    sheet = serializers.CharField(max_length=200)
    indent = serializers.IntegerField()
    header = serializers.BooleanField()


class TableDataSerializer(serializers.Serializer):

    source_id = serializers.IntegerField()
    table = serializers.CharField(max_length=200)


class TableSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=256)
    owner = serializers.CharField(max_length=256)


class NodeSerializer(serializers.Serializer):

    parent = serializers.CharField(max_length=256, allow_null=True)
    source_id = serializers.IntegerField()
    val = serializers.CharField(max_length=256)
    is_bind = serializers.BooleanField()

    def update(self, instance, validated_data):
        pass


class ParentIdSerializer(serializers.Serializer):

    parent_id = serializers.IntegerField()


class TreeSerializerRequest(serializers.Serializer):

    source_id = serializers.IntegerField(allow_null=False)
    table_name = serializers.CharField(max_length=256, allow_null=False)

    def create(self, validated_data):
        pass


class LoadDataSerializer(serializers.Serializer):
    pass


class TreeSerializer(serializers.Serializer):

    source_id = serializers.IntegerField()
    table_name = serializers.CharField(max_length=256)
    id = serializers.IntegerField()
    dest = serializers.CharField(max_length=256, allow_null=True)
    is_root = serializers.BooleanField()
    is_bind = serializers.BooleanField()
    is_remain = serializers.BooleanField()


class ChangeDestinationSerializer(serializers.Serializer):
    """
    """


class ColumnValidationSeria(serializers.Serializer):
    """
    Проверка аргументов при Проверки значений колонки на определенный тип
    """
    source_id = serializers.IntegerField(required=True, allow_null=False)
    table = serializers.CharField(required=True)
    column = serializers.CharField(required=True)
    type = serializers.CharField(required=True)
    default = serializers.IntegerField(required=True)
