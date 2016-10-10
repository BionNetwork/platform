# coding: utf-8
import logging
from rest_framework import serializers
from core.models import User, Datasource, DatasourceSettings
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
                  'conn_type', 'user_id', 'settings', 'file')

    def update(self, instance, validated_data):
        instance = super(DatasourceSerializer, self).update(
            instance, validated_data)
        return instance


class IndentSerializer(serializers.Serializer):

    sheet = serializers.CharField(max_length=200)
    indent = serializers.IntegerField()
    header = serializers.BooleanField()


class TableDataSerializer(serializers.Serializer):

    source_id = serializers.IntegerField()
    table = serializers.CharField(max_length=200)

# ==========================================
# TECT

class Task(object):
    def __init__(self, **kwargs):
        for field in ('id', 'name', 'owner', 'status'):
            setattr(self, field, kwargs.get(field, None))

tasks = {
    1: Task(id=1, name='Demo', owner='xordoquy', status='Done'),
    2: Task(id=2, name='Model less demo', owner='xordoquy', status='Ongoing'),
    3: Task(id=3, name='Sleep more', owner='xordoquy', status='New'),
}


class TaskSerializer(serializers.Serializer):
    id = serializers.IntegerField(read_only=True)
    name = serializers.CharField(max_length=256)
    owner = serializers.CharField(max_length=256)

    def update(self, instance, validated_data):
        for field, value in list(validated_data.items()):
            setattr(instance, field, value)
        return instance

    def create(self, validated_data):
        return Task(id=None, **validated_data)

# =======================================


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
    param = serializers.CharField(required=True)
    type = serializers.CharField(required=True)
    default = serializers.IntegerField(required=True)
