from rest_framework import serializers
from core.models import User, Datasource, DatasourceSettings


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ('phone', 'skype', 'site', 'city', 'middle_name', 'birth_date',
                  'verify_email_uuid', 'avatar_small', 'avatar')
        depth = 1

    def create(self, validated_data):
        return User(**validated_data)


class SettingsField(serializers.RelatedField):
    def to_representation(self, value):
        return [value.id, value.name, value.value]


class DatasourceSerializer(serializers.HyperlinkedModelSerializer):
    settings = SettingsField(many=True, queryset=DatasourceSettings.objects.all())
    class Meta:
        model = Datasource
        fields = ('id', 'db', 'host', 'port', 'login', 'password', 'conn_type', 'settings')