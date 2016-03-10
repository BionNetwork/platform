from rest_framework import serializers
from core.models import User


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ('phone', 'skype', 'site', 'city', 'middle_name', 'birth_date',
                  'verify_email_uuid', 'avatar_small', 'avatar')
        depth = 1

    def create(self, validated_data):
        return User(**validated_data)