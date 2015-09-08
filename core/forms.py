# coding: utf-8
from __future__ import unicode_literals

__author__ = 'damir'

from django import forms
from django.core.exceptions import ValidationError

from models import User


class UserForm(forms.ModelForm):
    """
        User form
    """
    email = forms.EmailField(required=True)
    birth_date = forms.DateField(required=True)
    username = forms.CharField(label='Логин', required=True)

    def __init__(self, *args, **kwargs):
        super(UserForm, self).__init__(*args, **kwargs)

    class Meta:
        model = User
        fields = (
            'username', 'first_name', 'last_name', 'middle_name', 'email',
            'birth_date', 'is_active', 'phone', 'skype', 'site', 'city',
        )


class NewUserForm(UserForm):

    password = forms.CharField(required=True, label='Пароль', widget=forms.PasswordInput())
    confirm = forms.CharField(required=True, label='Подтверждение', widget=forms.PasswordInput())

    class Meta(UserForm.Meta):
        fields = (
            'username', 'password', 'confirm', 'first_name', 'last_name', 'middle_name',
            'email', 'birth_date', 'phone', 'skype', 'site', 'city',
        )

    def clean_password(self):
        password = self.cleaned_data.get('password')
        confirm = self.cleaned_data.get('confirm')

        if password != confirm:
            raise ValidationError('Пароли не совпадают')

        return password
