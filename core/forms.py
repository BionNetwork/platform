# coding: utf-8
from __future__ import unicode_literals
from django import forms
from django.core.exceptions import ValidationError

from .models import User

__author__ = 'damir'


class UserForm(forms.ModelForm):
    """
        User form
    """
    email = forms.EmailField(required=True)
    birth_date = forms.DateField(required=True)
    username = forms.CharField(label='Логин', required=True)

    def __init__(self, *args, **kwargs):
        super(UserForm, self).__init__(*args, **kwargs)
        self.init_fields()

    def init_fields(self):
        for field in self.fields:
            if field != 'is_active':
                self.fields[field].widget.attrs['class'] = 'form-control'

        self.fields['email'].widget.attrs['placeholder'] = 'Email'
        self.fields['birth_date'].widget.attrs.update({'class': 'datepicker form-control', 'placeholder': 'дд.мм.ГГГГ'})

    class Meta:
        model = User
        fields = (
            'username', 'first_name', 'last_name', 'middle_name', 'email',
            'birth_date', 'is_active', 'phone', 'skype', 'site', 'city',
        )


class NewUserForm(UserForm):

    password = forms.CharField(required=True, label='Пароль', widget=forms.PasswordInput())
    confirm = forms.CharField(required=True, label='Подтверждение', widget=forms.PasswordInput())

    def __init__(self, *args, **kwargs):
        super(UserForm, self).__init__(*args, **kwargs)
        self.init_fields()

    class Meta(UserForm.Meta):
        fields = (
            'username', 'password', 'confirm', 'first_name', 'last_name', 'middle_name',
            'email', 'birth_date', 'phone', 'skype', 'site', 'city',
        )

    def clean(self):
        password = self.cleaned_data.get('password')
        confirm = self.cleaned_data.get('confirm')

        if password != confirm:
            raise ValidationError('Пароли не совпадают')
