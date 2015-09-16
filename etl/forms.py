# coding: utf-8
from __future__ import unicode_literals

from django import forms
from core.models import Datasource


class SourceForm(forms.ModelForm):
    """
        User form
    """
    def __init__(self, *args, **kwargs):
        super(SourceForm, self).__init__(*args, **kwargs)
        self.init_fields()

    def init_fields(self):
        for field in self.fields:
            self.fields[field].widget.attrs['class'] = 'form-control'
            self.fields[field].widget.attrs['required'] = 'true'

    class Meta:
        model = Datasource
        fields = ('conn_type', 'db', 'login', 'password', 'host', 'port')
        password = forms.CharField(widget=forms.PasswordInput)
        widgets = {
            'password': forms.PasswordInput(render_value=True),
        }
