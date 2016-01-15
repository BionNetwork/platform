# coding: utf-8
from __future__ import unicode_literals

from django import forms
from core.models import Datasource, DatasourceSettings


class SourceForm(forms.ModelForm):
    """
    Форма добавления и редактирования источника
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

cdc_type_values = {
    'apply_triggers': u'На основе триггеров (требуются права на создание триггеров)',
    'apply_checksum': u'Полное сравнение на основе расчета контрольных сумм'
}


class SettingsForm(forms.Form):
    """
    Форма добавления и редактирования настроек типа дозагрузки
    """
    CDC_TYPE = (
        (DatasourceSettings.CHECKSUM, cdc_type_values[DatasourceSettings.CHECKSUM]),
        (DatasourceSettings.TRIGGERS, cdc_type_values[DatasourceSettings.TRIGGERS]),

   )
    cdc_type_field = forms.ChoiceField(
        label=u'Тип обновления данных', choices=CDC_TYPE)

