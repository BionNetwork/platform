# coding: utf-8

from django.utils.translation import ugettext_lazy as _
from django.utils import six
from django.utils.encoding import smart_text
from django import forms
from django.db.models.fields import TextField
import collections
"""
Custom model fields
"""


class XmlField(TextField):
    description = _("Xml data")
    empty_values = [None, b'']

    def get_internal_type(self):
        return "XmlField"

    def db_type(self, connection):
        return 'xml'

    def get_default(self):
        if self.has_default() and not isinstance(self.default, collections.Callable):
            return self.default
        default = super(XmlField, self).get_default()
        if default == '':
            return b''
        return default

    def get_prep_value(self, value):
        value = super(TextField, self).get_prep_value(value)
        if isinstance(value, six.string_types) or value is None:
            return value
        return smart_text(value)

    def formfield(self, **kwargs):
        # Passing max_length to forms.CharField means that the value's length
        # will be validated twice. This is considered acceptable since we want
        # the value in the form field (to pass into widget for example).
        defaults = {'max_length': self.max_length, 'widget': forms.Textarea}
        defaults.update(kwargs)
        return super(TextField, self).formfield(**defaults)
