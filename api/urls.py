from __future__ import absolute_import, unicode_literals

from django.conf.urls import url
from . import views

__author__ = 'damir(GDR)'


urlpatterns = [
    url(r'^schema/import$', views.ImportSchemaView.as_view(), name='import_schema'),
    url(r'^query/execute$', views.ExecuteQueryView.as_view(), name='execute_query'),
    url(r'^schema$', views.SchemasListView.as_view(), name='schemas_list'),
    url(r'^schema/(?P<id>\d+)/$', views.GetSchemaView.as_view(), name='get_schema'),
]
