from __future__ import absolute_import, unicode_literals

__author__ = 'damir'

from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^datasources/$', views.SourcesListView.as_view(), name='datasources.index'),
    url(r'^datasources/delete/(?P<id>\d+)/$', views.RemoveSourceView.as_view(), name='datasources.delete'),
    url(r'^datasources/edit/(?P<id>\d+)/$', views.EditSourceView.as_view(), name='datasources.edit'),
    url(r'^datasources/add/$', views.NewSourceView.as_view(), name='datasources.add'),
    url(r'^datasources/check_conn/$', views.CheckConnectionView.as_view(), name='datasources.check_conn'),
    url(r'^datasources/get_data/(?P<id>\d+)/$', views.GetConnectionDataView.as_view(), name='datasources.get_data'),
    url(r'^datasources/get_columns/$', views.GetColumnsView.as_view(), name='datasources.get_columns'),
    url(r'^datasources/get_rows/$', views.GetDataRowsView.as_view(), name='datasources.get_rows'),
]
