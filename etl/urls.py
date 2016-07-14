from __future__ import absolute_import, unicode_literals

from django.conf.urls import url
from . import views

urlpatterns = [
    # url(r'^datasources/edit/(?P<id>\d+)/$', views.EditSourceView.as_view(), name='datasources.edit'),
    # url(r'^datasources/add/$', views.NewSourceView.as_view(), name='datasources.add'),
    # url(r'^datasources/check_conn/$', views.CheckConnectionView.as_view(), name='datasources.check_conn'),
    # url(r'^datasources/get_data/(?P<id>\d+)/$', views.GetConnectionDataView.as_view(), name='datasources.get_data'),
    # url(r'^datasources/retitle_column/$', views.RetitleColumnView.as_view(), name='datasources.retitle_column'),
    url(r'^datasources/get_rows/$', views.GetDataRowsView.as_view(), name='datasources.get_rows'),
    # url(r'^datasources/remove_all_tables/$', views.RemoveAllTablesView.as_view(),
    #     name='datasources.remove_all_tables'),
    # url(r'^datasources/loading_data/$', views.LoadDataView.as_view(),
    #     name='datasources.load_data'),
    # url(r'^datasources/user_tasks/$', views.GetUserTasksView.as_view(),
    #     name='datasources.user_tasks')
]
