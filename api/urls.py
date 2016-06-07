from __future__ import absolute_import, unicode_literals

from django.conf.urls import url, include
from . import views
from rest_framework import routers


router = routers.DefaultRouter()
router.register(r'users', views.UserViewSet)
router.register(r'datasources', views.DatasourceViewSet, 'Datasource')
# router.register(r'datasource/(?P<source_id>\d+)/tables', views.TablesViewSet, 'tables')
router.register(r'card_datasource', views.CardDataSourceViewSet, 'CardDatasource')
router.register(r'tasks', views.TaskViewSet, 'Task')
router.register(r'nodes', views.NodeViewSet, 'Node')

urlpatterns = [
    url(r'^schema/import$', views.ImportSchemaView.as_view(), name='import_schema'),
    url(r'^query/execute$', views.ExecuteQueryView.as_view(), name='execute_query'),
    url(r'^schema/(?P<pk>\d+)$', views.GetSchemaView.as_view(), name='get_schema'),
    url(r'^schema$', views.SchemasListView.as_view(), name='schemas_list'),
    url(r'^schema/(?P<id>\d+)/measures$', views.GetMeasureDataView.as_view(),
        name='measure_data'),
    url(r'^schema/(?P<id>\d+)/dimensions$', views.GetDimensionDataView.as_view(),
        name='dimension_data'),
    url(r'^', include(router.urls)),
    url(r'^tables_data/(?P<source_id>\d+)/(?P<table_name>\w+)/$', views.TablesDataView.as_view(), name='tables_data'),
    # url(r'^datasource/(?P<source_id>\d+)/tables/$', views.TablesViewSet, name='tables'),
    # url(r'^datasource/(?P<source_id>[0-9]+)/tables2/$', views.TablesViewSet.as_view({'get': 'get'}), name='tables2'),
    ]
