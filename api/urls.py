

from django.conf.urls import url, include
from . import views
# from rest_framework import routers
from rest_framework_nested import routers


router = routers.SimpleRouter()
router.register(r'users', views.UserViewSet)
router.register(r'datasources', views.DatasourceViewSet, 'Datasource')
# router.register(r'datasource/(?P<source_id>\d+)/tables', views.TablesViewSet, 'tables')

router.register(r'cards', views.CardViewSet, 'cards')
# nodes
card_router = routers.NestedSimpleRouter(router, r'cards', lookup='card')
card_router.register(r'nodes', views.NodeViewSet, base_name='card-nodes')
# joins
node_router = routers.NestedSimpleRouter(card_router, r'nodes', lookup='node')
node_router.register(r'joins', views.JoinViewSet, base_name='node-joins')


urlpatterns = [
    # url(r'^schema/import$', views.ImportSchemaView.as_view(), name='import_schema'),
    # url(r'^query/execute$', views.ExecuteQueryView.as_view(), name='execute_query'),
    # url(r'^schema/(?P<pk>\d+)$', views.GetSchemaView.as_view(), name='get_schema'),
    # url(r'^schema$', views.SchemasListView.as_view(), name='schemas_list'),
    # url(r'^schema/(?P<id>\d+)/measures$', views.GetMeasureDataView.as_view(),
    #     name='measure_data'),
    # url(r'^schema/(?P<id>\d+)/dimensions$', views.GetDimensionDataView.as_view(),
    #     name='dimension_data'),
    url(r'^', include(router.urls)),
    url(r'^', include(card_router.urls)),
    url(r'^', include(node_router.urls)),
    url(r'^datasources/(?P<source_id>\d+)/(?P<table_name>\w+)/$',
        views.TablesView.as_view(), name='tables_data'),
    url(r'^datasources/(?P<source_id>\d+)/(?P<table_name>\w+)/preview/$',
        views.TablesDataView.as_view(), name='tables_data'),
    ]
