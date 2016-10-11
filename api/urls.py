# coding: utf-8


from django.conf.urls import url, include
from . import views
# from rest_framework import routers
from rest_framework_nested import routers


router = routers.DefaultRouter()
router.register(r'cubes', views.CubeViewSet, 'cubes')
router.register(r'datasources', views.DatasourceViewSet, 'datasource')

# cube router
cube_router = routers.NestedSimpleRouter(router, r'cubes', lookup='cube')
# nodes
cube_router.register(r'nodes', views.NodeViewSet, base_name='cube-nodes')
# columns
cube_router.register(r'columns', views.ColumnsViewSet, base_name='cube-columns')

# joins
node_router = routers.NestedSimpleRouter(cube_router, r'nodes', lookup='node')
node_router.register(r'child', views.JoinViewSet, base_name='node-child')


urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^', include(cube_router.urls)),
    url(r'^', include(node_router.urls)),
    url(r'^datasources/(?P<source_id>\d+)/(?P<table_name>\w+)/$',
        views.TablesView.as_view(), name='tables_data'),
    url(r'^datasources/(?P<source_id>\d+)/(?P<table_name>\w+)/preview/$',
        views.TablesDataView.as_view(), name='preview'),
    ]
