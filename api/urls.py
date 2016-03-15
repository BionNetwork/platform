from __future__ import absolute_import, unicode_literals

from django.conf.urls import url, include
from . import views
from rest_framework import routers


router = routers.DefaultRouter()
router.register(r'users', views.UserViewSet)
router.register(r'datasources', views.DatasourceViewSet)

urlpatterns = [
    # url(r'^schema/import$', views.ImportSchemaView.as_view(), name='import_schema'),
    url(r'^query/execute$', views.ExecuteQueryView.as_view(), name='execute_query'),
    url(r'^schema/(?P<pk>\d+)/$', views.GetSchemaView.as_view(), name='get_schema'),
    url(r'^schema/$', views.SchemasListView.as_view(), name='cube-list'),
    url(r'^', include(router.urls)),
]
