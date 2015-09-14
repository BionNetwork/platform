from __future__ import absolute_import, unicode_literals

__author__ = 'damir'

from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^$', views.SourcesListView.as_view(), name='index'),
    url(r'^delete/(?P<id>\d+)/$', views.RemoveSourceView.as_view(), name='etl.delete'),
    url(r'^edit/(?P<id>\d+)/$', views.EditSourceView.as_view(), name='etl.edit'),
    url(r'^add$', views.NewSourceView.as_view(), name='etl.add'),
    url(r'^check_conn$', views.CheckConnectionView.as_view(), name='check_conn'),
]
