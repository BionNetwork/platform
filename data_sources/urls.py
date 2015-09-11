from __future__ import absolute_import, unicode_literals

__author__ = 'damir'

from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^$', views.SourcesListView.as_view(), name='index'),
    # url(r'^users/delete/(?P<id>\d+)/$', views.RemoveUserView.as_view(), name='users.delete'),
    # url(r'^users/edit/(?P<id>\d+)/$', views.EditUserView.as_view(), name='users.edit'),
    # url(r'^users/add$', views.NewUserView.as_view(), name='users.add'),
]
