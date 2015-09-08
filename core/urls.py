from __future__ import absolute_import, unicode_literals

__author__ = 'miholeus'

"""Core URL Configuration.
"""

from django.conf.urls import url

from . import views


urlpatterns = [
    url(r'^$', views.HomeView.as_view(), name='home'),
    url(r'^login$', views.LoginView.as_view(), name='login'),
    url(r'^logout$', views.LogoutView.as_view(), name='logout'),
    url(r'^registration$', views.RegistrationView.as_view(), name='registration'),
    url(r'^activation$', views.SetUserActive.as_view(), name='activate_user'),
    url(r'^user_list$', views.UserListView.as_view(), name='user_list'),
    url(r'^remove_user/(?P<id>\d+)/$', views.RemoveUserView.as_view(), name='remove_user'),
    url(r'^edit_user/(?P<id>\d+)/$', views.EditUserView.as_view(), name='edit_user'),
    url(r'^new_user/$', views.NewUserView.as_view(), name='new_user'),
]
