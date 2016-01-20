from __future__ import absolute_import, unicode_literals
from django.conf.urls import url
from . import views

__author__ = 'miholeus'

"""Core URL Configuration.
"""

urlpatterns = [
    url(r'^$', views.HomeView.as_view(), name='home'),
    url(r'^login/$', views.LoginView.as_view(), name='login'),
    url(r'^logout/$', views.LogoutView.as_view(), name='logout'),
    url(r'^registration/$', views.RegistrationView.as_view(), name='registration'),
    url(r'^activation/$', views.SetUserActive.as_view(), name='activate_user'),
    url(r'^users/$', views.UserListView.as_view(), name='users'),
    url(r'^users/delete/(?P<id>\d+)/$', views.RemoveUserView.as_view(), name='users.delete'),
    url(r'^users/edit/(?P<id>\d+)/$', views.EditUserView.as_view(), name='users.edit'),
    url(r'^users/add$', views.NewUserView.as_view(), name='users.add'),
    url(r'^users/profile/(?P<id>\d+)/$', views.UserProfileView.as_view(), name='users.profile'),
    url(r'^u/(?P<id>\d+)/$', views.TempImageView.as_view(), name='users.temp_image'),
]
