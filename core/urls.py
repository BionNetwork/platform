from __future__ import absolute_import, unicode_literals

__author__ = 'miholeus'

"""Core URL Configuration.
"""

from django.conf.urls import url

from . import views


urlpatterns = [
    url(r'^$', views.home, name='home'),
    # url(r'^login/$', views.LoginView.as_view(), name='login'),
]