# coding: utf-8
from __future__ import unicode_literals

__author__ = 'damir'

"""
Хелпер настроек
"""


class Settings:
    @classmethod
    def get_host(cls, request):
        host = request.get_host()
        if request.is_secure():
            protocol = 'https'
        else:
            protocol = 'http'

        return "%s://%s" % (protocol, host)
