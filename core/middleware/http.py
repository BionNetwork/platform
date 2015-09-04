# -*- coding: utf-8 -*-

__author__ = 'miholeus'

from django.http import HttpResponseNotAllowed
from django.template import RequestContext
from django.template import loader


class HttpResponseNotAllowedMiddleware(object):
    """
    Обработка 405 Not Allowed
    """
    def process_response(self, request, response):
        if isinstance(response, HttpResponseNotAllowed):
            context = RequestContext(request)
            response.content = loader.render_to_string("405.html", context_instance=context)
        return response
