# coding: utf-8

from django.conf import settings


def settings_processor(request):
    """ передача в темплэйты данных из settings.py
    """
    return {
        'socket_host': settings.SOCKET_HOST,
        'socket_port': settings.SOCKET_PORT,
    }
