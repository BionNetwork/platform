# -*- coding: utf-8 -*-


class ResponseError(Exception):
    """Ошибка для пользователя

    Attributes:
        message(str): Сообщение ошибки
        code(str): Код ошибки
    """
    def __init__(self, message, code='error'):
        self.message = message
        self.code = code
