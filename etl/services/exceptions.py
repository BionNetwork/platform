# coding: utf-8


# TODO make codes for exceptions
class BaseException(Exception):
    """
    Базовый для исключений
    """
    MSG = ""

    def __init__(self, message=None):
        super(BaseException, self).__init__()
        self.message = message or self.MSG

    def __str__(self):
        return "{0}: {1}".format(self.__class__.__name__, self.message)


class SheetException(BaseException):
    """
    Ошибка отсутствия страницы в файлах
    """
    MSG = "No such sheet!"


class SourceUpdateException(BaseException):
    """
    Ошибка при работе с источником
    """
