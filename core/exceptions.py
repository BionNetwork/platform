# -*- coding: utf-8 -*-

"""
Codes start with 1025, everything below 1025 is reserved and can't be used for codes
1xxx codes are system codes
2xxx codes are validation codes
3xxx codes are apis codes
"""


class ExceptionCode(object):
    ERR_SYSTEM = 'System error'
    ERR_CONNECT_TO_DATASOURCE = 'Can"t connect to datasource'
    ERR_TASK_ALREADY_IN_QUEUE = 'Task is already in queue list'
    ERR_CDC_TYPE_IS_NOT_SET = 'CDC type is not set'
    ERR_VALIDATION_ERROR = 'Validation error'


class ExceptionWithCodes(Exception):
    code_map = {
        ExceptionCode.ERR_CONNECT_TO_DATASOURCE: 1025,
        ExceptionCode.ERR_TASK_ALREADY_IN_QUEUE: 1026,
        ExceptionCode.ERR_CDC_TYPE_IS_NOT_SET: 1027,
        ExceptionCode.ERR_SYSTEM: 1028,
        ExceptionCode.ERR_VALIDATION_ERROR: 2000
    }

    def __init__(self, message, code='default'):
        """
        Get code from code map
        Args:
            message: str
            code:
        """
        if code not in self.code_map:
            code = self.code_map[ExceptionCode.ERR_SYSTEM]
        else:
            code = self.code_map[code]
        self.code = code
        self.message = message


class ResponseError(ExceptionWithCodes):
    """Response exceptions"""
    pass


class ValidationError(ExceptionWithCodes):
    """Validation exceptions"""
    def __init__(self, message, code=ExceptionCode.ERR_VALIDATION_ERROR):
        super(ValidationError, self).__init__(message, code)
