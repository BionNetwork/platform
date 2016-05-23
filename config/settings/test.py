from __future__ import absolute_import, unicode_literals

from .default import *

# Add test settings here.

# redis conf
REDIS_HOST = 'localhost'
REDIS_PORT = '6379'
REDIS_DB = '9'
REDIS_EXPIRE = 60 * 5
USE_REDIS_CACHE = True