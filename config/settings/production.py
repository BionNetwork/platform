

import sys
from .default import *

# Add production settings here.

try:
    from .local import *
except ImportError:
    pass

if sys.argv[:2] == ['manage.py', 'test']:
    try:
        from .test import *
    except ImportError:
        pass
