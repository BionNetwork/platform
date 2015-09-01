from __future__ import absolute_import, unicode_literals

from .default import *

# Add production settings here.

try:
    from .local import *
except ImportError:
    pass
