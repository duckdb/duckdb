import os
import sys
import platform

# Set global flag for loading duckdb extension
if platform.system() != 'Windows':
    old_flags = sys.getdlopenflags()
    sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_NOW)

from _duckdb_extension import *
import _duckdb_extension
__all__ = [name for name in dir(_duckdb_extension) if name[0] != '_']

# Pass metadata & docstring through loader
from _duckdb_extension import __version__, __package__, __git_revision__
__doc__ = _duckdb_extension.__doc__

# restore flags to prevent further issues
if platform.system() != 'Windows':
    sys.setdlopenflags(old_flags)
    del old_flags
