import os
import sys
import platform

from _duckdb_extension import *
import _duckdb_extension
__all__ = [name for name in dir(_duckdb_extension) if name[0] != '_']

# Pass metadata & docstring through loader
from _duckdb_extension import __version__, __package__, __git_revision__
__doc__ = _duckdb_extension.__doc__