# import mypy.stubtest
# import subprocess
# import sys
# import tempfile
# import contextlib
# import os
# from typing import *

# MYPY_INI_PATH = os.path.join(os.path.dirname(__file__), 'mypy.ini')

# def test_stubs():
# 	# just run stubtest

# 	assert (
# 		subprocess.run([
# 			sys.executable, '-m', 'mypy.stubtest',
# 			'duckdb',
# 			'--mypy-config-file', MYPY_INI_PATH,
# 		]).returncode
# 		== 0
# 	), 'Stubtest failed! Check stdout.'