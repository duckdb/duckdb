import mypy.stubtest
import subprocess
import sys
import tempfile
import contextlib
import os
from typing import *

MYPY_INI_PATH = os.path.join(os.path.dirname(__file__), 'mypy.ini')

def test_stubs():
	# just run stubtest
	stubs = subprocess.run([
			sys.executable, '-m', 'mypy.stubtest',
			'duckdb',
			'--mypy-config-file', MYPY_INI_PATH,
		], stdout=subprocess.PIPE)
	if (stubs.returncode != 0):
		errors = stubs.stdout.decode('utf-8').split('error')
		for error in errors:
			if error != '' and 'pybind11_module_local' not in error:
				print(errors)
				assert(0)


