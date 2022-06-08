import mypy.stubtest
import subprocess
import sys
import tempfile
import contextlib
import os
from typing import *

MYPY_INI_PATH = os.path.join(os.path.dirname(__file__), 'mypy.ini')

def test_stubs():
	# The test has false positives on the following keywords
	skip_stubs_errors = ['pybind11_module_local', 'git_revision']
	# just run stubtest
	stubs = subprocess.run([
			sys.executable, '-m', 'mypy.stubtest',
			'duckdb',
			'--mypy-config-file', MYPY_INI_PATH,
		], stdout=subprocess.PIPE)
	if (stubs.returncode != 0):
		errors = stubs.stdout.decode('utf-8').split('error')
		broken_stubs = []
		for error in errors:
			add_error = True
			for skip in skip_stubs_errors:
				if error == '' or skip in error:
					add_error = False
					break
			if (add_error):
				broken_stubs.append(error)
			if len(broken_stubs) > 0:
				print("Stubs must be updated, either add them to skip_stubs_errors or update __init__.pyi accordingly")
				print(broken_stubs)
				assert(0)