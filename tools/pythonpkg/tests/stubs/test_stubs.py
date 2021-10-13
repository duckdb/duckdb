import mypy.stubtest
import subprocess
import sys
import tempfile
import contextlib
import os
from typing import *

MYPY_INI = '''
[mypy]
[mypy-pandas]
ignore_missing_imports = True
'''

@contextlib.contextmanager
def temp_mypy_ini(contents: str) -> Generator[str, None, None]:
	with tempfile.NamedTemporaryFile('wt', suffix='mypy.ini', delete=False) as mypy_ini:
		mypy_ini.write(contents)
	mypy_ini_filename = mypy_ini.name
	try:
		yield mypy_ini_filename
	finally:
		os.remove(mypy_ini_filename)


def test_stubs():
	# just run stubtest

	with temp_mypy_ini(MYPY_INI) as ini_path:
		assert (
			subprocess.run([
				sys.executable, '-m', 'mypy.stubtest',
				'duckdb',
				'--mypy-config-file', ini_path,
			]).returncode
			== 0
		), 'Stubtest failed! Check stdout.'