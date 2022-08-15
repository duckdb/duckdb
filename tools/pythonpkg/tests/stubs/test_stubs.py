import os
import mypy.stubtest
MYPY_INI_PATH = os.path.join(os.path.dirname(__file__), 'mypy.ini')


def test_stubs():
	# The test has false positives on the following keywords
	skip_stubs_errors = ['pybind11_module_local', 'git_revision', '(checked 1 module)']
	# just run stubtest

	errors = mypy.stubtest.test_module('duckdb')

	broken_stubs = [
		error
		for error in errors
		if not any(skip in error for skip in skip_stubs_errors)
	]

	if broken_stubs:
		print("Stubs must be updated, either add them to skip_stubs_errors or update __init__.pyi accordingly")
		print(broken_stubs)
		assert broken_stubs == []
