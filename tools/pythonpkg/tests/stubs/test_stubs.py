import os

from mypy.stubtest import test_module, parse_options, test_stubs

MYPY_INI_PATH = os.path.join(os.path.dirname(__file__), 'mypy.ini')


def test_generated_stubs():
	# The test has false positives on the following keywords
	skip_stubs_errors = ['pybind11_module_local', 'git_revision']

	test_stubs(parse_options(['duckdb', '--mypy-config-file', MYPY_INI_PATH]))

	broken_stubs = [
		error
		for error in test_module('duckdb')
		if not any(skip in error.get_description() for skip in skip_stubs_errors)
	]

	if broken_stubs:
		print("Stubs must be updated, either add them to skip_stubs_errors or update __init__.pyi accordingly")
		print(broken_stubs)

		assert not broken_stubs
