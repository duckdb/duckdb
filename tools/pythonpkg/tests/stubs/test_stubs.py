import os

from mypy import stubtest

MYPY_INI_PATH = os.path.join(os.path.dirname(__file__), 'mypy.ini')


def test_generated_stubs():
    skip_stubs_errors = ['pybind11', 'git_revision', 'is inconsistent, metaclass differs']

    stubtest.test_stubs(stubtest.parse_options(['duckdb', '--mypy-config-file', MYPY_INI_PATH]))

    broken_stubs = [
        error.get_description()
        for error in stubtest.test_module('duckdb')
        if not any(skip in error.get_description() for skip in skip_stubs_errors)
    ]

    if broken_stubs:
        print("Stubs must be updated, either add them to skip_stubs_errors or update __init__.pyi accordingly")
        print(broken_stubs)

        assert not broken_stubs
