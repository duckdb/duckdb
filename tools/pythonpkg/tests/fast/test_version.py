import duckdb
import sys


def test_version():
    assert duckdb.__version__ != "0.0.0"


def test_formatted_python_version():
    formatted_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert duckdb.__formatted_python_version__ == formatted_python_version
