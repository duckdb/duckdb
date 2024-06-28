# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os


def test_http_logging_stderr(shell):
    test = (
        ShellTest(shell)
        .statement("SET enable_http_logging=true;")
        .statement("install 'http://extensions.duckdb.org/v0.10.1/osx_arm64/httpfs.duckdb_extension.gzzz';")
    )
    result = test.run()
    result.check_stderr("HTTP Request")
    result.check_stderr("HTTP Response")


def test_http_logging_file(shell, tmp_path):
    temp_dir = tmp_path / 'http_logging_dir'
    temp_dir.mkdir()
    temp_file = temp_dir / 'myfile'

    test = (
        ShellTest(shell)
        .statement("SET enable_http_logging=true;")
        .statement(f"SET http_logging_output='{temp_file.as_posix()}'")
        .statement("install 'http://extensions.duckdb.org/v0.10.1/osx_arm64/httpfs.duckdb_extension.gzzz';")
    )
    result = test.run()

    with open(temp_file, 'r') as f:
        file_content = f.read()
        assert "HTTP Request" in file_content
        assert "HTTP Response" in file_content
