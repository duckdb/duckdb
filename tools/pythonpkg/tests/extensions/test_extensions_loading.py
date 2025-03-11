import os
import platform

import duckdb
from pytest import raises
import pytest


pytestmark = pytest.mark.skipif(
    platform.system() == "Emscripten",
    reason="Extensions are not supported on Emscripten",
)


def test_extension_loading(require):
    if not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False):
        return
    extensions_list = ['json', 'excel', 'httpfs', 'tpch', 'tpcds', 'icu', 'fts']
    for extension in extensions_list:
        connection = require(extension)
        assert connection is not None


def test_install_non_existent_extension():
    conn = duckdb.connect()
    conn.execute("set custom_extension_repository = 'http://example.com'")

    with raises(duckdb.IOException) as exc:
        conn.install_extension('non-existent')

        if not isinstance(exc, duckdb.HTTPException):
            pytest.skip(reason='This test does not throw an HTTPException, only an IOException')
        value = exc.value

        assert value.status_code == 404
        assert value.reason == 'Not Found'
        assert 'Example Domain' in value.body
        assert 'Content-Length' in value.headers


def test_install_misuse_errors(duckdb_cursor):
    with pytest.raises(
        duckdb.InvalidInputException,
        match="Both 'repository' and 'repository_url' are set which is not allowed, please pick one or the other",
    ):
        duckdb_cursor.install_extension('name', repository='hello', repository_url='hello.com')

    with pytest.raises(
        duckdb.InvalidInputException, match="The provided 'repository' or 'repository_url' can not be empty!"
    ):
        duckdb_cursor.install_extension('name', repository_url='')

    with pytest.raises(
        duckdb.InvalidInputException, match="The provided 'repository' or 'repository_url' can not be empty!"
    ):
        duckdb_cursor.install_extension('name', repository='')

    with pytest.raises(duckdb.InvalidInputException, match="The provided 'version' can not be empty!"):
        duckdb_cursor.install_extension('name', version='')
