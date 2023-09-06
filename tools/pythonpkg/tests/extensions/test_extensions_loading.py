import os

import duckdb
from pytest import raises


def test_extension_loading(require):
    if not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False):
        return
    extensions_list = ['json', 'excel', 'httpfs', 'tpch', 'tpcds', 'icu', 'visualizer', 'fts']
    for extension in extensions_list:
        connection = require(extension)
        assert connection is not None


def test_install_non_existent_extension():
    conn = duckdb.connect()
    conn.execute("set custom_extension_repository = 'http://example.com'")

    with raises(duckdb.HTTPException) as exc:
        conn.install_extension('non-existent')

    value = exc.value

    assert value.status_code == 404
    assert value.reason == 'Not Found'
    assert 'Example Domain' in value.body
    assert 'Content-Length' in value.headers
