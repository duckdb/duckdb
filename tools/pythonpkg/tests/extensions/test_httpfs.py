import duckdb
import os
from pytest import raises, mark
import pytest
from conftest import NumpyPandas, ArrowPandas

# We only run this test if this env var is set
pytestmark = mark.skipif(
    not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False), reason='httpfs extension not available'
)


class TestHTTPFS(object):
    def test_read_json_httpfs(self, require):
        connection = require('httpfs')
        # FIXME: add test back
        # res = connection.read_json('https://jsonplaceholder.typicode.com/todos')
        # assert len(res.types) == 4

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_httpfs(self, require, pandas):
        connection = require('httpfs')
        try:
            connection.execute(
                "SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/duckdb/duckdb/main/data/parquet-testing/userdata1.parquet') LIMIT 3;"
            )
        except RuntimeError as e:
            # Test will ignore result if it fails due to networking issues while running the test.
            if str(e).startswith("HTTP HEAD error"):
                return
            elif str(e).startswith("Unable to connect"):
                return
            else:
                raise e

        result_df = connection.fetchdf()
        exp_result = pandas.DataFrame(
            {
                'id': pandas.Series([1, 2, 3], dtype="int32"),
                'first_name': ['Amanda', 'Albert', 'Evelyn'],
                'last_name': ['Jordan', 'Freeman', 'Morgan'],
            }
        )
        pandas.testing.assert_frame_equal(result_df, exp_result)

    def test_http_exception(self, require):
        connection = require('httpfs')

        with raises(duckdb.HTTPException) as exc:
            connection.execute("SELECT * FROM PARQUET_SCAN('https://example.com/userdata1.parquet')")

        value = exc.value
        assert value.status_code == 404
        assert value.reason == 'Not Found'
        assert value.body == ''
        assert 'Content-Length' in value.headers
