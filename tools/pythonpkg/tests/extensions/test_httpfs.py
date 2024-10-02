import duckdb
import os
from pytest import raises, mark
import pytest
from conftest import NumpyPandas, ArrowPandas
import datetime

# We only run this test if this env var is set
# FIXME: we can add a custom command line argument to pytest to provide an extension directory
# We can use that instead of checking this environment variable inside of conftest.py's 'require' method
pytestmark = mark.skipif(
    not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False),
    reason='DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED is not set',
)


class TestHTTPFS(object):
    def test_read_json_httpfs(self, require):
        connection = require('httpfs')
        try:
            res = connection.read_json('https://jsonplaceholder.typicode.com/todos')
            assert len(res.types) == 4
        except duckdb.Error as e:
            if '403' in e:
                pytest.skip(reason="Test is flaky, sometimes returns 403")
            else:
                pytest.fail(str(e))

    def test_s3fs(self, require):
        connection = require('httpfs')

        rel = connection.read_csv(f"s3://duckdb-blobs/data/Star_Trek-Season_1.csv", header=True)
        res = rel.fetchone()
        assert res == (1, 0, datetime.date(1965, 2, 28), 0, 0, 0, 1, 0, 1, 1, 1, 0, 0, 6, 0, 0, 0, 0)

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

        # Read from a bogus HTTPS url, assert that it errors with a non-succesfull status code
        with raises(duckdb.HTTPException) as exc:
            connection.execute("SELECT * FROM PARQUET_SCAN('https://example.com/userdata1.parquet')")

        value = exc.value
        assert value.status_code != 200
        assert value.body == ''
        assert 'Content-Length' in value.headers

    def test_fsspec_priority(self, require):
        pytest.importorskip("fsspec")
        pytest.importorskip("gscfs")
        import fsspec

        connection = require('httpfs')

        gcs = fsspec.filesystem("gcs")
        connection.register_filesystem(gcs)
        assert connection.sql("select count(*) from 'gcs://ibis-examples/data/band_members.csv.gz'").fetchone() == (3,)
