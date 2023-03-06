import duckdb
import os
import pandas as pd
from pytest import raises, mark


# We only run this test if this env var is set
pytestmark = mark.skipif(
    not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False),
    reason='httpfs extension not available'
)


def test_httpfs(require):
    connection = require('httpfs')     
    try:
        connection.execute("SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/cwida/duckdb/master/data/parquet-testing/userdata1.parquet') LIMIT 3;")
    except RuntimeError as e:
        # Test will ignore result if it fails due to networking issues while running the test.
        if (str(e).startswith("HTTP HEAD error")):
            return
        elif (str(e).startswith("Unable to connect")):
            return
        else:
            raise e

    result_df = connection.fetchdf()
    exp_result = pd.DataFrame({
        'id': pd.Series([1, 2, 3], dtype="int32"),
        'first_name': ['Amanda', 'Albert', 'Evelyn'],
        'last_name': ['Jordan', 'Freeman', 'Morgan']
    })
    assert(result_df.equals(exp_result))



def test_http_exception(require):
    connection = require('httpfs')

    with raises(duckdb.HTTPException) as exc:
        connection.execute("SELECT * FROM PARQUET_SCAN('https://example.com/userdata1.parquet')")

    value = exc.value
    assert value.status_code == 404
    assert value.reason == 'Not Found'
    assert value.body == 'NOT YET SUPPORTED'
    assert 'Content-Length' in value.headers
