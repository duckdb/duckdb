import duckdb
import os
import pandas as pd


def test_httpfs(require):
    # We only run this test if this env var is set
    if not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False):
        return
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