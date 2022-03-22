import duckdb
import os
import glob
import pandas as pd

class TestExtensions(object):
    def test_extensions(self, duckdb_cursor):
        extension_base_path = "../../../../build/release/extension"

        dirname = os.path.dirname(__file__)
        extension_base_path_abs = os.path.join(dirname, extension_base_path)

        def test_httpfs(duckdb_cursor):
            try:
                duckdb_cursor.execute("SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/cwida/duckdb/master/data/parquet-testing/userdata1.parquet') LIMIT 3;")
            except RuntimeError as e:
                # Test will ignore result if it fails due to networking issues while running the test.
                if (str(e).startswith("HTTP HEAD error")):
                    return
                elif (str(e).startswith("Unable to connect")):
                    return
                else:
                    raise e

            result_df = duckdb_cursor.fetchdf()
            exp_result = pd.DataFrame({
                'id': pd.Series([1, 2, 3], dtype="int32"),
                'first_name': ['Amanda', 'Albert', 'Evelyn'],
                'last_name': ['Jordan', 'Freeman', 'Morgan']
            })
            assert(result_df.equals(exp_result))

        for name in glob.glob(f"{extension_base_path_abs}/*/*.duckdb_extension"):
            conn = duckdb.connect()
            conn.execute(f"LOAD '{name}'")

            if (name.endswith("httpfs.duckdb_extension")):
                test_httpfs(conn)