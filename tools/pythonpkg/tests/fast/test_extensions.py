import duckdb
import os
import glob
import pandas as pd

class TestExtensions(object):
    def test_extensions(self, duckdb_cursor):

        # Paths to search for extensions, relative to this file
        extension_search_patterns = [
            "../../../../build/release/extension/*/*.duckdb_extension",
            "/tmp/duckdb_python_test_extensions/*/*.duckdb_extension",
            "./../../*.duckdb_extension"
        ]

        # Depending on the env var, the test will fail on not finding any extensions
        must_test_extension_load = os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False)

        dirname = os.path.dirname(__file__)
        extension_paths_found = []

        for pattern in extension_search_patterns:
            extension_pattern_abs = os.path.join(dirname, pattern)
            for path in glob.glob(extension_pattern_abs):
                extension_paths_found.append(path)

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

        if (must_test_extension_load and not extension_paths_found):
            raise Exception("Env var DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED was set, but no extensions were found to test with!");

        for path in extension_paths_found:
            conn = duckdb.connect()
            conn.execute(f"LOAD '{path}'")

            if (path.endswith("httpfs.duckdb_extension")):
                test_httpfs(conn)