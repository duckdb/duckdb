import duckdb
import os
import glob
import pandas as pd
from os.path import abspath

class TestExtensions(object):
    def test_extensions(self, duckdb_cursor):

        # Paths to search for extensions
        extension_search_patterns = [
            "../../../../build/release/extension/*/*.duckdb_extension",
            "../../*.duckdb_extension"
        ]

        # DUCKDB_PYTHON_TEST_EXTENSION_PATH can be used to add a path for the extension test to search for extensions
        if 'DUCKDB_PYTHON_TEST_EXTENSION_PATH' in os.environ:
            env_extension_path = os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_PATH');
            env_extension_path = env_extension_path.rstrip('/')
            extension_search_patterns.append(env_extension_path + '/*/*.duckdb_extension')
            extension_search_patterns.append(env_extension_path + '/*.duckdb_extension')

        # Depending on the env var, the test will fail on not finding any extensions
        must_test_extension_load = os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False)

        extension_paths_found = []
        for pattern in extension_search_patterns:
            extension_pattern_abs = abspath(pattern)
            print(f"Searching path: {extension_pattern_abs}")
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