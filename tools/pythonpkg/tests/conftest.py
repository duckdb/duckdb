import numpy
import os
import pytest
import shutil
from os.path import abspath
import glob
import duckdb

@pytest.fixture(scope="function")
def duckdb_empty_cursor(request):
    connection = duckdb.connect('')
    cursor = connection.cursor()
    return cursor
 

@pytest.fixture(scope="function")
def require():
    def _require(extension_name, db_name=''):
        # Paths to search for extensions
        extension_search_patterns = [
            "../../../../build/release/extension/*/*.duckdb_extension",
            "../../../../build/debug/extension/*/*.duckdb_extension",
            "../../*.duckdb_extension",
             "../../../../../build/release/extension/*/*.duckdb_extension",
            "../../../../../build/debug/extension/*/*.duckdb_extension",
            "../../../*.duckdb_extension"
        ]

        # DUCKDB_PYTHON_TEST_EXTENSION_PATH can be used to add a path for the extension test to search for extensions
        if 'DUCKDB_PYTHON_TEST_EXTENSION_PATH' in os.environ:
            env_extension_path = os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_PATH');
            env_extension_path = env_extension_path.rstrip('/')
            extension_search_patterns.append(env_extension_path + '/*/*.duckdb_extension')
            extension_search_patterns.append(env_extension_path + '/*.duckdb_extension')

        extension_paths_found = []
        for pattern in extension_search_patterns:
            extension_pattern_abs = abspath(pattern)
            print(f"Searching path: {extension_pattern_abs}")
            for path in glob.glob(extension_pattern_abs):
                extension_paths_found.append(path)

        for path in extension_paths_found:
            print(path)
            if (path.endswith(extension_name+".duckdb_extension")):
                conn = duckdb.connect(db_name)
                conn.execute(f"LOAD '{path}'")
                return conn
        return None

    return _require

@pytest.fixture(scope='session', autouse=True)
def duckdb_cursor(request):

    connection = duckdb.connect('')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE integers (i integer)')
    cursor.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
    cursor.execute('CREATE TABLE timestamps (t timestamp)')
    cursor.execute("INSERT INTO timestamps VALUES ('1992-10-03 18:34:45'), ('2010-01-01 00:00:01'), (NULL)")
    cursor.execute("CALL dbgen(sf=0.01)")
    return cursor


@pytest.fixture(scope="function")
def duckdb_cursor_autocommit(request, tmp_path):
    test_dbfarm = tmp_path.resolve().as_posix()

    def finalizer():
        duckdb.shutdown()
        if tmp_path.is_dir():
            shutil.rmtree(test_dbfarm)

    request.addfinalizer(finalizer)

    connection = duckdb.connect(test_dbfarm)
    connection.set_autocommit(True)
    cursor = connection.cursor()
    return (cursor, connection, test_dbfarm)


@pytest.fixture(scope="function")
def initialize_duckdb(request, tmp_path):
    test_dbfarm = tmp_path.resolve().as_posix()

    def finalizer():
        duckdb.shutdown()
        if tmp_path.is_dir():
            shutil.rmtree(test_dbfarm)

    request.addfinalizer(finalizer)

    duckdb.connect(test_dbfarm)
    return test_dbfarm
