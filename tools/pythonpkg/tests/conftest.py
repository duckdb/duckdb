import numpy
import os
import pytest
import shutil

import duckdb

@pytest.fixture(scope="function")
def duckdb_empty_cursor(request):
    test_dbfarm = tmp_path.resolve().as_posix()

    connection = duckdb.connect('')
    cursor = connection.cursor()
    return cursor

@pytest.fixture(scope="function")
def duckdb_cursor(request):

    connection = duckdb.connect('')
    cursor = connection.cursor()
   # cursor.create('integers', {'i': numpy.arange(10)})
    cursor.execute('CREATE TABLE integers (i integer)')
    cursor.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
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
