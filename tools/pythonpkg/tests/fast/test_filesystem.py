from pathlib import Path
from shutil import copyfileobj
from typing import Callable

from duckdb import DuckDBPyConnection, InvalidInputException
from pytest import raises, importorskip

importorskip('fsspec')

from fsspec.implementations.memory import MemoryFileSystem


class TestPythonFilesystem:
    def test_unregister_non_existent_filesystem(self, duckdb_cursor: DuckDBPyConnection):
        with raises(InvalidInputException):
            duckdb_cursor.unregister_filesystem('fake')

    def test_memory_filesystem(self, duckdb_cursor: DuckDBPyConnection):
        fs = MemoryFileSystem()
        filename = 'integers.csv'
        # copy csv into memory filesystem
        copyfileobj((Path(__file__).parent / 'data' / filename).open(), fs.open(filename, 'w'))
        duckdb_cursor.register_filesystem(fs)

        assert fs.protocol == 'memory'

        duckdb_cursor.execute("select * from 'memory://integers.csv'")

        assert duckdb_cursor.fetchall() == [(1, 10, 0), (2, 50, 30)]

        duckdb_cursor.unregister_filesystem('memory')

    def test_unregister_builtin(self, require: Callable[[str], DuckDBPyConnection]):
        duckdb_cursor = require('httpfs')
        assert 'S3FileSystem' in duckdb_cursor.list_filesystems()
        duckdb_cursor.unregister_filesystem('S3FileSystem')
