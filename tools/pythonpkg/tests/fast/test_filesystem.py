from pathlib import Path
from shutil import copyfileobj
from typing import Callable

from duckdb import DuckDBPyConnection, InvalidInputException
from pytest import raises, importorskip, fixture

importorskip('fsspec')
from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

FILENAME = 'integers.csv'


@fixture()
def memory():
    fs = filesystem('memory')
    # copy csv into memory filesystem
    add_file(fs)
    return fs


def add_file(fs, filename=FILENAME):
    copyfileobj((Path(__file__).parent / 'data' / filename).open('rb'), fs.open(filename, 'wb'))


class TestPythonFilesystem:
    def test_unregister_non_existent_filesystem(self, duckdb_cursor: DuckDBPyConnection):
        with raises(InvalidInputException):
            duckdb_cursor.unregister_filesystem('fake')

    def test_memory_filesystem(self, duckdb_cursor: DuckDBPyConnection, memory: AbstractFileSystem):
        duckdb_cursor.register_filesystem(memory)

        assert memory.protocol == 'memory'

        duckdb_cursor.execute(f"select * from 'memory://{FILENAME}'")

        assert duckdb_cursor.fetchall() == [(1, 10, 0), (2, 50, 30)]

        duckdb_cursor.unregister_filesystem('memory')

    def test_reject_abstract_filesystem(self, duckdb_cursor: DuckDBPyConnection):
        with raises(InvalidInputException):
            duckdb_cursor.register_filesystem(AbstractFileSystem())

    def test_unregister_builtin(self, require: Callable[[str], DuckDBPyConnection]):
        duckdb_cursor = require('httpfs')
        assert 'S3FileSystem' in duckdb_cursor.list_filesystems()
        duckdb_cursor.unregister_filesystem('S3FileSystem')

    def test_multiple_protocol_filesystems(self, duckdb_cursor: DuckDBPyConnection):
        memory = MemoryFileSystem()
        add_file(memory)
        memory.protocol = ('file', 'local')
        duckdb_cursor.register_filesystem(memory)

        duckdb_cursor.execute(f"select * from 'file://{FILENAME}'")

        assert duckdb_cursor.fetchall() == [(1, 10, 0), (2, 50, 30)]

    def test_write(self, duckdb_cursor: DuckDBPyConnection, memory: AbstractFileSystem):
        duckdb_cursor.register_filesystem(memory)

        duckdb_cursor.execute("copy (select 1) to 'memory://01.csv' (FORMAT CSV)")

        assert memory.open('01.csv').read() == b'1\n'

    def test_read_parquet(self, duckdb_cursor: DuckDBPyConnection, memory: AbstractFileSystem):
        filename = 'binary_string.parquet'
        add_file(memory, filename)

        duckdb_cursor.register_filesystem(memory)

        duckdb_cursor.execute(f"select * from read_parquet('memory://{filename}')")

        assert duckdb_cursor.fetchall() == [(b'foo',), (b'bar',), (b'baz',)]

    def test_write_parquet(self, duckdb_cursor: DuckDBPyConnection, memory: AbstractFileSystem):
        duckdb_cursor.register_filesystem(memory)
        filename = 'output.parquet'

        duckdb_cursor.execute(f'''COPY (SELECT 1) TO 'memory://{filename}' (FORMAT PARQUET);''')

        assert memory.open(filename).read().startswith(b'PAR1')
