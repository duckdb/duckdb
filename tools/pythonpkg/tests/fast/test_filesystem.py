import logging
import sys
from pathlib import Path
from shutil import copyfileobj
from typing import Callable

import duckdb
from duckdb import DuckDBPyConnection, InvalidInputException
from pytest import raises, importorskip, fixture, MonkeyPatch, mark

importorskip('fsspec', '2022.11.0')
from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

FILENAME = 'integers.csv'

logging.basicConfig(level=logging.DEBUG)


@fixture()
def memory():
    fs = filesystem('memory', skip_instance_cache=True)
    # copy csv into memory filesystem
    add_file(fs)
    return fs


def add_file(fs, filename=FILENAME):
    with (Path(__file__).parent / 'data' / filename).open('rb') as source, fs.open(filename, 'wb') as dest:
        copyfileobj(source, dest)


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
        memory = MemoryFileSystem(skip_instance_cache=True)
        add_file(memory)
        memory.protocol = ('file', 'local')
        duckdb_cursor.register_filesystem(memory)

        for protocol in memory.protocol:
            duckdb_cursor.execute(f"select * from '{protocol}://{FILENAME}'")

            assert duckdb_cursor.fetchall() == [(1, 10, 0), (2, 50, 30)]

    def test_write(self, duckdb_cursor: DuckDBPyConnection, memory: AbstractFileSystem):
        duckdb_cursor.register_filesystem(memory)

        duckdb_cursor.execute("copy (select 1) to 'memory://01.csv' (FORMAT CSV)")

        assert memory.open('01.csv').read() == b'1\n'

    def test_null_bytes(self, duckdb_cursor: DuckDBPyConnection, memory: AbstractFileSystem):
        with memory.open('test.csv', 'wb') as fh:
            fh.write(b'hello\n\0world\0')
        duckdb_cursor.register_filesystem(memory)

        duckdb_cursor.execute('select * from "memory://test.csv"')

        assert duckdb_cursor.fetchall() == [('hello',), ('\0world\0',)]

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

    def test_when_fsspec_not_installed(self, duckdb_cursor: DuckDBPyConnection, monkeypatch: MonkeyPatch):
        monkeypatch.setitem(sys.modules, 'fsspec', None)

        with raises(ModuleNotFoundError):
            duckdb_cursor.register_filesystem(None)

    @mark.skipif(sys.version_info < (3, 8), reason="ArrowFSWrapper requires python 3.8 or higher")
    def test_arrow_fs_wrapper(self, tmp_path: Path):
        fs = importorskip('pyarrow.fs')
        from fsspec.implementations.arrow import ArrowFSWrapper

        local = fs.LocalFileSystem()
        local_fsspec = ArrowFSWrapper(local, skip_instance_cache=True)
        local_fsspec.protocol = "local"
        filename = str(tmp_path / "test.csv")
        with local_fsspec.open(filename, mode='w') as f:
            f.write("a,b,c\n")
            f.write("1,2,3\n")
            f.write("4,5,6\n")

        duckdb_cursor = duckdb.connect()
        duckdb_cursor.register_filesystem(local_fsspec)
        duckdb_cursor.execute(f"select * from read_csv_auto('local://{filename}', header=true)")

        assert duckdb_cursor.fetchall() == [(1, 2, 3), (4, 5, 6)]
