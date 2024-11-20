import duckdb
import pytest
import os
import tempfile
import pandas as pd

VARCHAR = duckdb.typing.VARCHAR
BIGINT = duckdb.typing.BIGINT

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'binary_string.parquet')


@pytest.fixture(scope="session")
def tmp_parquets(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp('parquets', numbered=True)
    tmp_parquets = [str(tmp_dir / ('tmp' + str(i) + '.parquet')) for i in range(1, 4)]
    return tmp_parquets


class TestParquet(object):
    def test_scan_binary(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('" + filename + "') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('" + filename + "')").fetchall()
        assert res[0] == (b'foo',)

    def test_from_parquet_binary(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename)
        assert rel.types == ['BLOB']

        res = rel.execute().fetchall()
        assert res[0] == (b'foo',)

    def test_scan_binary_as_string(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute(
            "SELECT typeof(#1) FROM parquet_scan('" + filename + "',binary_as_string=True) limit 1"
        ).fetchall()
        assert res[0] == ('VARCHAR',)

        res = conn.execute("SELECT * FROM parquet_scan('" + filename + "',binary_as_string=True)").fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_binary_as_string(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename, True)
        assert rel.types == [VARCHAR]

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_file_row_number(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename, binary_as_string=True, file_row_number=True)
        assert rel.types == [VARCHAR, BIGINT]

        res = rel.execute().fetchall()
        assert res[0] == (
            'foo',
            0,
        )

    def test_from_parquet_filename(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename, binary_as_string=True, filename=True)
        assert rel.types == [VARCHAR, VARCHAR]

        res = rel.execute().fetchall()
        assert res[0] == (
            'foo',
            filename,
        )

    def test_from_parquet_list_binary_as_string(self, duckdb_cursor):
        rel = duckdb.from_parquet([filename], binary_as_string=True)
        assert rel.types == [VARCHAR]

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_list_file_row_number(self, duckdb_cursor):
        rel = duckdb.from_parquet([filename], binary_as_string=True, file_row_number=True)
        assert rel.types == [VARCHAR, BIGINT]

        res = rel.execute().fetchall()
        assert res[0] == (
            'foo',
            0,
        )

    def test_from_parquet_list_filename(self, duckdb_cursor):
        rel = duckdb.from_parquet([filename], binary_as_string=True, filename=True)
        assert rel.types == [VARCHAR, VARCHAR]

        res = rel.execute().fetchall()
        assert res[0] == (
            'foo',
            filename,
        )

    def test_parquet_binary_as_string_pragma(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('" + filename + "') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('" + filename + "')").fetchall()
        assert res[0] == (b'foo',)

        conn.execute("PRAGMA binary_as_string=1")

        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('" + filename + "') limit 1").fetchall()
        assert res[0] == ('VARCHAR',)

        res = conn.execute("SELECT * FROM parquet_scan('" + filename + "')").fetchall()
        assert res[0] == ('foo',)

        res = conn.execute(
            "SELECT typeof(#1) FROM parquet_scan('" + filename + "',binary_as_string=False) limit 1"
        ).fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('" + filename + "',binary_as_string=False)").fetchall()
        assert res[0] == (b'foo',)

        conn.execute("PRAGMA binary_as_string=0")

        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('" + filename + "') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('" + filename + "')").fetchall()
        assert res[0] == (b'foo',)

    def test_from_parquet_binary_as_string_default_conn(self, duckdb_cursor):
        duckdb.execute("PRAGMA binary_as_string=1")

        rel = duckdb.from_parquet(filename, True)
        assert rel.types == [VARCHAR]

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_union_by_name(self, tmp_parquets):
        conn = duckdb.connect()

        conn.execute(
            "copy (from (values (1::bigint), (2::bigint), (9223372036854775807::bigint)) t(a)) to '"
            + tmp_parquets[0]
            + "' (format 'parquet');"
        )

        conn.execute(
            "copy (from (values (3::integer, 4::integer), (5::integer, 6::integer)) t(a, b)) to '"
            + tmp_parquets[1]
            + "' (format 'parquet');"
        )

        conn.execute(
            "copy (from (values (100::integer, 101::integer), (102::integer, 103::integer)) t(a, c)) to '"
            + tmp_parquets[2]
            + "' (format 'parquet');"
        )

        rel = duckdb.from_parquet(tmp_parquets, union_by_name=True).order('a')
        assert rel.execute().fetchall() == [
            (
                1,
                None,
                None,
            ),
            (
                2,
                None,
                None,
            ),
            (
                3,
                4,
                None,
            ),
            (
                5,
                6,
                None,
            ),
            (
                100,
                None,
                101,
            ),
            (
                102,
                None,
                103,
            ),
            (
                9223372036854775807,
                None,
                None,
            ),
        ]
