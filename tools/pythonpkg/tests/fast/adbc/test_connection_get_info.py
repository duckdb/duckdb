import sys

import duckdb
import pytest

pa = pytest.importorskip("pyarrow")
adbc_driver_manager = pytest.importorskip("adbc_driver_manager")

if sys.version_info < (3, 9):
    pytest.skip(
        "Python Version must be higher or equal to 3.9 to run this test",
        allow_module_level=True,
    )

try:
    adbc_driver_duckdb = pytest.importorskip("adbc_driver_duckdb.dbapi")
    con = adbc_driver_duckdb.connect()
except adbc_driver_manager.InternalError as e:
    pytest.skip(
        f"'duckdb_adbc_init' was not exported in this install, try running 'python3 setup.py install': {e}",
        allow_module_level=True,
    )


class TestADBCConnectionGetInfo(object):
    def test_connection_basic(self):
        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            cursor.execute("select 42")
            res = cursor.fetchall()
            assert res == [(42,)]

    def test_connection_get_info_all(self):
        con = adbc_driver_duckdb.connect()
        adbc_con = con.adbc_connection
        res = adbc_con.get_info()
        reader = pa.RecordBatchReader._import_from_c(res.address)
        table = reader.read_all()
        values = table["info_value"]

        expected_result = pa.array(
            [
                "duckdb",
                "v" + duckdb.__version__,  # don't hardcode this, as it will change every version
                "ADBC DuckDB Driver",
                "(unknown)",
                "(unknown)",
            ],
            type=pa.string(),
        )

        assert values.num_chunks == 1
        chunk = values.chunk(0)
        string_values = chunk.field(0)
        assert string_values == expected_result

    def test_empty_result(self):
        con = adbc_driver_duckdb.connect()
        adbc_con = con.adbc_connection
        res = adbc_con.get_info([1337])
        reader = pa.RecordBatchReader._import_from_c(res.address)
        table = reader.read_all()
        values = table["info_value"]

        # Because all the codes we asked for were unrecognized, the result set is empty
        assert values.num_chunks == 0

    def test_unrecognized_codes(self):
        con = adbc_driver_duckdb.connect()
        adbc_con = con.adbc_connection
        res = adbc_con.get_info([0, 1000, 4, 2000])
        reader = pa.RecordBatchReader._import_from_c(res.address)
        table = reader.read_all()
        values = table["info_value"]

        expected_result = pa.array(["duckdb", "(unknown)"], type=pa.string())

        assert values.num_chunks == 1
        chunk = values.chunk(0)
        string_values = chunk.field(0)
        assert string_values == expected_result
