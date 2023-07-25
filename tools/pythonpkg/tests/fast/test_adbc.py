import duckdb
import pytest
import sys


adbc_driver_manager = pytest.importorskip('adbc_driver_manager.dbapi')
adbc_driver_manager_lib = pytest.importorskip('adbc_driver_manager._lib')

pyarrow = pytest.importorskip('pyarrow')


def test_insertion():
    con = adbc_driver_manager.connect(driver=duckdb.duckdb.__file__, entrypoint="duckdb_adbc_init")

    table = pyarrow.table(
        [
            [1, 2, 3, 4],
            ["a", "b", None, "d"],
        ],
        names=["ints", "strs"],
    )
    reader = table.to_reader()

    with con.cursor() as cursor:
        cursor.adbc_ingest("ingest", reader, "create")
        cursor.execute("SELECT * FROM ingest")
        assert cursor.fetch_arrow_table() == table

    with con.cursor() as cursor:
        cursor.adbc_ingest("ingest_table", table, "create")
        cursor.execute("SELECT * FROM ingest")
        assert cursor.fetch_arrow_table() == table

    # Test Append
    with con.cursor() as cursor:
        with pytest.raises(
            adbc_driver_manager_lib.InternalError,
            match=r'Failed to create table \'ingest_table\': Table with name "ingest_table" already exists!',
        ):
            cursor.adbc_ingest("ingest_table", table, "create")
        cursor.adbc_ingest("ingest_table", table, "append")
        cursor.execute("SELECT count(*) FROM ingest_table")
        assert cursor.fetch_arrow_table().to_pydict() == {'count_star()': [8]}


# def test_read():
#     con = adbc_driver_manager.connect(driver='/Users/holanda/Documents/Projects/duckdb/build/debug/src/libduckdb.dylib', entrypoint="duckdb_adbc_init")

#     table = pyarrow.table(
#             [
#                 [1, 2, 3, 4],
#                 ["a", "b", None, "d"],
#             ],
#             names=["ints", "strs"],
#         )
#     reader = table.to_reader()

#     with con.cursor() as cursor:
#         cursor.adbc_ingest("ingest", reader,"create")
#         cursor.execute("SELECT * FROM ingest")
#         assert cursor.fetch_arrow_table() == table
