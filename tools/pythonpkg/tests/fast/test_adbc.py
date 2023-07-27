import duckdb
import pytest
import sys
import datetime
import os
from pathlib import Path

adbc_driver_manager = pytest.importorskip("adbc_driver_manager.dbapi")
adbc_driver_manager_lib = pytest.importorskip("adbc_driver_manager._lib")

pyarrow = pytest.importorskip("pyarrow")


@pytest.fixture
def duck_conn():
    if sys.platform.startswith("win"):
        pytest.xfail("not supported on Windows")
    # duckdb.duckdb.__file__
    with adbc_driver_manager.connect(
        driver='/Users/holanda/Documents/Projects/duckdb/build/debug/src/libduckdb.dylib', entrypoint="duckdb_adbc_init"
    ) as conn:
        yield conn


def example_table():
    return pyarrow.table(
        [
            [1, 2, 3, 4],
            ["a", "b", None, "d"],
        ],
        names=["ints", "strs"],
    )


@pytest.mark.xfail
def test_connection_get_info(duck_conn):
    assert duck_conn.adbc_get_info() != {}


def test_connection_get_table_schema(duck_conn):
    with duck_conn.cursor() as cursor:
        # Test Default Schema
        cursor.execute("CREATE TABLE tableschema (ints BIGINT)")
        assert duck_conn.adbc_get_table_schema("tableschema") == pyarrow.schema(
            [
                ("ints", "int64"),
            ]
        )

        # Test Given Schema
        cursor.execute("CREATE SCHEMA test;")
        cursor.execute("CREATE TABLE test.tableschema (test_ints BIGINT)")
        assert duck_conn.adbc_get_table_schema("tableschema", db_schema_filter="test") == pyarrow.schema(
            [
                ("test_ints", "int64"),
            ]
        )
        assert duck_conn.adbc_get_table_schema("tableschema") == pyarrow.schema(
            [
                ("ints", "int64"),
            ]
        )

        # Catalog name is currently not supported
        with pytest.raises(
            adbc_driver_manager_lib.NotSupportedError,
            match=r'Catalog Name is not used in DuckDB. It must be set to nullptr or an empty string',
        ):
            duck_conn.adbc_get_table_schema("tableschema", catalog_filter="bla", db_schema_filter="test")


def test_insertion(duck_conn):
    table = example_table()
    reader = table.to_reader()

    with duck_conn.cursor() as cursor:
        cursor.adbc_ingest("ingest", reader, "create")
        cursor.execute("SELECT * FROM ingest")
        assert cursor.fetch_arrow_table() == table

    with duck_conn.cursor() as cursor:
        cursor.adbc_ingest("ingest_table", table, "create")
        cursor.execute("SELECT * FROM ingest")
        assert cursor.fetch_arrow_table() == table

    # Test Append
    with duck_conn.cursor() as cursor:
        with pytest.raises(
            adbc_driver_manager_lib.InternalError,
            match=r'Failed to create table \'ingest_table\': Table with name "ingest_table" already exists!',
        ):
            cursor.adbc_ingest("ingest_table", table, "create")
        cursor.adbc_ingest("ingest_table", table, "append")
        cursor.execute("SELECT count(*) FROM ingest_table")
        assert cursor.fetch_arrow_table().to_pydict() == {"count_star()": [8]}


def test_read(duck_conn):
    with duck_conn.cursor() as cursor:
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "category.csv")
        cursor.execute(f"SELECT * FROM '{filename}'")
        assert cursor.fetch_arrow_table().to_pydict() == {
            "CATEGORY_ID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "NAME": [
                "Action",
                "Animation",
                "Children",
                "Classics",
                "Comedy",
                "Documentary",
                "Drama",
                "Family",
                "Foreign",
                "Games",
                "Horror",
                "Music",
                "New",
                "Sci-Fi",
                "Sports",
                "Travel",
            ],
            "LAST_UPDATE": [
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
                datetime.datetime(2006, 2, 15, 4, 46, 27),
            ],
        }
