import duckdb
import pytest
import sys
import datetime

adbc_driver_manager = pytest.importorskip('adbc_driver_manager.dbapi')
adbc_driver_manager_lib = pytest.importorskip('adbc_driver_manager._lib')

pyarrow = pytest.importorskip('pyarrow')


def test_insertion():
    if sys.platform.startswith("win"):
        pytest.xfail("Not supported on Windows")
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


def test_read():
    if sys.platform.startswith("win"):
        pytest.xfail("Not supported on Windows")
    con = adbc_driver_manager.connect(driver=duckdb.duckdb.__file__, entrypoint="duckdb_adbc_init")
    with con.cursor() as cursor:
        cursor.execute("SELECT * FROM 'data/category.csv'")
        assert cursor.fetch_arrow_table().to_pydict() == {
            'CATEGORY_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            'NAME': [
                'Action',
                'Animation',
                'Children',
                'Classics',
                'Comedy',
                'Documentary',
                'Drama',
                'Family',
                'Foreign',
                'Games',
                'Horror',
                'Music',
                'New',
                'Sci-Fi',
                'Sports',
                'Travel',
            ],
            'LAST_UPDATE': [
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
