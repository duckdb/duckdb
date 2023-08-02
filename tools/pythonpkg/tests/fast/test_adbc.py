import duckdb
import pytest
import sys
import datetime
import os

adbc_driver_manager = pytest.importorskip("adbc_driver_manager.dbapi")
adbc_driver_manager_lib = pytest.importorskip("adbc_driver_manager._lib")

pyarrow = pytest.importorskip("pyarrow")

# When testing local, if you build via BUILD_PYTHON=1 make, you need to manually set up the
# dylib duckdb path.
driver_path = duckdb.duckdb.__file__


@pytest.fixture
def duck_conn():
    if sys.platform.startswith("win"):
        pytest.xfail("not supported on Windows")
    with adbc_driver_manager.connect(driver=driver_path, entrypoint="duckdb_adbc_init") as conn:
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


def test_connection_get_table_types(duck_conn):
    assert duck_conn.adbc_get_table_types() == []
    with duck_conn.cursor() as cursor:
        # Test Default Schema
        cursor.execute("CREATE TABLE tableschema (ints BIGINT)")
    assert duck_conn.adbc_get_table_types() == ['BASE TABLE']


def test_connection_get_objects(duck_conn):
    with duck_conn.cursor() as cursor:
        cursor.execute("CREATE TABLE getobjects (ints BIGINT)")
    assert duck_conn.adbc_get_objects(depth="all").read_all().to_pylist() == [
        {
            'db_schema_name': 'main',
            'db_schema_tables': [
                {
                    'table_name': 'getobjects',
                    'table_columns': [{'column_name': 'ints', 'ordinal_position': 2, 'remarks': ''}],
                }
            ],
        }
    ]


def test_commit(tmp_path):
    if sys.platform.startswith("win"):
        pytest.xfail("not supported on Windows")
    db = os.path.join(tmp_path, "tmp.db")
    if os.path.exists(db):
        os.remove(db)
    table = example_table()
    db_kwargs = {"path": f"{db}"}
    # Start connection with auto-commit off
    with adbc_driver_manager.connect(
        driver=driver_path,
        entrypoint="duckdb_adbc_init",
        db_kwargs=db_kwargs,
    ) as conn:
        assert not conn._autocommit
        with conn.cursor() as cur:
            cur.adbc_ingest("ingest", table, "create")

    # Check Data is not there
    with adbc_driver_manager.connect(
        driver=driver_path,
        entrypoint="duckdb_adbc_init",
        db_kwargs=db_kwargs,
        autocommit=True,
    ) as conn:
        assert conn._autocommit
        with conn.cursor() as cur:
            # This errors because the table does not exist
            with pytest.raises(
                adbc_driver_manager_lib.InternalError,
                match=r'Table with name ingest does not exist!',
            ):
                cur.execute("SELECT count(*) from ingest")

            cur.adbc_ingest("ingest", table, "create")

    # This now works because we enabled autocommit
    with adbc_driver_manager.connect(
        driver=driver_path,
        entrypoint="duckdb_adbc_init",
        db_kwargs=db_kwargs,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) from ingest")
            assert cur.fetch_arrow_table().to_pydict() == {'count_star()': [4]}


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


def test_prepared_statement(duck_conn):
    with duck_conn.cursor() as cursor:
        cursor.adbc_prepare("SELECT 1")
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None


def test_statement_query(duck_conn):
    with duck_conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

        cursor.execute("SELECT 1 AS foo")
        assert cursor.fetch_arrow_table().to_pylist() == [{"foo": 1}]


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
