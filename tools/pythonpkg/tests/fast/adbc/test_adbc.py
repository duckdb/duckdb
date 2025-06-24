import duckdb
import pytest
import sys
import datetime
import os

if sys.version_info < (3, 9):
    pytest.skip(
        "Python Version must be higher or equal to 3.9 to run this test",
        allow_module_level=True,
    )

adbc_driver_manager = pytest.importorskip("adbc_driver_manager.dbapi")
adbc_driver_manager_lib = pytest.importorskip("adbc_driver_manager._lib")

pyarrow = pytest.importorskip("pyarrow")

# When testing local, if you build via BUILD_PYTHON=1 make, you need to manually set up the
# dylib duckdb path.
driver_path = duckdb.duckdb.__file__


@pytest.fixture
def duck_conn():
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
        cursor.execute("CREATE TABLE getobjects (ints BIGINT PRIMARY KEY)")
        depth_all = duck_conn.adbc_get_objects(depth="all").read_all()
    assert sorted_get_objects(depth_all.to_pylist()) is not None

    depth_tables = duck_conn.adbc_get_objects(depth="tables").read_all()
    assert sorted_get_objects(depth_tables.to_pylist()) is not None

    depth_db_schemas = duck_conn.adbc_get_objects(depth="db_schemas").read_all()
    assert sorted_get_objects(depth_db_schemas.to_pylist()) is not None

    depth_catalogs = duck_conn.adbc_get_objects(depth="catalogs").read_all()
    assert sorted_get_objects(depth_catalogs.to_pylist()) is not None

    # All result schemas should be the same
    assert depth_all.schema == depth_tables.schema
    assert depth_all.schema == depth_db_schemas.schema
    assert depth_all.schema == depth_catalogs.schema


def test_connection_get_objects_filters(duck_conn):
    with duck_conn.cursor() as cursor:
        cursor.execute("CREATE TABLE getobjects (ints BIGINT PRIMARY KEY)")

    no_filter = duck_conn.adbc_get_objects(depth="all").read_all()
    assert sorted_get_objects(no_filter.to_pylist()) is not None

    column_filter = duck_conn.adbc_get_objects(depth="all", column_name_filter="notexist").read_all()
    assert sorted_get_objects(column_filter.to_pylist()) is not None

    table_name_filter = duck_conn.adbc_get_objects(depth="all", table_name_filter="notexist").read_all()
    assert sorted_get_objects(table_name_filter.to_pylist()) is not None

    db_schema_filter = duck_conn.adbc_get_objects(depth="all", db_schema_filter="notexist").read_all()
    assert sorted_get_objects(db_schema_filter.to_pylist()) is not None

    catalog_filter = duck_conn.adbc_get_objects(depth="all", catalog_filter="notexist").read_all()
    assert catalog_filter.to_pylist() == []

    assert no_filter.schema == column_filter.schema
    assert no_filter.schema == table_name_filter.schema
    assert no_filter.schema == db_schema_filter.schema
    assert no_filter.schema == catalog_filter.schema


def test_commit(tmp_path):
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

        # Test invalid catalog name
        with pytest.raises(
            adbc_driver_manager.InternalError,
            match=r'Catalog "bla" does not exist',
        ):
            duck_conn.adbc_get_table_schema("tableschema", catalog_filter="bla", db_schema_filter="test")

        # Catalog and DB Schema name
        assert duck_conn.adbc_get_table_schema(
            "tableschema", catalog_filter="memory", db_schema_filter="test"
        ) == pyarrow.schema(
            [
                ("test_ints", "int64"),
            ]
        )

        # DB Schema is inferred to be "main" if unspecified
        assert duck_conn.adbc_get_table_schema("tableschema", catalog_filter="memory") == pyarrow.schema(
            [
                ("ints", "int64"),
            ]
        )


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
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data", "category.csv")
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


def sorted_get_objects(catalogs):
    res = []
    for catalog in sorted(catalogs, key=lambda cat: cat['catalog_name']):
        new_catalog = {
            "catalog_name": catalog['catalog_name'],
            "catalog_db_schemas": [],
        }

        for db_schema in sorted(catalog['catalog_db_schemas'] or [], key=lambda sch: sch['db_schema_name']):
            new_db_schema = {
                "db_schema_name": db_schema['db_schema_name'],
                "db_schema_tables": [],
            }

            for table in sorted(db_schema['db_schema_tables'] or [], key=lambda tab: tab['table_name']):
                new_table = {
                    "table_name": table['table_name'],
                    "table_type": table['table_type'],
                    "table_columns": [],
                    "table_constraints": [],
                }

                for column in sorted(table['table_columns'] or [], key=lambda col: col['ordinal_position']):
                    new_table["table_columns"].append(column)

                for constraint in sorted(table['table_constraints'] or [], key=lambda con: con['constraint_name']):
                    new_table["table_constraints"].append(constraint)

                new_db_schema["db_schema_tables"].append(new_table)
            new_catalog["catalog_db_schemas"].append(new_db_schema)
        res.append(new_catalog)

    return res
