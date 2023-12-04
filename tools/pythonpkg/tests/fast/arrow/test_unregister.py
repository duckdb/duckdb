import pytest
import tempfile
import gc
import duckdb
import os

try:
    import pyarrow
    import pyarrow.parquet

    can_run = True
except:
    can_run = False


class TestArrowUnregister(object):
    def test_arrow_unregister1(self, duckdb_cursor):
        if not can_run:
            return
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        cols = 'id, first_name, last_name, email, gender, ip_address, cc, country, birthdate, salary, title, comments'

        arrow_table_obj = pyarrow.parquet.read_table(parquet_filename)
        connection = duckdb.connect(":memory:")
        connection.register("arrow_table", arrow_table_obj)

        arrow_table_2 = connection.execute("SELECT * FROM arrow_table;").fetch_arrow_table()
        connection.unregister("arrow_table")
        with pytest.raises(duckdb.CatalogException, match='Table with name arrow_table does not exist'):
            connection.execute("SELECT * FROM arrow_table;").fetch_arrow_table()
        with pytest.raises(duckdb.CatalogException, match='View with name arrow_table does not exist'):
            connection.execute("DROP VIEW arrow_table;")
        connection.execute("DROP VIEW IF EXISTS arrow_table;")

    def test_arrow_unregister2(self, duckdb_cursor):
        if not can_run:
            return
        fd, db = tempfile.mkstemp()
        os.close(fd)
        os.remove(db)

        connection = duckdb.connect(db)
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        cols = 'id, first_name, last_name, email, gender, ip_address, cc, country, birthdate, salary, title, comments'
        arrow_table_obj = pyarrow.parquet.read_table(parquet_filename)
        connection.register("arrow_table", arrow_table_obj)
        connection.unregister("arrow_table")  # Attempting to unregister.
        connection.close()
        # Reconnecting while Arrow Table still in mem.
        connection = duckdb.connect(db)
        assert len(connection.execute("PRAGMA show_tables;").fetchall()) == 0
        with pytest.raises(duckdb.CatalogException, match='Table with name arrow_table does not exist'):
            connection.execute("SELECT * FROM arrow_table;").fetch_arrow_table()
        connection.close()
        del arrow_table_obj
        gc.collect()
        # Reconnecting after Arrow Table is freed.
        connection = duckdb.connect(db)
        assert len(connection.execute("PRAGMA show_tables;").fetchall()) == 0
        with pytest.raises(duckdb.CatalogException, match='Table with name arrow_table does not exist'):
            connection.execute("SELECT * FROM arrow_table;").fetch_arrow_table()
        connection.close()
