# simple DB API testcase

import pytest
import duckdb


class TestDBAPICursor(object):
    def test_cursor_basic(self):
        # Create a connection
        con = duckdb.connect(':memory:')
        # Then create a cursor on the connection
        cursor = con.cursor()
        # Use the cursor for queries
        res = cursor.execute("select [1,2,3,NULL,4]").fetchall()
        assert res == [([1, 2, 3, None, 4],)]

    def test_cursor_preexisting(self):
        con = duckdb.connect(':memory:')
        con.execute("create table tbl as select i a, i+1 b, i+2 c from range(5) tbl(i)")
        cursor = con.cursor()
        res = cursor.execute("select * from tbl").fetchall()
        assert res == [(0, 1, 2), (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)]

    def test_cursor_after_creation(self):
        con = duckdb.connect(':memory:')
        # First create the cursor
        cursor = con.cursor()
        # Then create table on the source connection
        con.execute("create table tbl as select i a, i+1 b, i+2 c from range(5) tbl(i)")
        res = cursor.execute("select * from tbl").fetchall()
        assert res == [(0, 1, 2), (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)]

    def test_cursor_mixed(self):
        con = duckdb.connect(':memory:')
        # First create the cursor
        cursor = con.cursor()
        # Then create table on the cursor
        cursor.execute("create table tbl as select i a, i+1 b, i+2 c from range(5) tbl(i)")
        # Close the cursor and create a new one
        cursor.close()
        cursor = con.cursor()
        res = cursor.execute("select * from tbl").fetchall()
        assert res == [(0, 1, 2), (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)]

    def test_cursor_temp_schema_closed(self):
        con = duckdb.connect(':memory:')
        cursor = con.cursor()
        cursor.execute("create temp table tbl as select * from range(100)")
        other_cursor = con.cursor()
        # Connection that created the table is closed
        cursor.close()
        with pytest.raises(duckdb.CatalogException):
            # This table does not exist in this cursor
            res = other_cursor.execute("select * from tbl").fetchall()

    def test_cursor_temp_schema_open(self):
        con = duckdb.connect(':memory:')
        cursor = con.cursor()
        cursor.execute("create temp table tbl as select * from range(100)")
        other_cursor = con.cursor()
        # Connection that created the table is still open
        # cursor.close()
        with pytest.raises(duckdb.CatalogException):
            # This table does not exist in this cursor
            res = other_cursor.execute("select * from tbl").fetchall()

    def test_cursor_temp_schema_both(self):
        con = duckdb.connect(':memory:')
        cursor1 = con.cursor()
        cursor2 = con.cursor()
        cursor3 = con.cursor()
        cursor1.execute("create temp table tbl as select i from range(10) tbl(i)")
        cursor2.execute("create temp table tbl as select i+10 from range(10) tbl(i)")
        with pytest.raises(duckdb.CatalogException):
            # This table does not exist in this cursor
            res = cursor3.execute("select * from tbl").fetchall()
        res = cursor1.execute("select * from tbl").fetchall()
        assert res == [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)]
        res = cursor2.execute("select * from tbl").fetchall()
        assert res == [(10,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,)]

        cursor1.close()
        cursor2.close()

    def test_cursor_timezone(self):
        db = duckdb.connect()

        # We set the 'timezone' setting globally
        con1 = db.cursor()
        db.execute("set global timezone='UTC'")

        # Because the 'timezone' setting was not explicitly set for the connection
        # the setting of the DBConfig is used instead
        res = con1.execute("SELECT make_timestamptz(2000,01,20,03,30,59)").fetchone()
        assert str(res) == '(datetime.datetime(2000, 1, 20, 3, 30, 59, tzinfo=<UTC>),)'

    def test_cursor_closed(self):
        con = duckdb.connect(':memory:')
        con.close()
        with pytest.raises(duckdb.ConnectionException):
            cursor = con.cursor()

    def test_cursor_used_after_connection_closed(self):
        con = duckdb.connect(':memory:')
        cursor = con.cursor()
        con.close()
        with pytest.raises(duckdb.ConnectionException):
            cursor.execute("select [1,2,3,4]")

    def test_cursor_used_after_close(self):
        con = duckdb.connect(':memory:')
        cursor = con.cursor()
        cursor.close()
        with pytest.raises(duckdb.ConnectionException):
            cursor.execute("select [1,2,3,4]")
