# cursor description

import duckdb
import tempfile
import os

def check_exception(f):
    had_exception = False
    try:
        f()
    except BaseException:
        had_exception = True
    assert(had_exception)

class TestConnectionClose(object):
    def test_connection_close(self, duckdb_cursor):
        fd, db = tempfile.mkstemp()
        os.close(fd)
        os.remove(db)
        con = duckdb.connect(db)
        cursor = con.cursor()
        cursor.execute("create table a (i integer)")
        cursor.execute("insert into a values (42)")
        con.close()
        check_exception(lambda :cursor.execute("select * from a"))

    def test_reopen_connection(self, duckdb_cursor):
        fd, db = tempfile.mkstemp()
        os.close(fd)
        os.remove(db)
        con = duckdb.connect(db)
        cursor = con.cursor()
        cursor.execute("create table a (i integer)")
        cursor.execute("insert into a values (42)")
        con.close()
        con = duckdb.connect(db)
        cursor = con.cursor()
        results = cursor.execute("select * from a").fetchall()
        assert results == [(42,)]
