# cursor description

import duckdb
import tempfile
import os

def check_exception(f):
    had_exception = False
    try:
        f()
    except:
        had_exception = True
    assert(had_exception)

class TestReadOnly(object):
    def test_readonly(self, duckdb_cursor):
        db = tempfile.mkstemp()[1]
        try:
            os.remove(db)
        except OSError:
            pass

        check_exception(lambda :duckdb.connect(":memory:", True))

        # this is forbidden

        con_rw = duckdb.connect(db, False).cursor()
        con_rw.execute("create table a (i integer)")
        con_rw.execute("insert into a values (42)")
        del con_rw

        con_ro = duckdb.connect(db, True).cursor()
        con_ro.execute("select * from a").fetchall()

        check_exception(lambda : con_ro.execute("delete from a"))
       
        del con_ro

        con_rw = duckdb.connect(db, False).cursor()
        con_rw.execute("drop table a")
        del con_rw


