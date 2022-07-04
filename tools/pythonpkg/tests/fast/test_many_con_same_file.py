import duckdb
import os
import pytest

class TestManyConnectionSameFile(object):

    def test_multiple_writes(self, duckdb_cursor):
        con1 = duckdb.connect("test.db")
        con2 = duckdb.connect("test.db")
        con1.execute("CREATE TABLE foo1 as SELECT 1 as a, 2 as b")
        con1.commit()
        con2.execute("CREATE TABLE bar1 as SELECT 2 as a, 3 as b")
        con2.commit()
        con2.close()
        con1.close()
        con3 = duckdb.connect("test.db")
        tbls = con3.execute("select * from information_schema.tables").fetchall()
        assert tbls == [(None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)]
        con3.close()
        del con1
        del con2
        del con3
        os.remove('test.db') 

    def test_diff_config(self, duckdb_cursor):
        con1 = duckdb.connect("test.db")
        with pytest.raises(Exception, match="Can't open a connection to same database file with a different configuration than existing connections"):
            con2 = duckdb.connect("test.db",True)
        con1.close()
        del con1

        con1 = duckdb.connect("test.db", config={'default_order': 'desc'})

        with pytest.raises(Exception, match="Can't open a connection to same database file with a different configuration than existing connections"):
            con2 = duckdb.connect("test.db", config={'default_order': 'asc'})
        
        con2 = duckdb.connect("test.db", config={'default_order': 'desc'})
        con1.close()
        con2.close()
        del con1
        del con2
