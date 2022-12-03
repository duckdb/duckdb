import duckdb
import os
import pytest

def test_multiple_writes():
    con1 = duckdb.connect("test.db")
    con2 = duckdb.connect("test.db")
    con1.execute("CREATE TABLE foo1 as SELECT 1 as a, 2 as b")
    con2.execute("CREATE TABLE bar1 as SELECT 2 as a, 3 as b")
    con2.close()
    con1.close()
    con3 = duckdb.connect("test.db")
    tbls = con3.execute("select * from information_schema.tables").fetchall()
    assert tbls == [(None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)] or tbls == [(None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)]

    con4 = duckdb.connect(os.path.join(os.getcwd(), "test.db"))
    tbls = con4.execute("select * from information_schema.tables").fetchall()
    assert tbls == [(None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)] or tbls == [(None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)]
    del con1
    del con2
    del con3
    del con4

    os.remove('test.db')

def test_multiple_writes_memory():
    con1 = duckdb.connect()
    con2 = duckdb.connect()
    con1.execute("CREATE TABLE foo1 as SELECT 1 as a, 2 as b")
    con2.execute("CREATE TABLE bar1 as SELECT 2 as a, 3 as b")
    con3 = duckdb.connect(":memory:")
    tbls = con1.execute("select * from information_schema.tables").fetchall()
    assert tbls == [(None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)] 
    tbls = con2.execute("select * from information_schema.tables").fetchall()
    assert tbls == [(None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)] 
    tbls = con3.execute("select * from information_schema.tables").fetchall()
    assert tbls == [] 
    del con1
    del con2
    del con3

def test_multiple_writes_named_memory():
    con1 = duckdb.connect(":memory:1")
    con2 = duckdb.connect(":memory:1")
    con1.execute("CREATE TABLE foo1 as SELECT 1 as a, 2 as b")
    con2.execute("CREATE TABLE bar1 as SELECT 2 as a, 3 as b")
    con3 = duckdb.connect(":memory:1")
    tbls = con3.execute("select * from information_schema.tables").fetchall()
    assert tbls == [(None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)] or tbls == [(None, 'main', 'bar1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None), (None, 'main', 'foo1', 'BASE TABLE', None, None, None, None, None, 'YES', 'NO', None)]
    del con1
    del con2
    del con3

def test_diff_config():
    con1 = duckdb.connect("test.db",False)
    with pytest.raises(duckdb.Error, match="Can't open a connection to same database file with a different configuration than existing connections"):
        con2 = duckdb.connect("test.db",True)
    con1.close()
    del con1



    


    
