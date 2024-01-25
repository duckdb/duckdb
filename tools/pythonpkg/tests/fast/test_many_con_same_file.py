import duckdb
import os
import pytest


def get_tables(con):
    tbls = con.execute("SHOW TABLES").fetchall()
    tbls = [x[0] for x in tbls]
    tbls.sort()
    return tbls


def test_multiple_writes():
    try:
        os.remove("test.db")
    except:
        pass
    con1 = duckdb.connect("test.db")
    con2 = duckdb.connect("test.db")
    con1.execute("CREATE TABLE foo1 as SELECT 1 as a, 2 as b")
    con2.execute("CREATE TABLE bar1 as SELECT 2 as a, 3 as b")
    con2.close()
    con1.close()
    con3 = duckdb.connect("test.db")
    tbls = get_tables(con3)
    assert tbls == ['bar1', 'foo1']
    del con1
    del con2
    del con3

    try:
        os.remove("test.db")
    except:
        pass


def test_multiple_writes_memory():
    con1 = duckdb.connect()
    con2 = duckdb.connect()
    con1.execute("CREATE TABLE foo1 as SELECT 1 as a, 2 as b")
    con2.execute("CREATE TABLE bar1 as SELECT 2 as a, 3 as b")
    con3 = duckdb.connect(":memory:")
    tbls = get_tables(con1)
    assert tbls == ['foo1']
    tbls = get_tables(con2)
    assert tbls == ['bar1']
    tbls = get_tables(con3)
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
    tbls = get_tables(con3)
    assert tbls == ['bar1', 'foo1']
    del con1
    del con2
    del con3


def test_diff_config():
    con1 = duckdb.connect("test.db", False)
    with pytest.raises(
        duckdb.ConnectionException,
        match="Can't open a connection to same database file with a different configuration than existing connections",
    ):
        con2 = duckdb.connect("test.db", True)
    con1.close()
    del con1
