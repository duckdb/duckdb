import duckdb
import pytest

class TestConnectionInterrupt(object):
    def test_connection_interrupt(self):
        conn = duckdb.connect()
        conn.execute("create table t (i integer)")
        conn.execute("insert into t values (1)")
        conn.interrupt()
        conn.execute("drop table t")

    def test_interrupt_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.interrupt()
