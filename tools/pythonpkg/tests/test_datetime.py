
import datetime

class TestDatetime(object):
    def test_date(self, duckdb_cursor):
        today = datetime.date.today()
        duckdb_cursor.execute("CREATE TABLE t1 (d date)")
        duckdb_cursor.execute("INSERT INTO t1 VALUES (?)", (today, ))
        result = duckdb_cursor.execute("SELECT * FROM t1").fetchone()
        assert result == (today, )

    def test_datetime(self, duckdb_cursor):
        now = datetime.datetime.utcnow().replace(microsecond=0)
        duckdb_cursor.execute("CREATE TABLE t2 (t timestamp)")
        duckdb_cursor.execute("INSERT INTO t2 VALUES (?)", (now, ))
        result = duckdb_cursor.execute("SELECT * FROM t2").fetchone()
        assert result == (now, )

