import duckdb
import datetime


class TestDateTimeDate(object):
    def test_date_infinity(self):
        con = duckdb.connect()
        # Positive infinity
        con.execute("SELECT 'infinity'::DATE")
        result = con.fetchall()
        # datetime.date.max
        assert result == [(datetime.date(9999, 12, 31),)]

        con.execute("SELECT '-infinity'::DATE")
        result = con.fetchall()
        # datetime.date.min
        assert result == [(datetime.date(1, 1, 1),)]

    def test_date_infinity_roundtrip(self):
        con = duckdb.connect()

        # positive infinity
        con.execute("select $1, $1 = 'infinity'::DATE", [datetime.date.max])
        res = con.fetchall()
        assert res == [(datetime.date.max, True)]

        # negative infinity
        con.execute("select $1, $1 = '-infinity'::DATE", [datetime.date.min])
        res = con.fetchall()
        assert res == [(datetime.date.min, True)]
