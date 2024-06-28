import duckdb
import datetime
import pytest


def create_query(positive, type):
    inf = 'infinity' if positive else '-infinity'
    return f"""
        select '{inf}'::{type}
    """


class TestDateTimeDateTime(object):
    @pytest.mark.parametrize('positive', [True, False])
    @pytest.mark.parametrize(
        'type',
        [
            'TIMESTAMP',
            'TIMESTAMP_S',
            'TIMESTAMP_MS',
            'TIMESTAMP_NS',
            'TIMESTAMPTZ',
            'TIMESTAMP_US',
        ],
    )
    def test_timestamp_infinity(self, positive, type):
        con = duckdb.connect()

        if type in ['TIMESTAMP_S', 'TIMESTAMP_MS', 'TIMESTAMP_NS']:
            # Infinity (both positive and negative) is not supported for non-usecond timetamps
            return

        expected_val = datetime.datetime.max if positive else datetime.datetime.min
        query = create_query(positive, type)
        res = con.sql(query).fetchall()[0][0]
        assert res == expected_val

    def test_timestamp_infinity_roundtrip(self):
        con = duckdb.connect()

        # positive infinity
        con.execute("select $1, $1 = 'infinity'::TIMESTAMP", [datetime.datetime.max])
        res = con.fetchall()
        assert res == [(datetime.datetime.max, False)]

        # negative infinity
        con.execute("select $1, $1 = '-infinity'::TIMESTAMP", [datetime.datetime.min])
        res = con.fetchall()
        assert res == [(datetime.datetime.min, False)]

    def test_convert_negative_interval(self, duckdb_cursor):
        res = duckdb_cursor.execute(
            "SELECT CAST('2023-07-22T11:28:07' AS TIMESTAMP) - CAST('2023-07-23T11:28:07' AS TIMESTAMP)"
        ).fetchall()
        assert res == [(datetime.timedelta(days=-1),)]
