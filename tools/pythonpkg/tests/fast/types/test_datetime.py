import duckdb
import datetime
import pytest

def create_query(positive, type):
    inf = 'infinity' if positive else '-infinity'
    return f"""
        select '{inf}'::{type}
    """

class TestDateTime(object):
    @pytest.mark.parametrize('positive', [
        True,
        False
    ])
    @pytest.mark.parametrize('type', [
        'TIMESTAMP',
        'TIMESTAMP_S',
        'TIMESTAMP_MS',
        'TIMESTAMP_NS',
        'TIMESTAMPTZ',
        'TIMESTAMP_US',
    ])
    def test_timestamp_infinity(self, positive, type):
        con = duckdb.connect()

        expected_val = datetime.datetime.max if positive else datetime.datetime.min
        query = create_query(positive, type)
        if positive and type in [
                'TIMESTAMP_NS'
        ]:
            # This query is not supported in core duckdb
            with pytest.raises(duckdb.Error):
                con.execute(query).fetchall()
        elif not positive and type in [
                'TIMESTAMP_NS',
                'TIMESTAMP_MS',
                'TIMESTAMP_S',
        ]:
            # This query is not supported in core duckdb
            with pytest.raises(duckdb.Error):
                con.execute(query).fetchall()
        else:
            res = con.sql(query).fetchall()[0][0]
            assert res == expected_val
