import duckdb
import pandas
import pytest

from datetime import datetime
from pytz import timezone
from conftest import pandas_2_or_higher


@pytest.mark.parametrize('timezone', ['UTC', 'CET', 'Asia/Kathmandu'])
@pytest.mark.skipif(not pandas_2_or_higher(), reason="Pandas <2.0.0 does not support timezones in the metadata string")
def test_run_pandas_with_tz(timezone):
    con = duckdb.connect()
    con.execute(f"SET TimeZone = '{timezone}'")
    df = pandas.DataFrame(
        {
            'timestamp': pandas.Series(
                data=[pandas.Timestamp(year=2022, month=1, day=1, hour=10, minute=15, tz=timezone, unit='us')],
                dtype=f'datetime64[us, {timezone}]',
            )
        }
    )
    duck_df = con.from_df(df).df()
    assert duck_df['timestamp'][0] == df['timestamp'][0]


def test_timestamp_conversion(duckdb_cursor):
    tzinfo = pandas.Timestamp('2024-01-01 00:00:00+0100', tz='Europe/Copenhagen').tzinfo
    ts_df = pandas.DataFrame(
        {
            "ts": [
                pandas.Timestamp('2024-01-01 00:00:00+0100', tz=tzinfo),
                pandas.Timestamp('2024-01-02 00:00:00+0100', tz=tzinfo),
            ]
        }
    )

    query = """
        select
            *
        from ts_df
        where ts = $notationtime
    """
    params_zoneinfo = {"notationtime": datetime(2024, 1, 1, tzinfo=tzinfo)}
    duckdb_cursor.execute("set TimeZone = 'Europe/Copenhagen'")
    rel = duckdb_cursor.execute(query, parameters=params_zoneinfo)
    res = rel.fetchall()
    assert res[0][0] == datetime(2024, 1, 1, tzinfo=tzinfo)
