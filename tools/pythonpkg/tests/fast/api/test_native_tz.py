import duckdb
import datetime
import pytz
import os
import pandas as pd
import pytest

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data', 'tz.parquet')


class TestNativeTimeZone(object):
    def test_native_python_timestamp_timezone(self):
        con = duckdb.connect('')
        con.execute("SET timezone='America/Los_Angeles';")
        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").fetchone()
        assert res[0].hour == 14 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'America/Los_Angeles'

        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").fetchall()[0]
        assert res[0].hour == 14 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'America/Los_Angeles'

        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").fetchmany(1)[0]
        assert res[0].hour == 14 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'America/Los_Angeles'

        con.execute("SET timezone='UTC';")
        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").fetchone()
        assert res[0].hour == 21 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'UTC'

    def test_native_python_time_timezone(self):
        con = duckdb.connect('')
        with pytest.raises(duckdb.NotImplementedException, match="Not implemented Error: Unsupported type"):
            con.execute(f"select TimeRecStart::TIMETZ  as tz  from '{filename}'").fetchone()

    def test_pandas_timestamp_timezone(self):
        con = duckdb.connect('')
        res = con.execute("SET timezone='America/Los_Angeles';")
        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").df()
        assert res.dtypes["tz"].tz.zone == 'America/Los_Angeles'
        assert res['tz'][0].hour == 14 and res['tz'][0].minute == 52

        con.execute("SET timezone='UTC';")
        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").df()
        assert res['tz'][0].hour == 21 and res['tz'][0].minute == 52

    def test_pandas_timestamp_time(self):
        con = duckdb.connect('')
        with pytest.raises(
            duckdb.NotImplementedException, match="Not implemented Error: Unsupported type \"TIME WITH TIME ZONE\""
        ):
            con.execute(f"select TimeRecStart::TIMETZ  as tz  from '{filename}'").df()

    def test_arrow_timestamp_timezone(self):
        pa = pytest.importorskip('pyarrow')
        con = duckdb.connect('')
        res = con.execute("SET timezone='America/Los_Angeles';")
        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").arrow().to_pandas()
        assert res.dtypes["tz"].tz.zone == 'America/Los_Angeles'
        assert res['tz'][0].hour == 14 and res['tz'][0].minute == 52

        con.execute("SET timezone='UTC';")
        res = con.execute(f"select TimeRecStart as tz  from '{filename}'").arrow().to_pandas()
        assert res.dtypes["tz"].tz.zone == 'UTC'
        assert res['tz'][0].hour == 21 and res['tz'][0].minute == 52

    def test_arrow_timestamp_time(self):
        pa = pytest.importorskip('pyarrow')
        con = duckdb.connect('')
        with pytest.raises(duckdb.NotImplementedException, match="Unsupported Arrow type"):
            con.execute(f"select TimeRecStart::TIMETZ  as tz  from '{filename}'").arrow()
