import duckdb
import datetime
import pytz
import os
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
from packaging.version import Version

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data', 'tz.parquet')


class TestNativeTimeZone(object):
    def test_native_python_timestamp_timezone(self, duckdb_cursor):
        duckdb_cursor.execute("SET timezone='America/Los_Angeles';")
        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").fetchone()
        assert res[0].hour == 14 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'America/Los_Angeles'

        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").fetchall()[0]
        assert res[0].hour == 14 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'America/Los_Angeles'

        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").fetchmany(1)[0]
        assert res[0].hour == 14 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'America/Los_Angeles'

        duckdb_cursor.execute("SET timezone='UTC';")
        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").fetchone()
        assert res[0].hour == 21 and res[0].minute == 52
        assert res[0].tzinfo.zone == 'UTC'

    def test_native_python_time_timezone(self, duckdb_cursor):
        res = duckdb_cursor.execute(f"select TimeRecStart::TIMETZ as tz from '{filename}'").fetchone()
        assert res == (datetime.time(21, 52, 27, tzinfo=datetime.timezone.utc),)

        roundtrip = duckdb_cursor.execute("select $1", [res[0]]).fetchone()
        assert roundtrip == res

    def test_pandas_timestamp_timezone(self, duckdb_cursor):
        res = duckdb_cursor.execute("SET timezone='America/Los_Angeles';")
        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").df()
        assert res.dtypes["tz"].tz.zone == 'America/Los_Angeles'
        assert res['tz'][0].hour == 14 and res['tz'][0].minute == 52

        duckdb_cursor.execute("SET timezone='UTC';")
        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").df()
        assert res['tz'][0].hour == 21 and res['tz'][0].minute == 52

    def test_pandas_timestamp_time(self, duckdb_cursor):
        with pytest.raises(
            duckdb.NotImplementedException, match="Not implemented Error: Unsupported type \"TIME WITH TIME ZONE\""
        ):
            duckdb_cursor.execute(f"select TimeRecStart::TIMETZ  as tz  from '{filename}'").df()

    @pytest.mark.skipif(
        Version(pa.__version__) < Version('15.0.0'), reason="pyarrow 14.0.2 'to_pandas' causes a DeprecationWarning"
    )
    def test_arrow_timestamp_timezone(self, duckdb_cursor):
        res = duckdb_cursor.execute("SET timezone='America/Los_Angeles';")
        table = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").arrow()
        res = table.to_pandas()
        assert res.dtypes["tz"].tz.zone == 'America/Los_Angeles'
        assert res['tz'][0].hour == 14 and res['tz'][0].minute == 52

        duckdb_cursor.execute("SET timezone='UTC';")
        res = duckdb_cursor.execute(f"select TimeRecStart as tz  from '{filename}'").arrow().to_pandas()
        assert res.dtypes["tz"].tz.zone == 'UTC'
        assert res['tz'][0].hour == 21 and res['tz'][0].minute == 52

    def test_arrow_timestamp_time(self, duckdb_cursor):
        duckdb_cursor.execute("SET timezone='America/Los_Angeles';")
        res1 = duckdb_cursor.execute(f"select TimeRecStart::TIMETZ  as tz  from '{filename}'").arrow().to_pandas()
        res2 = duckdb_cursor.execute(f"select TimeRecStart::TIMETZ::TIME  as tz  from '{filename}'").arrow().to_pandas()
        assert res1['tz'][0].hour == 21 and res1['tz'][0].minute == 52
        assert res2['tz'][0].hour == res2['tz'][0].hour and res2['tz'][0].minute == res1['tz'][0].minute

        duckdb_cursor.execute("SET timezone='UTC';")
        res1 = duckdb_cursor.execute(f"select TimeRecStart::TIMETZ  as tz  from '{filename}'").arrow().to_pandas()
        res2 = duckdb_cursor.execute(f"select TimeRecStart::TIMETZ::TIME  as tz  from '{filename}'").arrow().to_pandas()
        assert res1['tz'][0].hour == 21 and res1['tz'][0].minute == 52
        assert res2['tz'][0].hour == res2['tz'][0].hour and res2['tz'][0].minute == res1['tz'][0].minute
