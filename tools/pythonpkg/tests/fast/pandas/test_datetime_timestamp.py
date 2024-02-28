import duckdb
import datetime
import numpy as np
import pytest
from conftest import NumpyPandas, ArrowPandas
from packaging.version import Version

pd = pytest.importorskip("pandas")


class TestDateTimeTimeStamp(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_timestamp_high(self, pandas, duckdb_cursor):
        duckdb_time = duckdb_cursor.sql("SELECT '2260-01-01 23:59:00'::TIMESTAMP AS '0'").df()
        df_in = pandas.DataFrame(
            {
                0: pandas.Series(
                    data=[datetime.datetime(year=2260, month=1, day=1, hour=23, minute=59)],
                    dtype='datetime64[us]',
                )
            }
        )
        df_out = duckdb_cursor.sql("select * from df_in").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_timestamp_low(self, pandas, duckdb_cursor):
        duckdb_time = duckdb_cursor.sql(
            """
            SELECT '1680-01-01 23:59:00.234243'::TIMESTAMP AS '0'
        """
        ).df()
        df_in = pandas.DataFrame(
            {
                '0': pandas.Series(
                    data=[
                        pandas.Timestamp(
                            datetime.datetime(year=1680, month=1, day=1, hour=23, minute=59, microsecond=234243),
                            unit='us',
                        )
                    ],
                    dtype='datetime64[us]',
                )
            }
        )
        print('original:', duckdb_time['0'].dtype)
        print('df_in:', df_in['0'].dtype)
        df_out = duckdb_cursor.sql("select * from df_in").df()
        print('df_out:', df_out['0'].dtype)
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.skipif(
        Version(pd.__version__) < Version('2.0.2'), reason="pandas < 2.0.2 does not properly convert timezones"
    )
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_timestamp_timezone_regular(self, pandas, duckdb_cursor):
        duckdb_time = duckdb_cursor.sql(
            """
            SELECT timestamp '2022-01-01 12:00:00' AT TIME ZONE 'Pacific/Easter' as "0"
        """
        ).df()

        offset = datetime.timedelta(hours=-2)
        timezone = datetime.timezone(offset)
        df_in = pandas.DataFrame(
            {
                0: pandas.Series(
                    data=[datetime.datetime(year=2022, month=1, day=1, hour=15, tzinfo=timezone)], dtype='object'
                )
            }
        )
        df_out = duckdb_cursor.sql("select * from df_in").df()
        print(df_out)
        print(duckdb_time)
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.skipif(
        Version(pd.__version__) < Version('2.0.2'), reason="pandas < 2.0.2 does not properly convert timezones"
    )
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_timestamp_timezone_negative_extreme(self, pandas, duckdb_cursor):
        duckdb_time = duckdb_cursor.sql(
            """
            SELECT timestamp '2022-01-01 12:00:00' AT TIME ZONE 'Chile/EasterIsland' as "0"
        """
        ).df()

        offset = datetime.timedelta(hours=-19)
        timezone = datetime.timezone(offset)

        df_in = pandas.DataFrame(
            {
                0: pandas.Series(
                    data=[datetime.datetime(year=2021, month=12, day=31, hour=22, tzinfo=timezone)], dtype='object'
                )
            }
        )
        df_out = duckdb_cursor.sql("select * from df_in").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.skipif(
        Version(pd.__version__) < Version('2.0.2'), reason="pandas < 2.0.2 does not properly convert timezones"
    )
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_timestamp_timezone_positive_extreme(self, pandas, duckdb_cursor):
        duckdb_time = duckdb_cursor.sql(
            """
            SELECT timestamp '2021-12-31 23:00:00' AT TIME ZONE 'kea_CV' as "0"
        """
        ).df()

        # 'kea_CV' is 20 hours ahead of UTC
        offset = datetime.timedelta(hours=20)
        timezone = datetime.timezone(offset)

        df_in = pandas.DataFrame(
            {
                0: pandas.Series(
                    data=[datetime.datetime(year=2022, month=1, day=1, hour=19, tzinfo=timezone)], dtype='object'
                )
            }
        )
        df_out = duckdb_cursor.sql("""select * from df_in""").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.skipif(
        Version(pd.__version__) < Version('2.0.2'), reason="pandas < 2.0.2 does not properly convert timezones"
    )
    @pytest.mark.parametrize('unit', ['ms', 'ns', 's'])
    def test_timestamp_timezone_coverage(self, unit, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        ts_df = pd.DataFrame(
            {'ts': pd.Series(data=[pd.Timestamp(datetime.datetime(1990, 12, 21))], dtype=f'datetime64[{unit}]')}
        )
        usecond_df = pd.DataFrame(
            {'ts': pd.Series(data=[pd.Timestamp(datetime.datetime(1990, 12, 21))], dtype='datetime64[us]')}
        )

        query = """
            select
                cast(ts as timestamptz) as tstz
            from {}
        """

        duckdb_cursor.sql("set TimeZone = 'UTC'")
        utc_usecond = duckdb_cursor.sql(query.format('usecond_df')).df()
        utc_other = duckdb_cursor.sql(query.format('ts_df')).df()

        duckdb_cursor.sql("set TimeZone = 'America/Los_Angeles'")
        us_usecond = duckdb_cursor.sql(query.format('usecond_df')).df()
        us_other = duckdb_cursor.sql(query.format('ts_df')).df()

        pd.testing.assert_frame_equal(utc_usecond, utc_other)
        pd.testing.assert_frame_equal(us_usecond, us_other)
