import platform
import pandas as pd
import duckdb
import datetime
import pytest


class TestTimedelta(object):
    def test_timedelta_positive(self, duckdb_cursor):
        duckdb_interval = duckdb_cursor.query(
            "SELECT '2290-01-01 23:59:00'::TIMESTAMP - '2000-01-01 23:59:00'::TIMESTAMP AS '0'"
        ).df()
        data = [datetime.timedelta(microseconds=9151574400000000)]
        df_in = pd.DataFrame({0: pd.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df", connection=duckdb_cursor).df()
        pd.testing.assert_frame_equal(df_out, duckdb_interval)

    def test_timedelta_basic(self, duckdb_cursor):
        duckdb_interval = duckdb_cursor.query(
            "SELECT '2290-08-30 23:53:40'::TIMESTAMP - '2000-02-01 01:56:00'::TIMESTAMP AS '0'"
        ).df()
        data = [datetime.timedelta(microseconds=9169797460000000)]
        df_in = pd.DataFrame({0: pd.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df", connection=duckdb_cursor).df()
        pd.testing.assert_frame_equal(df_out, duckdb_interval)

    def test_timedelta_negative(self, duckdb_cursor):
        duckdb_interval = duckdb_cursor.query(
            "SELECT '2000-01-01 23:59:00'::TIMESTAMP - '2290-01-01 23:59:00'::TIMESTAMP AS '0'"
        ).df()
        data = [datetime.timedelta(microseconds=-9151574400000000)]
        df_in = pd.DataFrame({0: pd.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df", connection=duckdb_cursor).df()
        pd.testing.assert_frame_equal(df_out, duckdb_interval)

    @pytest.mark.parametrize('days', [1, 9999])
    @pytest.mark.parametrize('seconds', [0, 60])
    @pytest.mark.parametrize(
        'microseconds',
        [
            0,
            232493,
            999_999,
        ],
    )
    @pytest.mark.parametrize('milliseconds', [0, 999])
    @pytest.mark.parametrize('minutes', [0, 60])
    @pytest.mark.parametrize('hours', [0, 24])
    @pytest.mark.parametrize('weeks', [0, 51])
    @pytest.mark.skipif(platform.system() == "Emscripten", reason="Bind parameters are broken when running on Pyodide")
    def test_timedelta_coverage(self, duckdb_cursor, days, seconds, microseconds, milliseconds, minutes, hours, weeks):
        def create_duck_interval(days, seconds, microseconds, milliseconds, minutes, hours, weeks) -> str:
            instant = f"""
                (INTERVAL {days + (weeks * 7)} DAYS +
                INTERVAL {seconds} SECONDS +
                INTERVAL {microseconds} MICROSECONDS +
                INTERVAL {milliseconds} MILLISECONDS +
                INTERVAL {minutes} MINUTE +
                INTERVAL {hours} HOURS)
            """
            return instant

        def create_python_interval(
            days, seconds, microseconds, milliseconds, minutes, hours, weeks
        ) -> datetime.timedelta:
            return datetime.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)

        duck_interval = create_duck_interval(days, seconds, microseconds, milliseconds, minutes, hours, weeks)

        query = "select '1990/02/11'::DATE - {value}, '1990/02/11'::DATE - $1"
        query = query.format(value=duck_interval)

        val = create_python_interval(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
        a, b = duckdb_cursor.execute(query, [val]).fetchone()
        assert a == b

        equality = "select {value} = $1, {value}, $1"
        equality = equality.format(value=duck_interval)
        res, a, b = duckdb_cursor.execute(equality, [val]).fetchone()
        if res != True:
            # FIXME: in some cases intervals that are identical don't compare equal.
            assert a == b
        else:
            assert res == True
