import numpy as np
import datetime
import duckdb
import pytest


def assert_nullness(items, null_indices):
    for i in range(len(items)):
        if i in null_indices:
            assert items[i] == None
        else:
            assert items[i] != None


class TestPandasNA(object):
    def test_pandas_na(self, duckdb_cursor):
        pd = pytest.importorskip('pandas', minversion='1.0.0', reason='Support for pandas.NA has not been added yet')
        # DataFrame containing a single pd.NA
        df = pd.DataFrame(pd.Series([pd.NA]))

        conn = duckdb.connect()

        res = conn.execute("select * from df").fetchall()
        assert res[0][0] == None

        # DataFrame containing multiple values, with a pd.NA mixed in
        null_index = 3
        df = pd.DataFrame(pd.Series([3, 1, 2, pd.NA, 8, 6]))
        res = conn.execute("select * from df").fetchall()
        items = [x[0] for x in [y for y in res]]
        assert_nullness(items, [null_index])

        # Test if pd.NA behaves the same as np.NaN once converted
        nan_df = pd.DataFrame(
            {
                'a': [
                    1.123,
                    5.23234,
                    np.NaN,
                    7234.0000124,
                    0.000000124,
                    0000000000000.0000001,
                    np.NaN,
                    -2342349234.00934580345,
                ]
            }
        )
        na_df = pd.DataFrame(
            {
                'a': [
                    1.123,
                    5.23234,
                    pd.NA,
                    7234.0000124,
                    0.000000124,
                    0000000000000.0000001,
                    pd.NA,
                    -2342349234.00934580345,
                ]
            }
        )
        assert str(nan_df['a'].dtype) == 'float64'
        assert str(na_df['a'].dtype) == 'object'  # pd.NA values turn the column into 'object'

        nan_result = conn.execute("select * from nan_df").df()
        na_result = conn.execute("select * from na_df").df()
        pd.testing.assert_frame_equal(nan_result, na_result)

        # Mixed with stringified pd.NA values
        na_string_df = pd.DataFrame({'a': [str(pd.NA), str(pd.NA), pd.NA, str(pd.NA), pd.NA, pd.NA, pd.NA, str(pd.NA)]})
        null_indices = [2, 4, 5, 6]
        res = conn.execute("select * from na_string_df").fetchall()
        items = [x[0] for x in [y for y in res]]
        assert_nullness(items, null_indices)
