import numpy as np
import datetime
import duckdb
import pytest

pandas = pytest.importorskip("pandas")


class TestPandasNaN(object):
    def test_pandas_nan(self, duckdb_cursor):
        # create a DataFrame with some basic values
        df = pandas.DataFrame([{"col1": "val1", "col2": 1.05}, {"col1": "val3", "col2": np.NaN}])
        # create a new column (newcol1) that includes either NaN or values from col1
        df["newcol1"] = np.where(df["col1"] == "val1", np.NaN, df["col1"])
        # now create a new column with the current time
        # (FIXME: we replace the microseconds with 0 for now, because we only support milisecond resolution)
        current_time = datetime.datetime.now().replace(microsecond=0)
        df['datetest'] = current_time
        # introduce a NaT (Not a Time value)
        df.loc[0, 'datetest'] = pandas.NaT
        # now pass the DF through duckdb:

        conn = duckdb.connect(':memory:')
        conn.register('testing_null_values', df)
        # scan the DF and fetch the results normally
        results = conn.execute('select * from testing_null_values').fetchall()
        assert results[0][0] == 'val1'
        assert results[0][1] == 1.05
        assert results[0][2] == None
        assert results[0][3] == None
        assert results[1][0] == 'val3'
        assert results[1][1] == None
        assert results[1][2] == 'val3'
        assert results[1][3] == current_time

        # now fetch the results as numpy:
        result_np = conn.execute('select * from testing_null_values').fetchnumpy()
        assert result_np['col1'][0] == df['col1'][0]
        assert result_np['col1'][1] == df['col1'][1]
        assert result_np['col2'][0] == df['col2'][0]

        assert result_np['col2'].mask[1]
        assert result_np['newcol1'].mask[0]
        assert result_np['newcol1'][1] == df['newcol1'][1]

        result_df = conn.execute('select * from testing_null_values').fetchdf()
        assert pandas.isnull(result_df['datetest'][0])
        assert result_df['datetest'][1] == df['datetest'][1]
