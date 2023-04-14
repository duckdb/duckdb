import duckdb
import pytest
import datetime
pd = pytest.importorskip("pandas", '2.0.0')
import numpy as np

class TestPandasArrow(object):
    def test_pandas_arrow(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({'a': pd.Series([5,4,3])}).convert_dtypes()
        res = duckdb.sql("select * from df").fetchall()
        assert res == [(5,),(4,),(3,)]

    def test_mixed_columns(self):
        df = pd.DataFrame({
            'strings': pd.Series([
                'abc',
                'DuckDB',
                'quack',
                'quack'
            ]),
            'timestamps': pd.Series([
                datetime.datetime(1990, 10, 21),
                datetime.datetime(2023, 1, 11),
                datetime.datetime(2001, 2, 5),
                datetime.datetime(1990, 10, 21),
            ]),
            'objects': pd.Series([
                [5,4,3],
                'test',
                None,
                {'a': 42}
            ]),
            'integers': np.ndarray((4,), buffer=np.array([1,2,3,4,5]), offset=np.int_().itemsize, dtype=int)
        })
        pyarrow_df = df.convert_dtypes(dtype_backend='pyarrow')
        with pytest.raises(duckdb.InvalidInputException, match='Invalid Input Error: The dataframe could not be converted to a pyarrow.lib.Table, due to the following python exception:'):
            res = duckdb.sql('select * from pyarrow_df').fetchall()
