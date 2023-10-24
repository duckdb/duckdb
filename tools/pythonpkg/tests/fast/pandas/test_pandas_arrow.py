import duckdb
import pytest
import datetime

from conftest import pandas_supports_arrow_backend

pd = pytest.importorskip("pandas", '2.0.0')
import numpy as np
from pandas.api.types import is_integer_dtype


@pytest.mark.skipif(not pandas_supports_arrow_backend(), reason="pandas does not support the 'pyarrow' backend")
class TestPandasArrow(object):
    def test_pandas_arrow(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({'a': pd.Series([5, 4, 3])}).convert_dtypes()
        con = duckdb.connect()
        res = con.sql("select * from df").fetchall()
        assert res == [(5,), (4,), (3,)]

    def test_mixed_columns(self):
        df = pd.DataFrame(
            {
                'strings': pd.Series(['abc', 'DuckDB', 'quack', 'quack']),
                'timestamps': pd.Series(
                    [
                        datetime.datetime(1990, 10, 21),
                        datetime.datetime(2023, 1, 11),
                        datetime.datetime(2001, 2, 5),
                        datetime.datetime(1990, 10, 21),
                    ]
                ),
                'objects': pd.Series([[5, 4, 3], 'test', None, {'a': 42}]),
                'integers': np.ndarray((4,), buffer=np.array([1, 2, 3, 4, 5]), offset=np.int_().itemsize, dtype=int),
            }
        )
        pyarrow_df = df.convert_dtypes(dtype_backend='pyarrow')
        con = duckdb.connect()
        with pytest.raises(
            duckdb.InvalidInputException, match='The dataframe could not be converted to a pyarrow.lib.Table'
        ):
            res = con.sql('select * from pyarrow_df').fetchall()

        numpy_df = pd.DataFrame(
            {'a': np.ndarray((2,), buffer=np.array([1, 2, 3]), offset=np.int_().itemsize, dtype=int)}
        ).convert_dtypes(dtype_backend='numpy_nullable')
        arrow_df = pd.DataFrame(
            {
                'a': pd.Series(
                    [
                        datetime.datetime(1990, 10, 21),
                        datetime.datetime(2023, 1, 11),
                        datetime.datetime(2001, 2, 5),
                        datetime.datetime(1990, 10, 21),
                    ]
                )
            }
        ).convert_dtypes(dtype_backend='pyarrow')
        python_df = pd.DataFrame({'a': pd.Series(['test', [5, 4, 3], {'a': 42}])}).convert_dtypes()

        df = pd.concat([numpy_df['a'], arrow_df['a'], python_df['a']], axis=1, keys=['numpy', 'arrow', 'python'])
        assert is_integer_dtype(df.dtypes['numpy'])
        assert isinstance(df.dtypes['arrow'], pd.ArrowDtype)
        assert isinstance(df.dtypes['python'], np.dtype('O').__class__)

        with pytest.raises(
            duckdb.InvalidInputException, match='The dataframe could not be converted to a pyarrow.lib.Table'
        ):
            res = con.sql('select * from df').fetchall()

    def test_empty_df(self):
        df = pd.DataFrame(
            {
                'string': pd.Series(data=[], dtype='string'),
                'object': pd.Series(data=[], dtype='object'),
                'Int64': pd.Series(data=[], dtype='Int64'),
                'Float64': pd.Series(data=[], dtype='Float64'),
                'bool': pd.Series(data=[], dtype='bool'),
                'datetime64[ns]': pd.Series(data=[], dtype='datetime64[ns]'),
                'datetime64[ms]': pd.Series(data=[], dtype='datetime64[ms]'),
                'datetime64[us]': pd.Series(data=[], dtype='datetime64[us]'),
                'datetime64[s]': pd.Series(data=[], dtype='datetime64[s]'),
                'category': pd.Series(data=[], dtype='category'),
                'timedelta64[ns]': pd.Series(data=[], dtype='timedelta64[ns]'),
            }
        )
        pyarrow_df = df.convert_dtypes(dtype_backend='pyarrow')

        con = duckdb.connect()
        res = con.sql('select * from pyarrow_df').fetchall()
        assert res == []

    def test_completely_null_df(self):
        df = pd.DataFrame(
            {
                'a': pd.Series(
                    data=[
                        None,
                        np.nan,
                        pd.NA,
                    ]
                )
            }
        )
        pyarrow_df = df.convert_dtypes(dtype_backend='pyarrow')

        con = duckdb.connect()
        res = con.sql('select * from pyarrow_df').fetchall()
        assert res == [(None,), (None,), (None,)]

    def test_mixed_nulls(self):
        df = pd.DataFrame(
            {
                'float': pd.Series(data=[4.123123, None, 7.23456], dtype='Float64'),
                'int64': pd.Series(data=[-234234124, 709329413, pd.NA], dtype='Int64'),
                'bool': pd.Series(data=[np.nan, True, False], dtype='boolean'),
                'string': pd.Series(data=['NULL', None, 'quack']),
                'list[str]': pd.Series(data=[['Huey', 'Dewey', 'Louie'], [None, pd.NA, np.nan, 'DuckDB'], None]),
                'datetime64': pd.Series(
                    data=[datetime.datetime(2011, 8, 16, 22, 7, 8), None, datetime.datetime(2010, 4, 26, 18, 14, 14)]
                ),
                'date': pd.Series(data=[datetime.date(2008, 5, 28), datetime.date(2013, 7, 14), None]),
            }
        )
        pyarrow_df = df.convert_dtypes(dtype_backend='pyarrow')
        con = duckdb.connect()
        res = con.sql('select * from pyarrow_df').fetchone()
        assert res == (
            4.123123,
            -234234124,
            None,
            'NULL',
            ['Huey', 'Dewey', 'Louie'],
            datetime.datetime(2011, 8, 16, 22, 7, 8),
            datetime.date(2008, 5, 28),
        )
