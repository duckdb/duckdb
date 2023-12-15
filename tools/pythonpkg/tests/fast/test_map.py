import duckdb
import numpy
import pytest
from datetime import date, timedelta
import re
from conftest import NumpyPandas, ArrowPandas


class TestMap(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_map(self, duckdb_cursor, pandas):
        testrel = duckdb.values([1, 2])
        conn = duckdb.connect()
        conn.execute('CREATE TABLE t (a integer)')
        empty_rel = conn.table('t')

        newdf1 = testrel.map(lambda df: df['col0'].add(42).to_frame())
        newdf2 = testrel.map(lambda df: df['col0'].astype('string').to_frame())
        newdf3 = testrel.map(lambda df: df)

        # column count differs from bind
        def evil1(df):
            if len(df) == 0:
                return df['col0'].to_frame()
            else:
                return df

        # column type differs from bind
        def evil2(df):
            result = df.copy(deep=True)
            if len(result) == 0:
                result['col0'] = result['col0'].astype('double')
            return result

        # column name differs from bind
        def evil3(df):
            if len(df) == 0:
                df = df.rename(columns={"col0": "col42"})
            return df

        # does not return a df
        def evil4(df):
            return 42

        # straight up throws exception
        def evil5(df):
            raise TypeError

        def return_dataframe(df):
            return pandas.DataFrame({'A': [1]})

        def return_big_dataframe(df):
            return pandas.DataFrame({'A': [1] * 5000})

        def return_none(df):
            return None

        def return_empty_df(df):
            return pandas.DataFrame()

        with pytest.raises(duckdb.InvalidInputException, match='Expected 1 columns from UDF, got 2'):
            print(testrel.map(evil1).df())

        with pytest.raises(duckdb.InvalidInputException, match='UDF column type mismatch'):
            print(testrel.map(evil2).df())

        with pytest.raises(duckdb.InvalidInputException, match='UDF column name mismatch'):
            print(testrel.map(evil3).df())

        with pytest.raises(
            duckdb.InvalidInputException, match="Expected the UDF to return an object of type 'pandas.DataFrame'"
        ):
            print(testrel.map(evil4).df())

        with pytest.raises(duckdb.InvalidInputException):
            print(testrel.map(evil5).df())

        # not a function
        with pytest.raises(TypeError):
            print(testrel.map(42).df())

        # nothing passed to map
        with pytest.raises(TypeError):
            print(testrel.map().df())

        testrel.map(return_dataframe).df().equals(pandas.DataFrame({'A': [1]}))

        with pytest.raises(
            duckdb.InvalidInputException, match='UDF returned more than 2048 rows, which is not allowed.'
        ):
            testrel.map(return_big_dataframe).df()

        empty_rel.map(return_dataframe).df().equals(pandas.DataFrame({'A': []}))

        with pytest.raises(duckdb.InvalidInputException, match='No return value from Python function'):
            testrel.map(return_none).df()

        with pytest.raises(duckdb.InvalidInputException, match='Need a DataFrame with at least one column'):
            testrel.map(return_empty_df).df()

    def test_map_with_object_column(self, duckdb_cursor):
        def return_with_no_modification(df):
            return df

        # BLOB maps to 'object'
        # when a dataframe with 'object' column is returned, we use the content to infer the type
        # when the dataframe is empty, this results in NULL, which is not desirable
        # in this case we assume the returned type should be the same as the input type
        duckdb_cursor.values([b'1234']).map(return_with_no_modification).fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_isse_3237(self, duckdb_cursor, pandas):
        def process(rel):
            def mapper(x):
                dates = x['date'].to_numpy("datetime64[us]")
                days = x['days_to_add'].to_numpy("int")
                x["result1"] = pandas.Series(
                    [pandas.to_datetime(y[0]).date() + timedelta(days=y[1].item()) for y in zip(dates, days)],
                    dtype='datetime64[us]',
                )
                x["result2"] = pandas.Series(
                    [pandas.to_datetime(y[0]).date() + timedelta(days=-y[1].item()) for y in zip(dates, days)],
                    dtype='datetime64[us]',
                )
                return x

            rel = rel.map(mapper)
            rel = rel.project("*, datediff('day', date, result1) as one")
            rel = rel.project("*, datediff('day', date, result2) as two")
            rel = rel.project("*, IF(ABS(one) > ABS(two), one, two) as three")
            return rel

        df = pandas.DataFrame(
            {'date': pandas.Series([date(2000, 1, 1), date(2000, 1, 2)], dtype="datetime64[us]"), 'days_to_add': [1, 2]}
        )
        rel = duckdb.from_df(df)
        rel = process(rel)
        x = rel.fetchdf()
        assert x['days_to_add'].to_numpy()[0] == 1

    def test_explicit_schema(self):
        def cast_to_string(df):
            df['i'] = df['i'].astype(str)
            return df

        con = duckdb.connect()
        rel = con.sql('select i from range (10) tbl(i)')
        assert rel.types[0] == int
        mapped_rel = rel.map(cast_to_string, schema={'i': str})
        assert mapped_rel.types[0] == str

    def test_explicit_schema_returntype_mismatch(self):
        def does_nothing(df):
            return df

        con = duckdb.connect()
        rel = con.sql('select i from range(10) tbl(i)')
        # expects the mapper to return a string column
        rel = rel.map(does_nothing, schema={'i': str})
        with pytest.raises(
            duckdb.InvalidInputException, match=re.escape("UDF column type mismatch, expected [VARCHAR], got [BIGINT]")
        ):
            rel.fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_explicit_schema_name_mismatch(self, pandas):
        def renames_column(df):
            return pandas.DataFrame({'a': df['i']})

        con = duckdb.connect()
        rel = con.sql('select i from range(10) tbl(i)')
        rel = rel.map(renames_column, schema={'i': int})
        with pytest.raises(duckdb.InvalidInputException, match=re.escape('UDF column name mismatch')):
            rel.fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_explicit_schema_error(self, pandas):
        def no_op(df):
            return df

        con = duckdb.connect()
        rel = con.sql('select 42')
        with pytest.raises(
            duckdb.InvalidInputException,
            match=re.escape("Invalid Input Error: 'schema' should be given as a Dict[str, DuckDBType]"),
        ):
            rel.map(no_op, schema=[int])

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_returns_non_dataframe(self, pandas):
        def returns_series(df):
            return df.loc[:, 'i']

        con = duckdb.connect()
        rel = con.sql('select i, i as j from range(10) tbl(i)')
        with pytest.raises(
            duckdb.InvalidInputException,
            match=re.escape(
                "Expected the UDF to return an object of type 'pandas.DataFrame', found '<class 'pandas.core.series.Series'>' instead"
            ),
        ):
            rel = rel.map(returns_series)

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_explicit_schema_columncount_mismatch(self, pandas):
        def returns_subset(df):
            return pandas.DataFrame({'i': df.loc[:, 'i']})

        con = duckdb.connect()
        rel = con.sql('select i, i as j from range(10) tbl(i)')
        rel = rel.map(returns_subset, schema={'i': int, 'j': int})
        with pytest.raises(
            duckdb.InvalidInputException, match='Invalid Input Error: Expected 2 columns from UDF, got 1'
        ):
            rel.fetchall()

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_pyarrow_df(self, pandas):
        # PyArrow backed dataframes only exist on pandas >= 2.0.0
        _ = pytest.importorskip("pandas", "2.0.0")

        def basic_function(df):
            # Create a pyarrow backed dataframe
            df = pandas.DataFrame({'a': [5, 3, 2, 1, 2]}).convert_dtypes(dtype_backend='pyarrow')
            return df

        con = duckdb.connect()
        with pytest.raises(duckdb.InvalidInputException):
            rel = con.sql('select 42').map(basic_function)
