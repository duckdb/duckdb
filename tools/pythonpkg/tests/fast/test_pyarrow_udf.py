import duckdb
import os
import pytest
pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
from typing import Union
import pyarrow.compute as pc
import uuid
import datetime

from duckdb.typing import *

def make_annotated_function(type):
    # Create a function that returns its input
    def test_base(x):
        return x

    import types
    test_function = types.FunctionType(
        test_base.__code__,
        test_base.__globals__,
        test_base.__name__,
        test_base.__defaults__,
        test_base.__closure__
    )
    # Add annotations for the return type and 'x'
    test_function.__annotations__ = {
        'return': type,
        'x': type
    }
    return test_function

class TestPyArrowUDF(object):

    def test_basic_use(self):
        def plus_one(x):
            table = pa.lib.Table.from_arrays([x], names=['c0'])
            import pandas as pd
            df = pd.DataFrame(x.to_pandas())
            df['c0'] = df['c0'] + 1
            return pa.lib.Table.from_pandas(df)

        con = duckdb.connect()
        con.register_scalar_udf('plus_one', plus_one, [BIGINT], BIGINT, vectorized=True)
        assert [(6,)] == con.sql('select plus_one(5)').fetchall()

        range_table = con.table_function('range', [5000])
        res = con.sql('select plus_one(i) from range_table tbl(i)').fetchall()
        assert len(res) == 5000

        vector_size = duckdb.__standard_vector_size__
        res = con.sql(f'select i, plus_one(i) from test_vector_types(NULL::BIGINT, false) t(i), range({vector_size})')
        assert len(res) == (vector_size * 11)

    # NOTE: This only works up to duckdb.__standard_vector_size__,
    # because we process up to STANDARD_VECTOR_SIZE tuples at a time
    def test_sort_table(self):
        def sort_table(x):
            table = pa.lib.Table.from_arrays([x], names=['c0'])
            sorted_table = table.sort_by([("c0", "ascending")])
            return sorted_table

        con = duckdb.connect()
        con.register_scalar_udf('sort_table', sort_table, [BIGINT], BIGINT, vectorized=True)
        res = con.sql("select 100-i as original, sort_table(original) from range(100) tbl(i)").fetchall()
        assert res[0] == (100, 1)

    @pytest.mark.parametrize('test_type', [
        (TINYINT, -42),
        (SMALLINT, -512),
        (INTEGER, -131072),
        (BIGINT, -17179869184),
        (UTINYINT, 254),
        (USMALLINT, 65535),
        (UINTEGER, 4294967295),
        (UBIGINT, 18446744073709551615),
        (HUGEINT, 18446744073709551616),
        (VARCHAR, 'long_string_test'),
        (UUID, uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')),
        (FLOAT, 0.12246409803628922),
        (DOUBLE, 123142.12312416293784721232344),
        (DATE, datetime.date(2005, 3, 11)),
        (TIMESTAMP, datetime.datetime(2009, 2, 13, 11, 5, 53)),
        (TIME, datetime.time(14, 1, 12)),
        (BLOB, b'\xF6\x96\xB0\x85'),
        (INTERVAL, datetime.timedelta(days=30969, seconds=999, microseconds=999999)),
        (BOOLEAN, True),
    ])
    def test_type_coverage(self, test_type):
        type = test_type[0]
        value = test_type[1]

        test_function = make_annotated_function(type)

        con = duckdb.connect()
        con.register_scalar_udf('test', test_function, vectorized=True)

        # Single value
        res = con.execute(f"select test(?::{str(type)})", [value]).fetchall()
        assert res[0][0] == value

        # NULLs
        res = con.execute(f"select res from (select ?, test(NULL::{str(type)}) as res)", [value]).fetchall()
        assert res[0][0] == None

        # Multiple chunks
        size = duckdb.__standard_vector_size__ * 3
        res = con.execute(f"select test(x) from repeat(?::{str(type)}, {size}) as tbl(x)", [value]).fetchall()
        assert(len(res) == size)

    def test_varargs(self):
        def variable_args(*args):
            # We return a chunked array here, but internally we convert this into a Table
            if (len(args) == 0):
                raise ValueError("Expected at least one argument")
            for item in args:
                return item

        con = duckdb.connect()
        # This function takes any number of arguments, returning the first column
        con.register_scalar_udf('varargs', variable_args, None, BIGINT, vectorized=True)
        res = con.sql("""select varargs(5, '3', '2', 1, 0.12345)""").fetchall()
        assert res == [(5,)]
    
        res = con.sql("""select varargs(42, 'test', [5,4,3])""").fetchall()
        assert res == [(42,)]

    def test_cast_varchar_to_int(self):
        def takes_string(col):
            return col
        con = duckdb.connect()
        # The return type of the function is set to BIGINT, but it takes a VARCHAR
        con.register_scalar_udf('pyarrow_string_to_num', takes_string, [VARCHAR], BIGINT, vectorized=True)

        # Succesful conversion
        res = con.sql("""select pyarrow_string_to_num('5')""").fetchall()
        assert res == [(5,)]

        with pytest.raises(duckdb.ConversionException, match="""Could not convert string 'test' to INT64"""):
            res = con.sql("""select pyarrow_string_to_num('test')""").fetchall()

    def test_return_multiple_columns(self):
        def returns_two_columns(col):
            import pandas as pd
            # Return a pyarrow table consisting of two columns
            return pa.lib.Table.from_pandas(pd.DataFrame({'a': [5,4,3], 'b': ['test', 'quack', 'duckdb']}))

        con = duckdb.connect()
        # Scalar functions only return a single value per tuple
        con.register_scalar_udf('two_columns', returns_two_columns, [BIGINT], BIGINT, vectorized=True)
        with pytest.raises(duckdb.InvalidInputException, match='The returned table from a pyarrow scalar udf should only contain one column, found 2'):
            res = con.sql("""select two_columns(5)""").fetchall()

    def test_return_none(self):
        def returns_none(col):
            return None

        con = duckdb.connect()
        con.register_scalar_udf('will_crash', returns_none, [BIGINT], BIGINT, vectorized=True)
        with pytest.raises(duckdb.Error, match="""Invalid Error: TypeError: 'NoneType' object is not iterable"""):
            res = con.sql("""select will_crash(5)""").fetchall()

    def test_empty_result(self):
        def return_empty(col):
            # Always returns an empty table
            return pa.lib.Table.from_arrays([[]], names=['c0'])

        con = duckdb.connect()
        con.register_scalar_udf('empty_result', return_empty, [BIGINT], BIGINT, vectorized=True)
        with pytest.raises(duckdb.InvalidInputException, match='Returned pyarrow table should have 1 tuples, found 0'):
            res = con.sql("""select empty_result(5)""").fetchall()

    def test_excessive_result(self):
        def return_too_many(col):
            # Always returns a table consisting of 5 tuples
            return pa.lib.Table.from_arrays([[5,4,3,2,1]], names=['c0'])

        con = duckdb.connect()
        con.register_scalar_udf('too_many_tuples', return_too_many, [BIGINT], BIGINT, vectorized=True)
        with pytest.raises(duckdb.InvalidInputException, match='Returned pyarrow table should have 1 tuples, found 5'):
            res = con.sql("""select too_many_tuples(5)""").fetchall()

    def test_return_struct(self):
        def return_struct(col):
            con = duckdb.connect()
            return con.sql("""
                select {'a': 5, 'b': 'test', 'c': [5,3,2]}
            """).arrow()
        
        con = duckdb.connect()
        struct_type = con.struct_type({'a': BIGINT, 'b': VARCHAR, 'c': con.list_type(BIGINT)})
        con.register_scalar_udf('return_struct', return_struct, [BIGINT], struct_type, vectorized=True)
        res = con.sql("""select return_struct(5)""").fetchall()
        assert res == [({'a': 5, 'b': 'test', 'c': [5, 3, 2]},)]

    def test_multiple_chunks(self):
        def return_unmodified(col):
            return col
        
        con = duckdb.connect()
        con.register_scalar_udf('unmodified', return_unmodified, [BIGINT], BIGINT, vectorized=True)
        res = con.sql("""
            select unmodified(i) from range(5000) tbl(i)
        """).fetchall()

        assert len(res) == 5000
        assert res == con.sql('select * from range(5000)').fetchall()

    def test_inferred(self):
        def func(x: int) -> int:
            import pandas as pd
            df = pd.DataFrame({'c0': x})
            df['c0'] = df['c0'] ** 2
            return pa.lib.Table.from_pandas(df)
        
        con = duckdb.connect()
        con.register_scalar_udf('inferred', func, vectorized=True)
        res = con.sql('select inferred(42)').fetchall()
        assert res == [(1764,)]

    def test_nulls(self):
        def return_five(x):
            import pandas as pd
            length = len(x)
            return pa.lib.Table.from_pandas(pd.DataFrame({'a': [5 for _ in range(length)]}))
        
        con = duckdb.connect()
        con.register_scalar_udf('return_five', return_five, [BIGINT], BIGINT, null_handling='special', vectorized=True)
        res = con.sql('select return_five(NULL) from range(10)').fetchall()
        # without 'special' null handling these would all be NULL
        assert res == [(5,), (5,), (5,), (5,), (5,), (5,), (5,), (5,), (5,), (5,)]

        con = duckdb.connect()
        con.register_scalar_udf('return_five', return_five, [BIGINT], BIGINT, null_handling='default', vectorized=True)
        res = con.sql('select return_five(NULL) from range(10)').fetchall()
        # without 'special' null handling these would all be NULL
        assert res == [(None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,)]

    def test_non_callable(self):
        con = duckdb.connect()
        with pytest.raises(TypeError):
            con.register_scalar_udf('func', 5, [BIGINT], BIGINT, vectorized=True)

        class MyCallable:
            def __init__(self):
                pass

            def __call__(self, x):
                return x

        my_callable = MyCallable()
        con.register_scalar_udf('func', my_callable, [BIGINT], BIGINT, vectorized=True)
        res = con.sql('select func(5)').fetchall()
        assert res == [(5,)]

    def test_exceptions(self):
        def raises_exception(x):
            raise AttributeError("error")
        
        con = duckdb.connect()
        con.register_scalar_udf('raises', raises_exception, [BIGINT], BIGINT, vectorized=True)
        with pytest.raises(duckdb.InvalidInputException, match=' Python exception occurred while executing the UDF: AttributeError: error'):
            res = con.sql('select raises(3)').fetchall()
        
        con.unregister_udf('raises')
        con.register_scalar_udf('raises', raises_exception, [BIGINT], BIGINT, exception_handling='return_null', vectorized=True)
        res = con.sql('select raises(3) from range(5)').fetchall()
        assert res == [(None,), (None,), (None,), (None,), (None,)]

    def test_binding(self):
        # TODO: add a way to do extra binding for the UDF
        pass

