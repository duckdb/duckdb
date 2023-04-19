import duckdb
import os
import pandas as pd
import pytest
from typing import Union
import pyarrow.compute as pc
import pyarrow as pa
import pyarrow.compute as pc
even_filter = (pc.bit_wise_and(pc.field("nums"), pc.scalar(1)) == pc.scalar(0))

from duckdb.typing import *

class TestPyArrowUDF(object):

    def test_basic_use(self):
        def plus_one(x : pa.lib.Table):
            import pandas as pd
            df = x.to_pandas()
            df['c0'] = df['c0'] + 1
            return pa.lib.Table.from_pandas(df)

        con = duckdb.connect()
        con.register_vectorized('plus_one', plus_one, [BIGINT], BIGINT)
        assert [(6,)] == con.sql('select plus_one(5)').fetchall()

        range_table = con.table_function('range', [5000])
        res = con.sql('select plus_one(i) from range_table tbl(i)').fetchall()
        assert len(res) == 5000

        # FIXME: this is dependent on the duckdb vector size
        # which we can get through `duckdb.__standard_vector_size__`
        res = con.sql('select i, plus_one(i) from test_vector_types(NULL::BIGINT, false) t(i), range(2000)')
        assert len(res) == 22000

    # NOTE: This only works up to duckdb.__standard_vector_size__,
    # because we process up to STANDARD_VECTOR_SIZE tuples at a time
    def test_sort_table(self):
        def sort_table(table):
            sorted_table = table.sort_by([("c0", "ascending")])
            return sorted_table

        con = duckdb.connect()
        con.register_vectorized('sort_table', sort_table, [BIGINT], BIGINT)
        res = con.sql("select 100-i as original, sort_table(original) from range(100) tbl(i)").fetchall()
        assert res[0] == (100, 1)

    def test_varargs(self):
        def variable_args(table):
	        # We return a chunked array here, but internally we convert this into a Table
            return table['c0']

        con = duckdb.connect()
        # This function takes any number of arguments, returning the first column
        con.register_vectorized('varargs', variable_args, None, BIGINT, varargs=True)
        res = con.sql("""select varargs(5, '3', '2', 1, 0.12345)""").fetchall()
        assert res == [(5,)]
    
        res = con.sql("""select varargs(42, 'test', [5,4,3])""").fetchall()
        assert res == [(42,)]

    def test_cast_varchar_to_int(self):
        def takes_string(table):
            return table
        con = duckdb.connect()
        # The return type of the function is set to BIGINT, but it takes a VARCHAR
        con.register_vectorized('pyarrow_string_to_num', takes_string, [VARCHAR], BIGINT)

        # Succesful conversion
        res = con.sql("""select pyarrow_string_to_num('5')""").fetchall()
        assert res == [(5,)]

        with pytest.raises(duckdb.ConversionException, match="""Could not convert string 'test' to INT64"""):
            res = con.sql("""select pyarrow_string_to_num('test')""").fetchall()

    def test_return_multiple_columns(self):
        def returns_two_columns(table):
            import pandas as pd
            # Return a pyarrow table consisting of two columns
            return pa.lib.Table.from_pandas(pd.DataFrame({'a': [5,4,3], 'b': ['test', 'quack', 'duckdb']}))

        con = duckdb.connect()
        # Scalar functions only return a single value per tuple
        con.register_vectorized('two_columns', returns_two_columns, [BIGINT], BIGINT)
        with pytest.raises(duckdb.InvalidInputException, match='The returned table from a pyarrow scalar udf should only contain one column, found 2'):
            res = con.sql("""select two_columns(5)""").fetchall()

    def test_return_none(self):
        def returns_none(table):
            return None

        con = duckdb.connect()
        con.register_vectorized('will_crash', returns_none, [BIGINT], BIGINT)
        with pytest.raises(duckdb.Error, match="""Invalid Error: TypeError: 'NoneType' object is not iterable"""):
            res = con.sql("""select will_crash(5)""").fetchall()

    def test_empty_result(self):
        def return_empty(table):
	        # Always returns an empty table
            return pa.lib.Table.from_arrays([[]], names=['c0'])

        con = duckdb.connect()
        con.register_vectorized('empty_result', return_empty, [BIGINT], BIGINT)
        with pytest.raises(duckdb.InvalidInputException, match='Returned pyarrow table should have 1 tuples, found 0'):
            res = con.sql("""select empty_result(5)""").fetchall()

    def test_excessive_result(self):
        def return_too_many(table):
	        # Always returns a table consisting of 5 tuples
            return pa.lib.Table.from_arrays([[5,4,3,2,1]], names=['c0'])

        con = duckdb.connect()
        con.register_vectorized('too_many_tuples', return_too_many, [BIGINT], BIGINT)
        with pytest.raises(duckdb.InvalidInputException, match='Returned pyarrow table should have 1 tuples, found 5'):
            res = con.sql("""select too_many_tuples(5)""").fetchall()

    def test_return_struct(self):
        def return_struct(table):
            con = duckdb.connect()
            return con.sql("""
                select {'a': 5, 'b': 'test', 'c': [5,3,2]}
            """).arrow()
        
        con = duckdb.connect()
        struct_type = con.struct_type({'a': BIGINT, 'b': VARCHAR, 'c': con.list_type(BIGINT)})
        con.register_vectorized('return_struct', return_struct, [BIGINT], struct_type)
        res = con.sql("""select return_struct(5)""").fetchall()
        assert res == [({'a': 5, 'b': 'test', 'c': [5, 3, 2]},)]

    def test_multiple_chunks(self):
        def return_unmodified(table):
            return table
        
        con = duckdb.connect()
        con.register_vectorized('unmodified', return_unmodified, [BIGINT], BIGINT)
        res = con.sql("""
            select unmodified(i) from range(5000) tbl(i)
        """).fetchall()

        assert len(res) == 5000
        assert res == con.sql('select * from range(5000)').fetchall()

    def test_nulls(self):
        # TODO: provide 'null_handling' option?
        pass
    
    def test_exceptions(self):
        # TODO: we likely want an enum to define how exceptions should be handled
        # - propagate:
        #    throw the exception as a duckdb exception
        # - ignore:
        #    return NULL instead
        pass

    def test_binding(self):
        # TODO: add a way to do extra binding for the UDF
        pass

