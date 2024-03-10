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


class TestPyArrowUDF(object):
    def test_basic_use(self):
        def plus_one(x):
            table = pa.lib.Table.from_arrays([x], names=['c0'])
            import pandas as pd

            df = pd.DataFrame(x.to_pandas())
            df['c0'] = df['c0'] + 1
            return pa.lib.Table.from_pandas(df)

        con = duckdb.connect()
        con.create_function('plus_one', plus_one, [BIGINT], BIGINT, type='arrow')
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
        con.create_function('sort_table', sort_table, [BIGINT], BIGINT, type='arrow')
        res = con.sql("select 100-i as original, sort_table(original) from range(100) tbl(i)").fetchall()
        assert res[0] == (100, 1)

    def test_varargs(self):
        def variable_args(*args):
            # We return a chunked array here, but internally we convert this into a Table
            if len(args) == 0:
                raise ValueError("Expected at least one argument")
            for item in args:
                return item

        con = duckdb.connect()
        # This function takes any number of arguments, returning the first column
        con.create_function('varargs', variable_args, None, BIGINT, type='arrow')
        res = con.sql("""select varargs(5, '3', '2', 1, 0.12345)""").fetchall()
        assert res == [(5,)]

        res = con.sql("""select varargs(42, 'test', [5,4,3])""").fetchall()
        assert res == [(42,)]

    def test_cast_varchar_to_int(self):
        def takes_string(col):
            return col

        con = duckdb.connect()
        # The return type of the function is set to BIGINT, but it takes a VARCHAR
        con.create_function('pyarrow_string_to_num', takes_string, [VARCHAR], BIGINT, type='arrow')

        # Succesful conversion
        res = con.sql("""select pyarrow_string_to_num('5')""").fetchall()
        assert res == [(5,)]

        with pytest.raises(duckdb.ConversionException, match="""Could not convert string 'test' to INT64"""):
            res = con.sql("""select pyarrow_string_to_num('test')""").fetchall()

    def test_return_multiple_columns(self):
        def returns_two_columns(col):
            import pandas as pd

            # Return a pyarrow table consisting of two columns
            return pa.lib.Table.from_pandas(pd.DataFrame({'a': [5, 4, 3], 'b': ['test', 'quack', 'duckdb']}))

        con = duckdb.connect()
        # Scalar functions only return a single value per tuple
        con.create_function('two_columns', returns_two_columns, [BIGINT], BIGINT, type='arrow')
        with pytest.raises(
            duckdb.InvalidInputException,
            match='The returned table from a pyarrow scalar udf should only contain one column, found 2',
        ):
            res = con.sql("""select two_columns(5)""").fetchall()

    def test_return_none(self):
        def returns_none(col):
            return None

        con = duckdb.connect()
        con.create_function('will_crash', returns_none, [BIGINT], BIGINT, type='arrow')
        with pytest.raises(duckdb.Error, match="""Could not convert the result into an Arrow Table"""):
            res = con.sql("""select will_crash(5)""").fetchall()

    def test_empty_result(self):
        def return_empty(col):
            # Always returns an empty table
            return pa.lib.Table.from_arrays([[]], names=['c0'])

        con = duckdb.connect()
        con.create_function('empty_result', return_empty, [BIGINT], BIGINT, type='arrow')
        with pytest.raises(duckdb.InvalidInputException, match='Returned pyarrow table should have 1 tuples, found 0'):
            res = con.sql("""select empty_result(5)""").fetchall()

    def test_excessive_result(self):
        def return_too_many(col):
            # Always returns a table consisting of 5 tuples
            return pa.lib.Table.from_arrays([[5, 4, 3, 2, 1]], names=['c0'])

        con = duckdb.connect()
        con.create_function('too_many_tuples', return_too_many, [BIGINT], BIGINT, type='arrow')
        with pytest.raises(duckdb.InvalidInputException, match='Returned pyarrow table should have 1 tuples, found 5'):
            res = con.sql("""select too_many_tuples(5)""").fetchall()

    def test_arrow_side_effects(self, duckdb_cursor):
        import random as r

        def random_arrow(x):
            if not hasattr(random_arrow, 'data'):
                random_arrow.data = 0

            input = x.to_pylist()
            val = random_arrow.data
            output = [val + i for i in range(len(input))]
            random_arrow.data += len(input)
            return output

        duckdb_cursor.create_function(
            "random_arrow",
            random_arrow,
            [VARCHAR],
            INTEGER,
            side_effects=True,
            type="arrow",
        )
        res = duckdb_cursor.query("SELECT random_arrow('') FROM range(10)").fetchall()
        assert res == [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)]

    def test_return_struct(self):
        def return_struct(col):
            con = duckdb.connect()
            return con.sql(
                """
                select {'a': 5, 'b': 'test', 'c': [5,3,2]}
            """
            ).arrow()

        con = duckdb.connect()
        struct_type = con.struct_type({'a': BIGINT, 'b': VARCHAR, 'c': con.list_type(BIGINT)})
        con.create_function('return_struct', return_struct, [BIGINT], struct_type, type='arrow')
        res = con.sql("""select return_struct(5)""").fetchall()
        assert res == [({'a': 5, 'b': 'test', 'c': [5, 3, 2]},)]

    def test_multiple_chunks(self):
        def return_unmodified(col):
            return col

        con = duckdb.connect()
        con.create_function('unmodified', return_unmodified, [BIGINT], BIGINT, type='arrow')
        res = con.sql(
            """
            select unmodified(i) from range(5000) tbl(i)
        """
        ).fetchall()

        assert len(res) == 5000
        assert res == con.sql('select * from range(5000)').fetchall()

    def test_inferred(self):
        def func(x: int) -> int:
            import pandas as pd

            df = pd.DataFrame({'c0': x})
            df['c0'] = df['c0'] ** 2
            return pa.lib.Table.from_pandas(df)

        con = duckdb.connect()
        con.create_function('inferred', func, type='arrow')
        res = con.sql('select inferred(42)').fetchall()
        assert res == [(1764,)]

    def test_nulls(self):
        def return_five(x):
            import pandas as pd

            length = len(x)
            return pa.lib.Table.from_pandas(pd.DataFrame({'a': [5 for _ in range(length)]}))

        con = duckdb.connect()
        con.create_function('return_five', return_five, [BIGINT], BIGINT, null_handling='special', type='arrow')
        res = con.sql('select return_five(NULL) from range(10)').fetchall()
        # without 'special' null handling these would all be NULL
        assert res == [(5,), (5,), (5,), (5,), (5,), (5,), (5,), (5,), (5,), (5,)]

        con = duckdb.connect()
        con.create_function('return_five', return_five, [BIGINT], BIGINT, null_handling='default', type='arrow')
        res = con.sql('select return_five(NULL) from range(10)').fetchall()
        # Because we didn't specify 'special' null handling, these are all NULL
        assert res == [(None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,)]
