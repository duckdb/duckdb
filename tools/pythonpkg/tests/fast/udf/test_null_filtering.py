import duckdb
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
from typing import Union
import pyarrow.compute as pc

from duckdb.typing import *


class TestUDFNullFiltering(object):
    @pytest.mark.parametrize(
        'table_data',
        [
            ['12', None, '42'],
            [None, '42', None],
            ['12', None, None],
            [None, None, '42'],
            [None, None, None],
        ],
    )
    def test_null_filtering_native(self, duckdb_cursor, table_data):
        null_count = sum([1 for x in table_data if not x])
        row_count = len(table_data)

        df = pd.DataFrame({'a': table_data})
        duckdb_cursor.execute("create table tbl as select * FROM df")

        def my_native_func(x):
            my_native_func.count += 1
            return int(x)

        my_native_func.count = 0
        duckdb_cursor.create_function('other_test', my_native_func, [str], int)
        result = duckdb_cursor.sql("select other_test(a::VARCHAR) from tbl").fetchall()
        assert result == [tuple([int(x)]) if x else (None,) for x in table_data]
        assert len(result) == row_count
        # Only the non-null tuples should have been seen by the UDF
        assert my_native_func.count == row_count - null_count

    @pytest.mark.parametrize(
        'table_data',
        [
            ['12', None, '42'],
            [None, '42', None],
            ['12', None, None],
            [None, None, '42'],
            [None, None, None],
        ],
    )
    def test_null_filtering_vectorized(self, duckdb_cursor, table_data):
        null_count = sum([1 for x in table_data if not x])
        row_count = len(table_data)

        df = pd.DataFrame({'a': table_data})
        duckdb_cursor.execute("create table tbl as select * FROM df")

        def my_vectorized_func(column):
            python_array = column.to_pylist()
            my_vectorized_func.count += len(python_array)
            transformed_array = [int(value) for value in python_array]
            arrow_array = pa.array(transformed_array, type=pa.int64())
            return arrow_array

        my_vectorized_func.count = 0
        duckdb_cursor.create_function('other_test', my_vectorized_func, [str], int, type='arrow')
        result = duckdb_cursor.sql("select other_test(a::VARCHAR) from tbl").fetchall()
        assert result == [tuple([int(x)]) if x else (None,) for x in table_data]
        assert len(result) == row_count
        # Only the non-null tuples should have been seen by the UDF
        assert my_vectorized_func.count == row_count - null_count
