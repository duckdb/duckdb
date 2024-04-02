import duckdb
import pytest

pa = pytest.importorskip("pyarrow")
ds = pytest.importorskip("pyarrow.dataset")


class TestArrowTypes(object):
    def test_null_type(self, duckdb_cursor):
        schema = pa.schema([("data", pa.null())])
        inputs = [pa.array([None, None, None], type=pa.null())]
        arrow_table = pa.Table.from_arrays(inputs, schema=schema)
        duckdb_cursor.register("testarrow", arrow_table)
        rel = duckdb.from_arrow(arrow_table).arrow()
        # We turn it to an array of int32 nulls
        schema = pa.schema([("data", pa.int32())])
        inputs = [pa.array([None, None, None], type=pa.null())]
        arrow_table = pa.Table.from_arrays(inputs, schema=schema)

        assert rel['data'] == arrow_table['data']

    def test_invalid_struct(self, duckdb_cursor):
        empty_struct_type = pa.struct([])

        # Create an empty array with the defined struct type
        empty_array = pa.array([], type=empty_struct_type)
        arrow_table = pa.Table.from_arrays([empty_array], schema=pa.schema([("data", empty_struct_type)]))
        with pytest.raises(
            duckdb.InvalidInputException,
            match='Attempted to convert a STRUCT with no fields to DuckDB which is not supported',
        ):
            duckdb_cursor.register('invalid_struct', arrow_table)

    def test_invalid_union(self, duckdb_cursor):
        # Create a sparse union array from dense arrays
        types = pa.array([0, 1, 1], type=pa.int8())
        sparse_union_array = pa.UnionArray.from_sparse(types, [], type_codes=[])

        arrow_table = pa.Table.from_arrays([sparse_union_array], schema=pa.schema([("data", sparse_union_array.type)]))
        with pytest.raises(
            duckdb.InvalidInputException,
            match='Attempted to convert a UNION with no fields to DuckDB which is not supported',
        ):
            duckdb_cursor.register('invalid_union', arrow_table)

            res = duckdb_cursor.sql("select * from invalid_union").fetchall()
            print(res)
