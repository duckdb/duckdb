import duckdb
import pytest

pa = pytest.importorskip("pyarrow")

# This causes Arrow to output an array at the very end with an offset
# the parent struct will have an offset that needs to be used when scanning the child array
MAGIC_ARRAY_SIZE = 2**17 + 1

class TestArrowOffsets(object):
    def test_struct_of_strings(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.string()), ("col2", pa.struct({"a": pa.string()}))]),
        )

        res = duckdb_cursor.sql(
            f"""
			SELECT
				col1,
				col2.a
			FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
		"""
        ).fetchall()
        assert res == [('131072', '131072')]

    def test_struct_of_list(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": range(i, i + 3)} for i in range(len(col1))]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.string()), ("col2", pa.struct({"a": pa.list_(pa.int32())}))]),
        )
        res = duckdb_cursor.sql(
            f"""
			SELECT
				col1,
				col2.a
			FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
		"""
        ).fetchall()
        assert res == [('131072', [131072, 131073, 131074])]

    def test_struct_of_list_of_list(self, duckdb_cursor):
        col1 = [i for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": [[i, i, i], [], None, [i]]} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            # thanks formatter
            schema=pa.schema([("col1", pa.int32()), ("col2", pa.struct({"a": pa.list_(pa.list_(pa.int32()))}))]),
        )
        res = duckdb_cursor.sql(
            f"""
			SELECT
				col1,
				col2.a
			FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
		"""
        ).fetchall()
        assert res == [(131072, [[131072, 131072, 131072], [], None, [131072]])]
