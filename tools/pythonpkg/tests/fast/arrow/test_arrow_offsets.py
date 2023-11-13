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

    def test_struct_of_blobs(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.binary()), ("col2", pa.struct({"a": pa.binary()}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == [(b'131072', b'131072')]

    def test_struct_of_time(self, duckdb_cursor):
        import datetime

        col1 = [i for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.time32('ms')), ("col2", pa.struct({"a": pa.time32('ms')}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == [(datetime.time(0, 2, 11, 72000), datetime.time(0, 2, 11, 72000))]

    def test_struct_of_large_blobs(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.large_binary()), ("col2", pa.struct({"a": pa.large_binary()}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == [(b'131072', b'131072')]

    def test_struct_of_small_list(self, duckdb_cursor):
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

    def test_struct_of_fixed_size_list(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": range(i, i + 3)} for i in range(len(col1))]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.string()), ("col2", pa.struct({"a": pa.list_(pa.int32(), 3)}))]),
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

    def test_struct_of_fixed_size_blob(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": [i, str(int(i) + 1), str(int(i) + 2)]} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.binary()), ("col2", pa.struct({"a": pa.list_(pa.binary(), 3)}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == [(b'131072', [b'131072', b'131073', b'131074'])]

    def test_struct_of_list_of_blobs(self, duckdb_cursor):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": [i, str(int(i) + 1), str(int(i) + 2)]} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.binary()), ("col2", pa.struct({"a": pa.list_(pa.binary())}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == [(b'131072', [b'131072', b'131073', b'131074'])]

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

    def test_list_of_struct(self, duckdb_cursor):
        # One single tuple containing a very big list
        arrow_table = pa.Table.from_pydict(
            {"col1": [[{"a": i} for i in range(0, MAGIC_ARRAY_SIZE)]]},
            schema=pa.schema([("col1", pa.list_(pa.struct({"a": pa.int32()})))]),
        )
        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1
            FROM arrow_table
        """
        ).fetchall()
        res = res[0][0]
        for i, x in enumerate(res):
            assert x.__class__ == dict
            assert x['a'] == i

    def test_list_of_list_of_struct(self, duckdb_cursor):
        tuples = [[[{"a": str(i), "b": None, "c": [i]}]] for i in range(MAGIC_ARRAY_SIZE)]
        tuples.append([[{"a": 'aaaaaaaaaaaaaaa', "b": 'test', "c": [1, 2, 3]}] for _ in range(MAGIC_ARRAY_SIZE)])

        # One single tuple containing a very big list
        arrow_table = pa.Table.from_pydict(
            {"col1": tuples},
            schema=pa.schema(
                [
                    (
                        "col1",
                        pa.list_(
                            pa.list_(pa.struct([("a", pa.string()), ("b", pa.string()), ("c", pa.list_(pa.int32()))]))
                        ),
                    )
                ]
            ),
        )
        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1
            FROM arrow_table OFFSET {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        print(res)

    def test_struct_of_list(self, duckdb_cursor):
        # All elements are of size 1
        tuples = [{"a": [str(i)]} for i in range(MAGIC_ARRAY_SIZE - 1)]

        # Except the very last element, which is big
        tuples.append({"a": [str(x) for x in range(MAGIC_ARRAY_SIZE)]})

        arrow_table = pa.Table.from_pydict(
            {"col1": tuples}, schema=pa.schema([("col1", pa.struct({"a": pa.list_(pa.string())}))])
        )
        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchone()
        assert res[0]['a'][-1] == str(MAGIC_ARRAY_SIZE - 1)
