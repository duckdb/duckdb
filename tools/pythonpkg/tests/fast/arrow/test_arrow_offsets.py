import duckdb
import pytest
import datetime
import pytz

pa = pytest.importorskip("pyarrow")

# This causes Arrow to output an array at the very end with an offset
# the parent struct will have an offset that needs to be used when scanning the child array
MAGIC_ARRAY_SIZE = 2**17 + 1


def pa_time32():
    return pa.time32


def pa_time64():
    return pa.time64


def pa_timestamp():
    return pa.timestamp


def pa_duration():
    return pa.duration


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

    @pytest.mark.parametrize(
        ["constructor", "unit", "expected"],
        [
            (pa_time32(), 'ms', datetime.time(0, 2, 11, 72000)),
            (pa_time32(), 's', datetime.time(23, 59, 59)),
            (pa_time64(), 'ns', datetime.time(0, 0, 0, 131)),
            (pa_time64(), 'us', datetime.time(0, 0, 0, 131072)),
        ],
    )
    def test_struct_of_time(self, duckdb_cursor, constructor, unit, expected):
        size = MAGIC_ARRAY_SIZE
        if unit == 's':
            # FIXME: We limit the size because we don't support time values > 24 hours
            size = 86400  # The amount of seconds in a day

        col1 = [i for i in range(0, size)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", constructor(unit)), ("col2", pa.struct({"a": constructor(unit)}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {size-1}
        """
        ).fetchall()
        assert res == [(expected, expected)]

    @pytest.mark.parametrize(
        ["constructor", "unit", "expected"],
        [
            (pa_duration(), 'ms', datetime.timedelta(seconds=131, microseconds=72000)),
            (pa_duration(), 's', datetime.timedelta(days=1, seconds=44672)),
            (pa_duration(), 'ns', datetime.timedelta(microseconds=131)),
            (pa_duration(), 'us', datetime.timedelta(microseconds=131072)),
        ],
    )
    def test_struct_of_duration(self, duckdb_cursor, constructor, unit, expected):
        size = MAGIC_ARRAY_SIZE

        col1 = [i for i in range(0, size)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", constructor(unit)), ("col2", pa.struct({"a": constructor(unit)}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {size-1}
        """
        ).fetchall()
        assert res == [(expected, expected)]

    @pytest.mark.parametrize(
        ["constructor", "unit", "expected"],
        [
            (pa_timestamp(), 'ms', datetime.datetime(1970, 1, 1, 0, 2, 11, 72000, tzinfo=pytz.utc)),
            (pa_timestamp(), 's', datetime.datetime(1970, 1, 2, 12, 24, 32, 0, tzinfo=pytz.utc)),
            (pa_timestamp(), 'ns', datetime.datetime(1970, 1, 1, 0, 0, 0, 131, tzinfo=pytz.utc)),
            (pa_timestamp(), 'us', datetime.datetime(1970, 1, 1, 0, 0, 0, 131072, tzinfo=pytz.utc)),
        ],
    )
    def test_struct_of_timestamp_tz(self, duckdb_cursor, constructor, unit, expected):
        size = MAGIC_ARRAY_SIZE

        duckdb_cursor.execute("set timezone='UTC'")
        col1 = [i for i in range(0, size)]
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema(
                [("col1", constructor(unit, 'UTC')), ("col2", pa.struct({"a": constructor(unit, 'UTC')}))]
            ),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {size-1}
        """
        ).fetchall()
        assert res == [(expected, expected)]

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
