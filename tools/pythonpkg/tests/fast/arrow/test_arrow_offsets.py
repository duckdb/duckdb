import duckdb
import pytest
from pytest import mark
import datetime
import decimal
import pytz

pa = pytest.importorskip("pyarrow")

# This causes Arrow to output an array at the very end with an offset
# the parent struct will have an offset that needs to be used when scanning the child array
MAGIC_ARRAY_SIZE = 2**17 + 1


def pa_time32():
    return pa.time32


def pa_time64():
    return pa.time64


def pa_date32():
    return pa.date32


def pa_date64():
    return pa.date64


def pa_timestamp():
    return pa.timestamp


def pa_duration():
    return pa.duration


def pa_month_day_nano_interval():
    return pa.month_day_nano_interval


def month_interval(months):
    return (months, 0, 0)


def day_interval(days):
    return (0, days, 0)


def nano_interval(nanos):
    return (0, 0, nanos)


def increment_or_null(val: str, increment):
    if not val:
        return val
    return str(int(val) + increment)


def decimal_value(value, precision, scale):
    val = str(value)
    actual_width = precision - scale
    if len(val) > actual_width:
        return decimal.Decimal('9' * actual_width)
    return decimal.Decimal(val)


def expected_result(col1_null, col2_null, expected):
    col1 = None if col1_null else expected
    if col1_null or col2_null:
        col2 = None
    else:
        col2 = expected
    return [(col1, col2)]


test_nulls = lambda: mark.parametrize(
    ['col1_null', 'col2_null'], [(False, True), (True, False), (True, True), (False, False)]
)


class TestArrowOffsets(object):
    @test_nulls()
    def test_struct_of_strings(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
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
        assert res == expected_result(col1_null, col2_null, '131072')

    @test_nulls()
    def test_struct_of_bools(self, duckdb_cursor, col1_null, col2_null):
        tuples = [False for i in range(0, MAGIC_ARRAY_SIZE)]
        tuples[-1] = True

        col1 = tuples
        if col1_null:
            col1[-1] = None
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.bool_()), ("col2", pa.struct({"a": pa.bool_()}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == expected_result(col1_null, col2_null, True)

    @pytest.mark.parametrize(
        ["constructor", "expected"],
        [
            (pa_date32(), datetime.date(2328, 11, 12)),
            (pa_date64(), datetime.date(1970, 1, 1)),
        ],
    )
    @test_nulls()
    def test_struct_of_dates(self, duckdb_cursor, constructor, expected, col1_null, col2_null):
        tuples = [i for i in range(0, MAGIC_ARRAY_SIZE)]

        col1 = tuples
        if col1_null:
            col1[-1] = None
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", constructor()), ("col2", pa.struct({"a": constructor()}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == expected_result(col1_null, col2_null, expected)

    @test_nulls()
    def test_struct_of_enum(self, duckdb_cursor, col1_null, col2_null):
        enum_type = pa.dictionary(pa.int64(), pa.utf8())

        tuples = ['red' for i in range(MAGIC_ARRAY_SIZE)]
        tuples[-1] = 'green'
        if col1_null:
            tuples[-1] = None

        struct_tuples = [{"a": x} for x in tuples]
        if col2_null:
            struct_tuples[-1] = None

        arrow_table = pa.Table.from_pydict(
            {'col1': pa.array(tuples, enum_type), 'col2': pa.array(struct_tuples, pa.struct({"a": enum_type}))},
            schema=pa.schema([("col1", enum_type), ("col2", pa.struct({"a": enum_type}))]),
        )
        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == expected_result(col1_null, col2_null, 'green')

    @test_nulls()
    def test_struct_of_blobs(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
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
        assert res == expected_result(col1_null, col2_null, b'131072')

    @test_nulls()
    @pytest.mark.parametrize(
        ["constructor", "unit", "expected"],
        [
            (pa_time32(), 'ms', datetime.time(0, 2, 11, 72000)),
            (pa_time32(), 's', datetime.time(23, 59, 59)),
            (pa_time64(), 'ns', datetime.time(0, 0, 0, 131)),
            (pa_time64(), 'us', datetime.time(0, 0, 0, 131072)),
        ],
    )
    def test_struct_of_time(self, duckdb_cursor, constructor, unit, expected, col1_null, col2_null):
        size = MAGIC_ARRAY_SIZE
        if unit == 's':
            # FIXME: We limit the size because we don't support time values > 24 hours
            size = 86400  # The amount of seconds in a day

        col1 = [i for i in range(0, size)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
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
        assert res == expected_result(col1_null, col2_null, expected)

    @test_nulls()
    # NOTE: there is sadly no way to create a 'interval[months]' (tiM) type from pyarrow
    @pytest.mark.parametrize(
        ["constructor", "expected", "converter"],
        [
            (pa_month_day_nano_interval(), datetime.timedelta(days=3932160), month_interval),
            (pa_month_day_nano_interval(), datetime.timedelta(days=131072), day_interval),
            (pa_month_day_nano_interval(), datetime.timedelta(microseconds=131), nano_interval),
        ],
    )
    def test_struct_of_interval(self, duckdb_cursor, constructor, expected, converter, col1_null, col2_null):
        size = MAGIC_ARRAY_SIZE

        col1 = [converter(i) for i in range(0, size)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", constructor()), ("col2", pa.struct({"a": constructor()}))]),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {size-1}
        """
        ).fetchall()
        assert res == expected_result(col1_null, col2_null, expected)

    @test_nulls()
    @pytest.mark.parametrize(
        ["constructor", "unit", "expected"],
        [
            (pa_duration(), 'ms', datetime.timedelta(seconds=131, microseconds=72000)),
            (pa_duration(), 's', datetime.timedelta(days=1, seconds=44672)),
            (pa_duration(), 'ns', datetime.timedelta(microseconds=131)),
            (pa_duration(), 'us', datetime.timedelta(microseconds=131072)),
        ],
    )
    def test_struct_of_duration(self, duckdb_cursor, constructor, unit, expected, col1_null, col2_null):
        size = MAGIC_ARRAY_SIZE

        col1 = [i for i in range(0, size)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
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
        assert res == expected_result(col1_null, col2_null, expected)

    @test_nulls()
    @pytest.mark.parametrize(
        ["constructor", "unit", "expected"],
        [
            (pa_timestamp(), 'ms', datetime.datetime(1970, 1, 1, 0, 2, 11, 72000, tzinfo=pytz.utc)),
            (pa_timestamp(), 's', datetime.datetime(1970, 1, 2, 12, 24, 32, 0, tzinfo=pytz.utc)),
            (pa_timestamp(), 'ns', datetime.datetime(1970, 1, 1, 0, 0, 0, 131, tzinfo=pytz.utc)),
            (pa_timestamp(), 'us', datetime.datetime(1970, 1, 1, 0, 0, 0, 131072, tzinfo=pytz.utc)),
        ],
    )
    def test_struct_of_timestamp_tz(self, duckdb_cursor, constructor, unit, expected, col1_null, col2_null):
        size = MAGIC_ARRAY_SIZE

        duckdb_cursor.execute("set timezone='UTC'")
        col1 = [i for i in range(0, size)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
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
        assert res == expected_result(col1_null, col2_null, expected)

    @test_nulls()
    def test_struct_of_large_blobs(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None
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
        assert res == expected_result(col1_null, col2_null, b'131072')

    @test_nulls()
    @pytest.mark.parametrize(
        ["precision_scale", "expected"],
        [
            ((38, 37), decimal.Decimal('9.0000000000000000000000000000000000000')),
            ((38, 24), decimal.Decimal('131072.000000000000000000000000')),
            ((18, 14), decimal.Decimal('9999.00000000000000')),
            ((18, 5), decimal.Decimal('131072.00000')),
            ((9, 7), decimal.Decimal('99.0000000')),
            ((9, 3), decimal.Decimal('131072.000')),
            ((4, 2), decimal.Decimal('99.00')),
            ((4, 0), decimal.Decimal('9999')),
        ],
    )
    def test_struct_of_decimal(self, duckdb_cursor, precision_scale, expected, col1_null, col2_null):
        precision, scale = precision_scale
        col1 = [decimal_value(i, precision, scale) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": i} for i in col1]
        if col2_null:
            col2[-1] = None

        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema(
                [("col1", pa.decimal128(precision, scale)), ("col2", pa.struct({"a": pa.decimal128(precision, scale)}))]
            ),
        )

        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        assert res == expected_result(col1_null, col2_null, expected)

    @test_nulls()
    def test_struct_of_small_list(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": [i, i, i]} for i in col1]
        if col2_null:
            col2[-1] = None
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.string()), ("col2", pa.struct({"a": pa.list_(pa.string())}))]),
        )
        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        res1 = None if col1_null else '131072'
        if col2_null:
            res2 = None
        elif col1_null:
            res2 = [None, None, None]
        else:
            res2 = ['131072', '131072', '131072']
        assert res == [(res1, res2)]

    @test_nulls()
    def test_struct_of_fixed_size_list(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": [i, i, i]} for i in col1]
        if col2_null:
            col2[-1] = None
        arrow_table = pa.Table.from_pydict(
            {"col1": col1, "col2": col2},
            schema=pa.schema([("col1", pa.string()), ("col2", pa.struct({"a": pa.list_(pa.string(), 3)}))]),
        )
        res = duckdb_cursor.sql(
            f"""
            SELECT
                col1,
                col2.a
            FROM arrow_table offset {MAGIC_ARRAY_SIZE-1}
        """
        ).fetchall()
        res1 = None if col1_null else '131072'
        if col2_null:
            res2 = None
        elif col1_null:
            res2 = (None, None, None)
        else:
            res2 = ('131072', '131072', '131072')
        assert res == [(res1, res2)]

    @test_nulls()
    def test_struct_of_fixed_size_blob(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": [i, increment_or_null(i, 1), increment_or_null(i, 2)]} for i in col1]
        if col2_null:
            col2[-1] = None
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
        res1 = None if col1_null else b'131072'
        if col2_null:
            res2 = None
        elif col1_null:
            res2 = (None, None, None)
        else:
            res2 = (b'131072', b'131073', b'131074')
        assert res == [(res1, res2)]

    @test_nulls()
    def test_struct_of_list_of_blobs(self, duckdb_cursor, col1_null, col2_null):
        col1 = [str(i) for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": [i, increment_or_null(i, 1), increment_or_null(i, 2)]} for i in col1]
        if col2_null:
            col2[-1] = None
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
        res1 = None if col1_null else b'131072'
        if col2_null:
            res2 = None
        elif col1_null:
            res2 = [None, None, None]
        else:
            res2 = [b'131072', b'131073', b'131074']
        assert res == [(res1, res2)]

    @test_nulls()
    def test_struct_of_list_of_list(self, duckdb_cursor, col1_null, col2_null):
        col1 = [i for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            col1[-1] = None
        # "a" in the struct matches the value for col1
        col2 = [{"a": [[i, i, i], [], None, [i]]} for i in col1]
        if col2_null:
            col2[-1] = None
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
        res1 = None if col1_null else 131072
        if col2_null:
            res2 = None
        elif col1_null:
            res2 = [[None, None, None], [], None, [None]]
        else:
            res2 = [[131072, 131072, 131072], [], None, [131072]]
        assert res == [(res1, res2)]

    @pytest.mark.parametrize('col1_null', [True, False])
    def test_list_of_struct(self, duckdb_cursor, col1_null):
        # One single tuple containing a very big list
        tuples = [{"a": i} for i in range(0, MAGIC_ARRAY_SIZE)]
        if col1_null:
            tuples[-1] = None
        tuples = [tuples]
        arrow_table = pa.Table.from_pydict(
            {"col1": tuples},
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
        for i, x in enumerate(res[:-1]):
            assert x.__class__ == dict
            assert x['a'] == i
        if col1_null:
            assert res[-1] == None
        else:
            assert res[-1]['a'] == len(res) - 1

    @pytest.mark.parametrize(['outer_null', 'inner_null'], [(True, False), (False, True)])
    def test_list_of_list_of_struct(self, duckdb_cursor, outer_null, inner_null):
        tuples = [[[{"a": str(i), "b": None, "c": [i]}]] for i in range(MAGIC_ARRAY_SIZE)]
        if outer_null:
            tuples[-1] = None
        else:
            inner = [[{"a": 'aaaaaaaaaaaaaaa', "b": 'test', "c": [1, 2, 3]}] for _ in range(MAGIC_ARRAY_SIZE)]
            if inner_null:
                inner[-1] = None
            tuples[-1] = inner

        # MAGIC_ARRAY_SIZE tuples, all containing a single child list
        # except the last tuple, the child list of which is also MAGIC_ARRAY_SIZE in length
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
        if outer_null:
            assert res == [(None,)]
        else:
            if inner_null:
                assert res[-1][-1][-1] == None
            else:
                assert res[-1][-1][-1] == 131072

    @pytest.mark.parametrize('col1_null', [True, False])
    def test_struct_of_list(self, duckdb_cursor, col1_null):
        # All elements are of size 1
        tuples = [{"a": [str(i)]} for i in range(MAGIC_ARRAY_SIZE)]
        if col1_null:
            tuples[-1] = None
        else:
            tuples[-1] = {"a": [str(x) for x in range(MAGIC_ARRAY_SIZE)]}

        # Except the very last element, which is big

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
        if col1_null:
            assert res[0] == None
        else:
            assert res[0]['a'][-1] == '131072'
