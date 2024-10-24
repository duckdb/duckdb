import duckdb
import pandas as pd
import numpy as np
import datetime
import math
from decimal import Decimal
from uuid import UUID
import pytz
import pytest
import warnings
from contextlib import suppress


def replace_with_ndarray(obj):
    if hasattr(obj, '__getitem__'):
        if isinstance(obj, dict):
            for key, value in obj.items():
                obj[key] = replace_with_ndarray(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = replace_with_ndarray(item)
        return np.array(obj)
    return obj


# we need to write our own equality function that considers nan==nan for testing purposes
def recursive_equality(o1, o2):
    import math

    if type(o1) != type(o2):
        return False
    if type(o1) == float and math.isnan(o1) and math.isnan(o2):
        return True
    if o1 is np.ma.masked and o2 is np.ma.masked:
        return True
    try:
        if len(o1) != len(o2):
            return False
        for i in range(len(o1)):
            if not recursive_equality(o1[i], o2[i]):
                return False
        return True
    except:
        return o1 == o2


# Regenerate the 'all_types' list using:

# def get_all_types():
#    conn = duckdb.connect()
#    rel = conn.sql("""
#        select * EXCLUDE
#            time_tz
#        from test_all_types()
#    """)
#    return rel.columns
# all_types = get_all_types()
# for type in all_types:
# 	print(f'\t"{type}",')
# exit()

all_types = [
    "bool",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "hugeint",
    "utinyint",
    "usmallint",
    "uint",
    "ubigint",
    "date",
    "time",
    "timestamp",
    "timestamp_s",
    "timestamp_ms",
    "timestamp_ns",
    "timestamp_tz",
    "float",
    "double",
    "dec_4_1",
    "dec_9_4",
    "dec_18_6",
    "dec38_10",
    "uuid",
    "interval",
    "varchar",
    "blob",
    "bit",
    "small_enum",
    "medium_enum",
    "large_enum",
    "int_array",
    "double_array",
    "date_array",
    "timestamp_array",
    "timestamptz_array",
    "varchar_array",
    "nested_int_array",
    "struct",
    "struct_of_arrays",
    "array_of_structs",
    "map",
    "union",
    "fixed_int_array",
    "fixed_varchar_array",
    "fixed_nested_int_array",
    "fixed_nested_varchar_array",
    "fixed_struct_array",
    "struct_of_fixed_array",
    "fixed_array_of_int_list",
    "list_of_fixed_int_array",
]


class TestAllTypes(object):
    @pytest.mark.parametrize('cur_type', all_types)
    def test_fetchall(self, cur_type):
        conn = duckdb.connect()
        conn.execute("SET TimeZone =UTC")
        # We replace these values since the extreme ranges are not supported in native-python.
        replacement_values = {
            'timestamp': "'1990-01-01 00:00:00'::TIMESTAMP",
            'timestamp_s': "'1990-01-01 00:00:00'::TIMESTAMP_S",
            'timestamp_ns': "'1990-01-01 00:00:00'::TIMESTAMP_NS",
            'timestamp_ms': "'1990-01-01 00:00:00'::TIMESTAMP_MS",
            'timestamp_tz': "'1990-01-01 00:00:00Z'::TIMESTAMPTZ",
            'date': "'1990-01-01'::DATE",
            'date_array': "[], ['1970-01-01'::DATE, NULL, '0001-01-01'::DATE, '9999-12-31'::DATE,], [NULL::DATE,]",
            'timestamp_array': "[], ['1970-01-01'::TIMESTAMP, NULL, '0001-01-01'::TIMESTAMP, '9999-12-31 23:59:59.999999'::TIMESTAMP,], [NULL::TIMESTAMP,]",
            'timestamptz_array': "[], ['1970-01-01 00:00:00Z'::TIMESTAMPTZ, NULL, '0001-01-01 00:00:00Z'::TIMESTAMPTZ, '9999-12-31 23:59:59.999999Z'::TIMESTAMPTZ,], [NULL::TIMESTAMPTZ,]",
        }
        adjusted_values = {
            'time': """CASE WHEN "time" = '24:00:00'::TIME THEN '23:59:59.999999'::TIME ELSE "time" END AS "time" """,
            'time_tz': """CASE WHEN time_tz = '24:00:00-1559'::TIMETZ THEN '23:59:59.999999-1559'::TIMETZ ELSE time_tz END AS "time_tz" """,
        }
        min_datetime = datetime.datetime.min
        min_datetime_with_utc = min_datetime.replace(tzinfo=pytz.UTC)
        max_datetime = datetime.datetime.max
        max_datetime_with_utc = max_datetime.replace(tzinfo=pytz.UTC)
        correct_answer_map = {
            'bool': [(False,), (True,), (None,)],
            'tinyint': [(-128,), (127,), (None,)],
            'smallint': [(-32768,), (32767,), (None,)],
            'int': [(-2147483648,), (2147483647,), (None,)],
            'bigint': [(-9223372036854775808,), (9223372036854775807,), (None,)],
            'hugeint': [
                (-170141183460469231731687303715884105728,),
                (170141183460469231731687303715884105727,),
                (None,),
            ],
            'utinyint': [(0,), (255,), (None,)],
            'usmallint': [(0,), (65535,), (None,)],
            'uint': [(0,), (4294967295,), (None,)],
            'ubigint': [(0,), (18446744073709551615,), (None,)],
            'time': [(datetime.time(0, 0),), (datetime.time(23, 59, 59, 999999),), (None,)],
            'float': [(-3.4028234663852886e38,), (3.4028234663852886e38,), (None,)],
            'double': [(-1.7976931348623157e308,), (1.7976931348623157e308,), (None,)],
            'dec_4_1': [(Decimal('-999.9'),), (Decimal('999.9'),), (None,)],
            'dec_9_4': [(Decimal('-99999.9999'),), (Decimal('99999.9999'),), (None,)],
            'dec_18_6': [(Decimal('-999999999999.999999'),), (Decimal('999999999999.999999'),), (None,)],
            'dec38_10': [
                (Decimal('-9999999999999999999999999999.9999999999'),),
                (Decimal('9999999999999999999999999999.9999999999'),),
                (None,),
            ],
            'uuid': [
                (UUID('00000000-0000-0000-0000-000000000000'),),
                (UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),),
                (None,),
            ],
            'varchar': [('🦆🦆🦆🦆🦆🦆',), ('goo\0se',), (None,)],
            'json': [('🦆🦆🦆🦆🦆🦆',), ('goose',), (None,)],
            'blob': [(b'thisisalongblob\x00withnullbytes',), (b'\x00\x00\x00a',), (None,)],
            'bit': [('0010001001011100010101011010111',), ('10101',), (None,)],
            'small_enum': [('DUCK_DUCK_ENUM',), ('GOOSE',), (None,)],
            'medium_enum': [('enum_0',), ('enum_299',), (None,)],
            'large_enum': [('enum_0',), ('enum_69999',), (None,)],
            'date_array': [
                (
                    [],
                    [datetime.date(1970, 1, 1), None, datetime.date.min, datetime.date.max],
                    [
                        None,
                    ],
                )
            ],
            'timestamp_array': [
                (
                    [],
                    [datetime.datetime(1970, 1, 1), None, datetime.datetime.min, datetime.datetime.max],
                    [
                        None,
                    ],
                ),
            ],
            'timestamptz_array': [
                (
                    [],
                    [
                        datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC),
                        None,
                        min_datetime_with_utc,
                        max_datetime_with_utc,
                    ],
                    [
                        None,
                    ],
                ),
            ],
            'int_array': [([],), ([42, 999, None, None, -42],), (None,)],
            'varchar_array': [([],), (['🦆🦆🦆🦆🦆🦆', 'goose', None, ''],), (None,)],
            'double_array': [([],), ([42.0, float('nan'), float('inf'), float('-inf'), None, -42.0],), (None,)],
            'nested_int_array': [
                ([],),
                ([[], [42, 999, None, None, -42], None, [], [42, 999, None, None, -42]],),
                (None,),
            ],
            'struct': [({'a': None, 'b': None},), ({'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'},), (None,)],
            'struct_of_arrays': [
                ({'a': None, 'b': None},),
                ({'a': [42, 999, None, None, -42], 'b': ['🦆🦆🦆🦆🦆🦆', 'goose', None, '']},),
                (None,),
            ],
            'array_of_structs': [([],), ([{'a': None, 'b': None}, {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}, None],), (None,)],
            'map': [
                ({},),
                ({'key1': '🦆🦆🦆🦆🦆🦆', 'key2': 'goose'},),
                (None,),
            ],
            'time_tz': [(datetime.time(0, 0),), (datetime.time(23, 59, 59, 999999),), (None,)],
            'interval': [
                (datetime.timedelta(0),),
                (datetime.timedelta(days=30969, seconds=999, microseconds=999999),),
                (None,),
            ],
            'timestamp': [(datetime.datetime(1990, 1, 1, 0, 0),)],
            'date': [(datetime.date(1990, 1, 1),)],
            'timestamp_s': [(datetime.datetime(1990, 1, 1, 0, 0),)],
            'timestamp_ns': [(datetime.datetime(1990, 1, 1, 0, 0),)],
            'timestamp_ms': [(datetime.datetime(1990, 1, 1, 0, 0),)],
            'timestamp_tz': [(datetime.datetime(1990, 1, 1, 0, 0, tzinfo=pytz.UTC),)],
            'union': [('Frank',), (5,), (None,)],
            'fixed_int_array': [((None, 2, 3),), ((4, 5, 6),), (None,)],
            'fixed_varchar_array': [(('a', None, 'c'),), (('d', 'e', 'f'),), (None,)],
            'fixed_nested_int_array': [
                (((None, 2, 3), None, (None, 2, 3)),),
                (((4, 5, 6), (None, 2, 3), (4, 5, 6)),),
                (None,),
            ],
            'fixed_nested_varchar_array': [
                ((('a', None, 'c'), None, ('a', None, 'c')),),
                ((('d', 'e', 'f'), ('a', None, 'c'), ('d', 'e', 'f')),),
                (None,),
            ],
            'fixed_struct_array': [
                (({'a': None, 'b': None}, {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}, {'a': None, 'b': None}),),
                (({'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}, {'a': None, 'b': None}, {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}),),
                (None,),
            ],
            'struct_of_fixed_array': [
                ({'a': (None, 2, 3), 'b': ('a', None, 'c')},),
                ({'a': (4, 5, 6), 'b': ('d', 'e', 'f')},),
                (None,),
            ],
            'fixed_array_of_int_list': [
                (([], [42, 999, None, None, -42], []),),
                (([42, 999, None, None, -42], [], [42, 999, None, None, -42]),),
                (None,),
            ],
            'list_of_fixed_int_array': [
                ([(None, 2, 3), (4, 5, 6), (None, 2, 3)],),
                ([(4, 5, 6), (None, 2, 3), (4, 5, 6)],),
                (None,),
            ],
        }
        if cur_type in replacement_values:
            result = conn.execute("select " + replacement_values[cur_type]).fetchall()
        elif cur_type in adjusted_values:
            result = conn.execute(f'select {adjusted_values[cur_type]} from test_all_types()').fetchall()
        else:
            result = conn.execute(f'select "{cur_type}" from test_all_types()').fetchall()
        correct_result = correct_answer_map[cur_type]
        assert recursive_equality(result, correct_result)

    def test_bytearray_with_nulls(self):
        con = duckdb.connect(database=':memory:')
        con.execute("CREATE TABLE test (content BLOB)")
        want = bytearray([1, 2, 0, 3, 4])
        con.execute("INSERT INTO test VALUES (?)", [want])

        con.execute("SELECT * from test")
        got = bytearray(con.fetchall()[0][0])
        # Don't truncate the array on the nullbyte
        assert want == bytearray(got)

    @pytest.mark.parametrize('cur_type', all_types)
    def test_fetchnumpy(self, cur_type):
        conn = duckdb.connect()

        correct_answer_map = {
            'bool': np.ma.array(
                [False, True, False],
                mask=[0, 0, 1],
            ),
            'tinyint': np.ma.array(
                [-128, 127, -1],
                mask=[0, 0, 1],
                dtype=np.int8,
            ),
            'smallint': np.ma.array(
                [-32768, 32767, -1],
                mask=[0, 0, 1],
                dtype=np.int16,
            ),
            'int': np.ma.array(
                [-2147483648, 2147483647, -1],
                mask=[0, 0, 1],
                dtype=np.int32,
            ),
            'bigint': np.ma.array(
                [-9223372036854775808, 9223372036854775807, -1],
                mask=[0, 0, 1],
                dtype=np.int64,
            ),
            'utinyint': np.ma.array(
                [0, 255, 42],
                mask=[0, 0, 1],
                dtype=np.uint8,
            ),
            'usmallint': np.ma.array(
                [0, 65535, 42],
                mask=[0, 0, 1],
                dtype=np.uint16,
            ),
            'uint': np.ma.array(
                [0, 4294967295, 42],
                mask=[0, 0, 1],
                dtype=np.uint32,
            ),
            'ubigint': np.ma.array(
                [0, 18446744073709551615, 42],
                mask=[0, 0, 1],
                dtype=np.uint64,
            ),
            'float': np.ma.array(
                [-3.4028234663852886e38, 3.4028234663852886e38, 42.0],
                mask=[0, 0, 1],
                dtype=np.float32,
            ),
            'double': np.ma.array(
                [-1.7976931348623157e308, 1.7976931348623157e308, 42.0],
                mask=[0, 0, 1],
                dtype=np.float64,
            ),
            'uuid': np.ma.array(
                [
                    UUID('00000000-0000-0000-0000-000000000000'),
                    UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),
                    UUID('00000000-0000-0000-0000-000000000042'),
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'varchar': np.ma.array(
                ['🦆🦆🦆🦆🦆🦆', 'goo\0se', "42"],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'json': np.ma.array(
                ['🦆🦆🦆🦆🦆🦆', 'goose', "42"],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'blob': np.ma.array(
                [b'thisisalongblob\x00withnullbytes', b'\x00\x00\x00a', b"42"],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'interval': np.ma.array(
                [
                    np.timedelta64(0),
                    np.timedelta64(2675722599999999000),
                    np.timedelta64(42),
                ],
                mask=[0, 0, 1],
            ),
            # For timestamp_ns, the lowest value is out-of-range for numpy,
            # such that the conversion yields "Not a Time"
            'timestamp_ns': np.ma.array(
                [
                    np.datetime64("NaT"),
                    np.datetime64(9223372036854775806, "ns"),
                    np.datetime64("1990-01-01T00:42"),
                ],
                mask=[0, 0, 1],
            ),
            # Enums don't have a numpy equivalent and yield pandas Categorical.
            'small_enum': pd.Categorical(
                ['DUCK_DUCK_ENUM', 'GOOSE', np.nan],
                ordered=True,
            ),
            'medium_enum': pd.Categorical(
                ['enum_0', 'enum_299', np.nan],
                ordered=True,
            ),
            'large_enum': pd.Categorical(
                ['enum_0', 'enum_69999', np.nan],
                ordered=True,
            ),
            # The following types don't have a numpy equivalent and yield
            # object arrays:
            'int_array': np.ma.array(
                [
                    [],
                    [42, 999, None, None, -42],
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'varchar_array': np.ma.array(
                [
                    [],
                    ['🦆🦆🦆🦆🦆🦆', 'goose', None, ''],
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'double_array': np.ma.array(
                [
                    [],
                    [42.0, float('nan'), float('inf'), float('-inf'), None, -42.0],
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'nested_int_array': np.ma.array(
                [
                    [],
                    [[], [42, 999, None, None, -42], None, [], [42, 999, None, None, -42]],
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'struct': np.ma.array(
                [
                    {'a': None, 'b': None},
                    {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'},
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'struct_of_arrays': np.ma.array(
                [
                    {'a': None, 'b': None},
                    {'a': [42, 999, None, None, -42], 'b': ['🦆🦆🦆🦆🦆🦆', 'goose', None, '']},
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'array_of_structs': np.ma.array(
                [
                    [],
                    [{'a': None, 'b': None}, {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}, None],
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'map': np.ma.array(
                [
                    {},
                    {'key1': '🦆🦆🦆🦆🦆🦆', 'key2': 'goose'},
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'time': np.ma.array(
                ['00:00:00', '24:00:00', None],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'time_tz': np.ma.array(
                ['00:00:00', '23:59:59.999999', None],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'union': np.ma.array(['Frank', 5, None], mask=[0, 0, 1], dtype=object),
        }
        correct_answer_map = replace_with_ndarray(correct_answer_map)

        # The following types don't have a numpy equivalent, and are coerced to
        # floating point types by fetchnumpy():
        # - 'uhugeint'
        # - 'hugeint'
        # - 'dec_4_1'
        # - 'dec_9_4'
        # - 'dec_18_6'
        # - 'dec38_10'

        # The following types lead to errors:
        # Conversion Error: Could not convert DATE to nanoseconds
        # - 'date'
        # Conversion Error: Date out of range in timestamp conversion
        # - 'timestamp_array'
        # - 'timestamptz_array'
        # Conversion Error: Could not convert Timestamp(US) to Timestamp(NS)
        # - 'timestamp'
        # - 'timestamp_s'
        # - 'timestamp_ms'
        # - 'timestamp_tz'
        # SystemError: <built-in function __import__> returned a result with an exception set
        # - 'date_array'

        rel = conn.table_function("test_all_types")
        if cur_type not in correct_answer_map:
            return
        result = rel.project(f'"{cur_type}"').fetchnumpy()
        result = result[cur_type]
        correct_answer = correct_answer_map[cur_type]
        if isinstance(result, pd.Categorical) or result.dtype == object:
            assert recursive_equality(list(result), list(correct_answer))
        else:
            # assert_equal compares NaN equal, but also compares masked
            # elements equal to any unmasked element
            if isinstance(result, np.ma.MaskedArray) or isinstance(correct_answer, np.ma.MaskedArray):
                assert np.all(result.mask == correct_answer.mask)
            np.testing.assert_equal(result, correct_answer)

    @pytest.mark.parametrize('cur_type', all_types)
    def test_arrow(self, cur_type):
        try:
            import pyarrow as pa
        except:
            return
        # We skip those since the extreme ranges are not supported in arrow.
        replacement_values = {'interval': "INTERVAL '2 years'"}
        # We do not round trip enum types
        enum_types = {'small_enum', 'medium_enum', 'large_enum', 'double_array'}

        # uhugeint currently not supported by arrow
        skip_types = {'uhugeint'}
        if cur_type in skip_types:
            return

        conn = duckdb.connect()
        if cur_type in replacement_values:
            arrow_table = conn.execute("select " + replacement_values[cur_type]).arrow()
        else:
            arrow_table = conn.execute(f'select "{cur_type}" from test_all_types()').arrow()
        if cur_type in enum_types:
            round_trip_arrow_table = conn.execute("select * from arrow_table").arrow()
            result_arrow = conn.execute("select * from arrow_table").fetchall()
            result_roundtrip = conn.execute("select * from round_trip_arrow_table").fetchall()
            assert recursive_equality(result_arrow, result_roundtrip)
        else:
            round_trip_arrow_table = conn.execute("select * from arrow_table").arrow()
            assert arrow_table.equals(round_trip_arrow_table, check_metadata=True)

    @pytest.mark.parametrize('cur_type', all_types)
    def test_pandas(self, cur_type):
        # We skip those since the extreme ranges are not supported in python.
        replacement_values = {
            'timestamp': "'1990-01-01 00:00:00'::TIMESTAMP",
            'timestamp_s': "'1990-01-01 00:00:00'::TIMESTAMP_S",
            'timestamp_ns': "'1990-01-01 00:00:00'::TIMESTAMP_NS",
            'timestamp_ms': "'1990-01-01 00:00:00'::TIMESTAMP_MS",
            'timestamp_tz': "'1990-01-01 00:00:00Z'::TIMESTAMPTZ",
            'date': "'1990-01-01'::DATE",
            'date_array': "[], ['1970-01-01'::DATE, NULL, '0001-01-01'::DATE, '9999-12-31'::DATE,], [NULL::DATE,]",
            'timestamp_array': "[], ['1970-01-01'::TIMESTAMP, NULL, '0001-01-01'::TIMESTAMP, '9999-12-31 23:59:59.999999'::TIMESTAMP,], [NULL::TIMESTAMP,]",
            'timestamptz_array': "[], ['1970-01-01 00:00:00Z'::TIMESTAMPTZ, NULL, '0001-01-01 00:00:00Z'::TIMESTAMPTZ, '9999-12-31 23:59:59.999999Z'::TIMESTAMPTZ,], [NULL::TIMESTAMPTZ,]",
        }

        adjusted_values = {
            'time': """CASE WHEN "time" = '24:00:00'::TIME THEN '23:59:59.999999'::TIME ELSE "time" END AS "time" """,
        }
        conn = duckdb.connect()
        # Pandas <= 2.2.3 does not convert without throwing a warning
        conn.execute("SET timezone = UTC")
        warnings.simplefilter(action='ignore', category=RuntimeWarning)
        with suppress(TypeError):
            if cur_type in replacement_values:
                dataframe = conn.execute("select " + replacement_values[cur_type]).df()
            elif cur_type in adjusted_values:
                dataframe = conn.execute(f'select {adjusted_values[cur_type]} from test_all_types()').df()
            else:
                dataframe = conn.execute(f'select "{cur_type}" from test_all_types()').df()
            print(cur_type)
            round_trip_dataframe = conn.execute("select * from dataframe").df()
            result_dataframe = conn.execute("select * from dataframe").fetchall()
            print(round_trip_dataframe)
            result_roundtrip = conn.execute("select * from round_trip_dataframe").fetchall()
            warnings.resetwarnings()
            assert recursive_equality(result_dataframe, result_roundtrip)
