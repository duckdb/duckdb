import duckdb
import pandas as pd
import numpy as np
import datetime
import math
from decimal import Decimal
from uuid import UUID

def get_all_types():
    conn = duckdb.connect()
    all_types = conn.execute("describe select * from test_all_types()").fetchall()
    types = []
    for cur_type in all_types:
        types.append(cur_type[0])
    return types

all_types = get_all_types()

# we need to write our own equality function that considers nan==nan for testing purposes
def recursive_equality(o1, o2):
    if o1 == o2:
        return True
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
        return False

class TestAllTypes(object):
    def test_fetchall(self, duckdb_cursor):
        conn = duckdb.connect()
        # We replace these values since the extreme ranges are not supported in native-python.
        replacement_values = {
            'timestamp': "'1990-01-01 00:00:00'::TIMESTAMP",
            'timestamp_s': "'1990-01-01 00:00:00'::TIMESTAMP_S",
            'timestamp_ns': "'1990-01-01 00:00:00'::TIMESTAMP_NS",
            'timestamp_ms': "'1990-01-01 00:00:00'::TIMESTAMP_MS",
            'timestamp_tz': "'1990-01-01 00:00:00'::TIMESTAMPTZ",
            'date': "'1990-01-01'::DATE",
            'date_array': "[], ['1970-01-01'::DATE, NULL, '0001-01-01'::DATE, '9999-12-31'::DATE,], [NULL::DATE,]",
            'timestamp_array': "[], ['1970-01-01'::TIMESTAMP, NULL, '0001-01-01'::TIMESTAMP, '9999-12-31 23:59:59.999999'::TIMESTAMP,], [NULL::TIMESTAMP,]",
            'timestamptz_array': "[], ['1970-01-01'::TIMESTAMPTZ, NULL, '0001-01-01'::TIMESTAMPTZ, '9999-12-31 23:59:59.999999'::TIMESTAMPTZ,], [NULL::TIMESTAMPTZ,]",
        }

        correct_answer_map = {'bool':[(False,), (True,), (None,)]
            , 'tinyint':[(-128,), (127,), (None,)], 'smallint': [(-32768,), (32767,), (None,)]
            , 'int':[(-2147483648,), (2147483647,), (None,)],'bigint':[(-9223372036854775808,), (9223372036854775807,), (None,)]
            , 'hugeint':[(-170141183460469231731687303715884105727,), (170141183460469231731687303715884105727,), (None,)]
            , 'utinyint': [(0,), (255,), (None,)], 'usmallint': [(0,), (65535,), (None,)]
            , 'uint':[(0,), (4294967295,), (None,)], 'ubigint': [(0,), (18446744073709551615,), (None,)]
            , 'time':[(datetime.time(0, 0),), (datetime.time(23, 59, 59, 999999),), (None,)]
            , 'float': [(-3.4028234663852886e+38,), (3.4028234663852886e+38,), (None,)], 'double': [(-1.7976931348623157e+308,), (1.7976931348623157e+308,), (None,)]
            , 'dec_4_1': [(Decimal('-999.9'),), (Decimal('999.9'),), (None,)], 'dec_9_4': [(Decimal('-99999.9999'),), (Decimal('99999.9999'),), (None,)]
            , 'dec_18_6': [(Decimal('-999999999999.999999'),), (Decimal('999999999999.999999'),), (None,)], 'dec38_10':[(Decimal('-9999999999999999999999999999.9999999999'),), (Decimal('9999999999999999999999999999.9999999999'),), (None,)]
            , 'uuid': [(UUID('00000000-0000-0000-0000-000000000001'),), (UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),), (None,)]
            , 'varchar': [('🦆🦆🦆🦆🦆🦆',), ('goo\0se',), (None,)], 'json': [('🦆🦆🦆🦆🦆🦆',), ('goose',), (None,)], 'blob': [(b'thisisalongblob\x00withnullbytes',), (b'\x00\x00\x00a',), (None,)]
            , 'small_enum':[('DUCK_DUCK_ENUM',), ('GOOSE',), (None,)], 'medium_enum': [('enum_0',), ('enum_299',), (None,)], 'large_enum': [('enum_0',), ('enum_69999',), (None,)]
            , 'date_array': [([], [datetime.date(1970, 1, 1), None, datetime.date.min, datetime.date.max], [None,],)]
            , 'timestamp_array': [([], [datetime.datetime(1970, 1, 1), None, datetime.datetime.min, datetime.datetime.max], [None,],),]
            , 'timestamptz_array': [([], [datetime.datetime(1970, 1, 1), None, datetime.datetime.min, datetime.datetime.max], [None,],),]
            , 'int_array': [([],), ([42, 999, None, None, -42],), (None,)], 'varchar_array': [([],), (['🦆🦆🦆🦆🦆🦆', 'goose', None, ''],), (None,)]
            , 'double_array': [([],), ([42.0, float('nan'), float('inf'), float('-inf'), None, -42.0],), (None,)]
            , 'nested_int_array': [([],), ([[], [42, 999, None, None, -42], None, [], [42, 999, None, None, -42]],), (None,)], 'struct': [({'a': None, 'b': None},), ({'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'},), (None,)]
            , 'struct_of_arrays': [({'a': None, 'b': None},), ({'a': [42, 999, None, None, -42], 'b': ['🦆🦆🦆🦆🦆🦆', 'goose', None, '']},), (None,)]
            , 'array_of_structs': [([],), ([{'a': None, 'b': None}, {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}, None],), (None,)], 'map':[({'key': [], 'value': []},), ({'key': ['key1', 'key2'], 'value': ['🦆🦆🦆🦆🦆🦆', 'goose']},), (None,)]
            , 'time_tz':[(datetime.time(0, 0),), (datetime.time(23, 59, 59, 999999),), (None,)], 'interval': [(datetime.timedelta(0),), (datetime.timedelta(days=30969, seconds=999, microseconds=999999),), (None,)]
            , 'timestamp':[(datetime.datetime(1990, 1, 1, 0, 0),)], 'date':[(datetime.date(1990, 1, 1),)], 'timestamp_s':[(datetime.datetime(1990, 1, 1, 0, 0),)]
            , 'timestamp_ns':[(datetime.datetime(1990, 1, 1, 0, 0),)], 'timestamp_ms':[(datetime.datetime(1990, 1, 1, 0, 0),)], 'timestamp_tz':[(datetime.datetime(1990, 1, 1, 0, 0),)],}

        for cur_type in all_types:
            if cur_type in replacement_values:
                result = conn.execute("select "+replacement_values[cur_type]).fetchall()
                print(cur_type, result)
            else:
                result = conn.execute("select "+cur_type+" from test_all_types()").fetchall()
            correct_result = correct_answer_map[cur_type]
            assert recursive_equality(result, correct_result)

    def test_fetchnumpy(self, duckdb_cursor):
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
                [-3.4028234663852886e+38, 3.4028234663852886e+38, 42.0],
                mask=[0, 0, 1],
                dtype=np.float32,
            ),
            'double': np.ma.array(
                [-1.7976931348623157e+308, 1.7976931348623157e+308, 42.0],
                mask=[0, 0, 1],
                dtype=np.float64,
            ),
            'uuid': np.ma.array(
                [
                    UUID('00000000-0000-0000-0000-000000000001'),
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
            'date': np.ma.array(
                [
                    np.datetime64(-5235121029329846272, "ns"),
                    np.datetime64(5235121029329846272, "ns"),
                    np.datetime64("1990-01-01T00:42"),
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
                ['DUCK_DUCK_ENUM', 'GOOSE', np.NaN],
                ordered=True,
            ),
            'medium_enum': pd.Categorical(
                ['enum_0', 'enum_299', np.NaN],
                ordered=True,
            ),
            'large_enum': pd.Categorical(
                ['enum_0', 'enum_69999', np.NaN],
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
                    {'key': [], 'value': []},
                    {'key': ['key1', 'key2'], 'value': ['🦆🦆🦆🦆🦆🦆', 'goose']},
                    None,
                ],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'time': np.ma.array(
                ['00:00:00', '23:59:59.999999', None],
                mask=[0, 0, 1],
                dtype=object,
            ),
            'time_tz': np.ma.array(
                ['00:00:00', '23:59:59.999999', None],
                mask=[0, 0, 1],
                dtype=object,
            ),
        }

        # The following types don't have a numpy equivalent, and are coerced to
        # floating point types by fetchnumpy():
        # - 'hugeint'
        # - 'dec_4_1'
        # - 'dec_9_4'
        # - 'dec_18_6'
        # - 'dec38_10'

        # The following types lead to errors:
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
        for cur_type in all_types:
            if cur_type not in correct_answer_map:
                continue

            result = rel.project(cur_type).fetchnumpy()
            result = result[cur_type]
            correct_answer = correct_answer_map[cur_type]

            if isinstance(result, pd.Categorical) or result.dtype == object:
                assert recursive_equality(list(result), list(correct_answer))
            else:
                # assert_equal compares NaN equal, but also compares masked
                # elements equal to any unmasked element
                if (isinstance(result, np.ma.MaskedArray)
                        or isinstance(correct_answer, np.ma.MaskedArray)):
                    assert np.all(result.mask == correct_answer.mask)
                np.testing.assert_equal(result, correct_answer)

    def test_arrow(self, duckdb_cursor):
        try:
            import pyarrow as pa
        except:
            return
        # We skip those since the extreme ranges are not supported in arrow.
        replacement_values = {'interval': "INTERVAL '2 years'"}
        # We do not round trip enum types
        enum_types = {'small_enum', 'medium_enum', 'large_enum', 'double_array'}
        conn = duckdb.connect()
        for cur_type in all_types:
            if cur_type in replacement_values:
                arrow_table = conn.execute("select "+replacement_values[cur_type]).arrow()
            else:
                arrow_table = conn.execute("select "+cur_type+" from test_all_types()").arrow()
            if cur_type in enum_types:
                round_trip_arrow_table = conn.execute("select * from arrow_table").arrow()
                result_arrow = conn.execute("select * from arrow_table").fetchall()
                result_roundtrip = conn.execute("select * from round_trip_arrow_table").fetchall()
                assert recursive_equality(result_arrow, result_roundtrip)
            else:
                round_trip_arrow_table = conn.execute("select * from arrow_table").arrow()
                assert arrow_table.equals(round_trip_arrow_table, check_metadata=True)

    def test_pandas(self):
        # We skip those since the extreme ranges are not supported in python.
        replacement_values = { 'timestamp': "'1990-01-01 00:00:00'::TIMESTAMP",
            'timestamp_s': "'1990-01-01 00:00:00'::TIMESTAMP_S",
            'timestamp_ns': "'1990-01-01 00:00:00'::TIMESTAMP_NS",
            'timestamp_ms': "'1990-01-01 00:00:00'::TIMESTAMP_MS",
            'timestamp_tz': "'1990-01-01 00:00:00'::TIMESTAMPTZ",
            'date': "'1990-01-01'::DATE",
            'date_array': "[], ['1970-01-01'::DATE, NULL, '0001-01-01'::DATE, '9999-12-31'::DATE,], [NULL::DATE,]",
            'timestamp_array': "[], ['1970-01-01'::TIMESTAMP, NULL, '0001-01-01'::TIMESTAMP, '9999-12-31 23:59:59.999999'::TIMESTAMP,], [NULL::TIMESTAMP,]",
            'timestamptz_array': "[], ['1970-01-01'::TIMESTAMPTZ, NULL, '0001-01-01'::TIMESTAMPTZ, '9999-12-31 23:59:59.999999'::TIMESTAMPTZ,], [NULL::TIMESTAMPTZ,]",
            }

        conn = duckdb.connect()
        for cur_type in all_types:
            if cur_type in replacement_values:
                dataframe = conn.execute("select "+replacement_values[cur_type]).df()
            else:
                dataframe = conn.execute("select "+cur_type+" from test_all_types()").df()
            print(cur_type)
            round_trip_dataframe = conn.execute("select * from dataframe").df()
            result_dataframe = conn.execute("select * from dataframe").fetchall()
            print(round_trip_dataframe)
            result_roundtrip = conn.execute("select * from round_trip_dataframe").fetchall()
            assert recursive_equality(result_dataframe, result_roundtrip)
