import duckdb
import pandas as pd
import numpy as np
import datetime
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
    import math
    if o1 == o2:
        return True
    if type(o1) != type(o2):
        return False
    if type(o1) == float and math.isnan(o1) and math.isnan(o2):
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
            , 'varchar': [('🦆🦆🦆🦆🦆🦆',), ('goose',), (None,)], 'json': [('🦆🦆🦆🦆🦆🦆',), ('goose',), (None,)], 'blob': [(b'thisisalongblob\x00withnullbytes',), (b'\x00\x00\x00a',), (None,)]
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

    def test_pandas(self, duckdb_cursor):
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
            result_roundtrip = conn.execute("select * from round_trip_dataframe").fetchall()
            assert recursive_equality(result_dataframe, result_roundtrip)
