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

class Test2747(object):
	# def test_fetchall(self, duckdb_cursor):
	# 	conn = duckdb.connect()	
	# 	# We skip those since the extreme ranges are not supported in python.
	# 	skip_types = {'date', 'timestamp', 'timestamp_s', 'timestamp_ns', 'timestamp_ms', 'date_tz', 'timestamp_tz'}
	# 	correct_answer_map = {'bool':[(False,), (True,), (None,)]
	# 		, 'tinyint':[(-128,), (127,), (None,)], 'smallint': [(-32768,), (32767,), (None,)]
	# 		, 'int':[(-2147483648,), (2147483647,), (None,)],'bigint':[(-9223372036854775808,), (9223372036854775807,), (None,)]
	# 		, 'hugeint':[(-170141183460469231731687303715884105727,), (170141183460469231731687303715884105727,), (None,)]
	# 		, 'utinyint': [(0,), (255,), (None,)], 'usmallint': [(0,), (65535,), (None,)]
	# 		, 'uint':[(0,), (4294967295,), (None,)], 'ubigint': [(0,), (18446744073709551615,), (None,)]
	# 		, 'time':[(datetime.time(0, 0),), (datetime.time(23, 59, 59, 999999),), (None,)]
	# 		, 'float': [(-3.4028234663852886e+38,), (3.4028234663852886e+38,), (None,)], 'double': [(-1.7976931348623157e+308,), (1.7976931348623157e+308,), (None,)]
	# 		, 'dec_4_1': [(Decimal('-999.9'),), (Decimal('999.9'),), (None,)], 'dec_9_4': [(Decimal('-99999.9999'),), (Decimal('99999.9999'),), (None,)]
	# 		, 'dec_18_3': [(Decimal('-999999999999.999999'),), (Decimal('999999999999.999999'),), (None,)], 'dec38_10':[(Decimal('-9999999999999999999999999999.9999999999'),), (Decimal('9999999999999999999999999999.9999999999'),), (None,)]
	# 		, 'uuid': [(UUID('00000000-0000-0000-0000-000000000001'),), (UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),), (None,)]
	# 		, 'varchar': [('🦆🦆🦆🦆🦆🦆',), ('goose',), (None,)], 'blob': [(b'thisisalongblob\x00withnullbytes',), (b'\\x00\\x00\\x00a',), (None,)]
	# 		, 'small_enum':[('DUCK_DUCK_ENUM',), ('GOOSE',), (None,)], 'medium_enum': [('enum_0',), ('enum_299',), (None,)], 'large_enum': [('enum_0',), ('enum_69999',), (None,)]
	# 		, 'int_array': [([],), ([42, 999, None, None, -42],), (None,)], 'varchar_array': [([],), (['🦆🦆🦆🦆🦆🦆', 'goose', None, ''],), (None,)]
	# 		, 'nested_int_array': [([],), ([[], [42, 999, None, None, -42], None, [], [42, 999, None, None, -42]],), (None,)], 'struct': [({'a': None, 'b': None},), ({'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'},), (None,)]
	# 		, 'struct_of_arrays': [({'a': None, 'b': None},), ({'a': [42, 999, None, None, -42], 'b': ['🦆🦆🦆🦆🦆🦆', 'goose', None, '']},), (None,)]
	# 		, 'array_of_structs': [([],), ([{'a': None, 'b': None}, {'a': 42, 'b': '🦆🦆🦆🦆🦆🦆'}, None],), (None,)], 'map':[({'key': [], 'value': []},), ({'key': ['key1', 'key2'], 'value': ['🦆🦆🦆🦆🦆🦆', 'goose']},), (None,)] 
	# 		, 'time_tz':[(datetime.time(0, 0),), (datetime.time(23, 59, 59, 999999),), (None,)], 'interval': [(datetime.timedelta(0),), (datetime.timedelta(days=30969, seconds=999, microseconds=999999),), (None,)]}

	# 	for cur_type in all_types:
	# 		if cur_type not in skip_types:
	# 			result = conn.execute("select "+cur_type+" from test_all_types()").fetchall()
	# 			print (result)
	# 			correct_result = correct_answer_map[cur_type]
	# 			assert result == correct_result

	def test_arrow(self, duckdb_cursor):
		try:
			import pyarrow as pa
		except:
			return

		skip_types = {'small_enum', 'medium_enum', 'large_enum', 'interval'}
		conn = duckdb.connect()	
		for cur_type in all_types:
			if cur_type not in skip_types:
				print(cur_type)
				arrow_table = conn.execute("select "+cur_type+" from test_all_types()").arrow()
				round_trip_arrow_table = conn.execute("select * from arrow_table").arrow()
				assert arrow_table.equals(round_trip_arrow_table, check_metadata=True)
