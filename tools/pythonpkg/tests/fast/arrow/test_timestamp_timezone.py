import duckdb
import pytest
import datetime

try:
	import pyarrow as pa
	can_run = True
except:
	can_run = False

def generate_table(current_time, precision, timezone):
	timestamp_type =  pa.timestamp(precision, tz=timezone)
	schema = pa.schema([("data",timestamp_type)])
	inputs = [pa.array([current_time], type=timestamp_type)]
	return pa.Table.from_arrays(inputs, schema=schema)
timezones = ['UTC', 'BET', 'CET', 'Asia/Kathmandu']

class TestArrowTimestampsTimezone(object):
	def test_timestamp_timezone(self, duckdb_cursor):
		if not can_run:
			return
		precisions = ['us','s','ns','ms']
		current_time = datetime.datetime.now()
		con = duckdb.connect()
		con.execute("SET TimeZone = 'UTC'")
		for precision in precisions:
			arrow_table = generate_table(current_time,precision,'UTC')
			res_utc = con.from_arrow(arrow_table).execute().fetchall()
			assert res_utc[0][0] == current_time

	def test_timestamp_timezone_overflow(self, duckdb_cursor):
		if not can_run:
			return
		precisions = ['s','ms']
		current_time = 9223372036854775807
		for precision in precisions:
			with pytest.raises(Exception, match='Could not convert'):
				arrow_table = generate_table(current_time,precision,'UTC')
				res_utc = duckdb.from_arrow(arrow_table).execute().fetchall()

	def test_timestamp_tz_to_arrow(self, duckdb_cursor):
		if not can_run:
			return
precisions = ['us','s','ns','ms']
current_time = datetime.datetime.now()
con = duckdb.connect()
for precision in precisions:
	for timezone in timezones:
		print(con.execute("SET TimeZone = '"+timezone+"'").fetchone())
		print(con.execute("SELECT * FROM duckdb_settings() WHERE name = 'TimeZone';").fetchone())
		arrow_table = generate_table(current_time,precision,timezone)
		res = con.from_arrow(arrow_table).arrow()
		assert res[0].type == pa.timestamp('us', tz=timezone)
		assert res == generate_table(current_time,'us',timezone)

	# def test_arrow_table_multiple_timestamps(self, duckdb_cursor):
	# 	if not can_run:
	# 		return
	# 	precisions = ['us','s','ns','ms']
	# 	current_time = datetime.datetime.now()
	# 	for precision in precisions:
	# 		for timezone in timezones:
	# 			arrow_table = generate_table(current_time,precision,timezone)
	# 			res = duckdb.from_arrow(arrow_table).arrow()
	# 			assert res[0].type == pa.timestamp('us', tz=timezone)
	# 			assert res == generate_table(current_time,'us',timezone)
