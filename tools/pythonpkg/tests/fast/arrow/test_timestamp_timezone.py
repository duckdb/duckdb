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
		current_time = datetime.datetime(2017, 11, 28, 23, 55, 59)
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
		current_time =  datetime.datetime(2017, 11, 28, 23, 55, 59)
		con = duckdb.connect()
		for precision in precisions:
			for timezone in timezones:
				con.execute("SET TimeZone = '"+timezone+"'")
				arrow_table = generate_table(current_time,precision,timezone)
				res = con.from_arrow(arrow_table).arrow()
				assert res[0].type == pa.timestamp('us', tz=timezone)
				assert res == generate_table(current_time,'us',timezone)

	def test_timestamp_tz_with_null(self, duckdb_cursor):
		if not can_run:
			return
		con = duckdb.connect()
		con.execute("create table t (i timestamptz)")
		con.execute("insert into t values (NULL),('2021-11-15 02:30:00'::timestamptz)")
		rel = con.table('t')
		arrow_tbl = rel.arrow()
		con.register('t2',arrow_tbl)

		assert con.execute("select * from t").fetchall() == con.execute("select * from t2").fetchall() 

	def test_timestamp_stream(self, duckdb_cursor):
		if not can_run:
			return
		con = duckdb.connect()
		con.execute("create table t (i timestamptz)")
		con.execute("insert into t values (NULL),('2021-11-15 02:30:00'::timestamptz)")
		rel = con.table('t')
		arrow_tbl = rel.record_batch().read_all()
		con.register('t2',arrow_tbl)

		assert con.execute("select * from t").fetchall() == con.execute("select * from t2").fetchall() 

