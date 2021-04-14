import numpy as np
import pyarrow as pa
import time
import duckdb
import pyarrow.parquet as pq
import random
import pandas

def round_trip(csv_name, con):
	con.execute ("create temporary table t as SELECT * FROM read_csv_auto('"+csv_name+"');")
	con.execute("select * from t;")
	
	start_time = time.time()
	result = con.fetch_arrow_table()
	print(" duck-arrow-"+csv_name+" --- %s seconds ---" % (time.time() - start_time))
	
	# con.execute("select * from t;")
	# start_time = time.time()
	# result = con.fetch_df()
	# print(" duck-pandas-"+csv_name+" --- %s seconds ---" % (time.time() - start_time))

	# con.execute("select * from t;")
	# start_time = time.time()
	# result = con.fetchnumpy()
	# print(" duck-numpy-"+csv_name+" --- %s seconds ---" % (time.time() - start_time))

	con.execute("drop table t;")
	# This should do arrow -> duckdb segfaultign right now
	start_time = time.time()
	round_tripping = duckdb.from_arrow_table(result).to_arrow_table()
	print(" arrow-duck --- %s seconds ---" % (time.time() - start_time))



con = duckdb.connect()
round_trip('integers.csv',con)
round_trip('integers_null.csv',con)
round_trip('strings.csv',con)
round_trip('strings_null.csv',con)
