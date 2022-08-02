import duckdb
import pandas as pd

def test_pandas_tz():
	timezones = ['UTC', 'CET', 'Asia/Kathmandu']
	con = duckdb.connect()
	for timezone in timezones:
		con.execute("SET TimeZone = '"+timezone+"'")
		df = pd.DataFrame({"timestamp": [pd.Timestamp("2022-01-01 10:15", tz=timezone)]})
		duck_df = con.from_df(df).df()
		print (df['timestamp'].dtype)
		print(duck_df['timestamp'].dtype)
		print (df)
		print(duck_df)
		assert df.equals(duck_df)