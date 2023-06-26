import duckdb
import datetime
import pytz

# def test_native_tz():
import os
filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','data','tz.parquet')
con = duckdb.connect('')
res = con.execute("SET timezone='America/Denver';").fetchall()
print(res)
res = con.execute(f"select TimeRecStart as tz  from '{filename}'").fetchone()
res = con.execute(f"select TimeRecStart as tz  from '{filename}'").fetchone()
# assert res[0].tzinfo is not None
# assert res[1].tzinfo is None
# utc = pytz.timezone('UTC')
# dt_utc = res[0].astimezone(utc)
# no_tz = dt_utc.replace(tzinfo=None)
print (res[0])
# print(dt_utc)
# print (no_tz)


assert 0 == res[1]