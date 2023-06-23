import duckdb
import datetime
import pytz

# def test_native_tz():
con = duckdb.connect()
res = con.execute("SELECT a, a::TIMESTAMP FROM (SELECT now() as a) T").fetchone()
# assert res[0].tzinfo is not None
# assert res[1].tzinfo is None
# utc = pytz.timezone('UTC')
# dt_utc = res[0].astimezone(utc)
# no_tz = dt_utc.replace(tzinfo=None)
print (res[0])
# print(dt_utc)
# print (no_tz)
print (res[1])

assert 0 == res[1]