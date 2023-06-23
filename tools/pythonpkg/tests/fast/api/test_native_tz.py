import duckdb
import datetime
import pytz

# def test_native_tz():
con = duckdb.connect()
res = con.execute("SELECT a, a::TIMESTAMP FROM (SELECT now() as a) T").fetchone()
assert res[0].tzinfo == pytz.utc
assert res[1].tzinfo is None
no_tz = res[0].replace(tzinfo=None)
print (res[0])
print (no_tz)
print (res[1])

assert no_tz == res[1]