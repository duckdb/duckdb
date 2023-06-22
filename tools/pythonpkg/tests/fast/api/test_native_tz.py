import duckdb

# def test_native_tz():
con = duckdb.connect()
res = con.execute("SELECT now()").fetchone()
print (res)
assert res == 0