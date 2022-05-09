import duckdb

with duckdb.connect() as con:
    res = con.execute("select 1").fetchall()
    print (res)