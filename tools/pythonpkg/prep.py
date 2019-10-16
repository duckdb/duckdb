import duckdb
con = duckdb.connect('')
print(con.cursor().execute('SELECT CAST(? AS INTEGER)', b'42').fetchall())
