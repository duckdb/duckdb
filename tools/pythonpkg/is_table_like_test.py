import duckdb

class TableMock:
	def __init__(self):
		pass
	def read():
		print(5)

x = duckdb.query("select 1")
x = duckdb.is_table_like(x)
print(x)
x = TableMock()
print(duckdb.is_table_like(x))