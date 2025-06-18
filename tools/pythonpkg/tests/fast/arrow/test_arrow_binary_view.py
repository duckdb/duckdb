import duckdb
import pyarrow as pa

tab = pa.table({"x": pa.array([b"abc"], pa.binary_view())})
print(duckdb.execute("FROM tab").fetchall())
assert duckdb.execute("FROM tab").fetchall() == [(b'abc',)]
assert duckdb.execute("FROM tab").arrow()
