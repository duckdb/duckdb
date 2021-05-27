import duckdb
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

# class TestArrowParallel(object):
#     def test_parallel_run(self,duckdb_cursor):
#         if not can_run:
#         	return
       # def test_lists(self, duckdb_cursor):
# duckdb_conn = duckdb.connect()
# result = duckdb_conn.execute("create table integers ( a integer)")
# result = duckdb_conn.execute("insert into integers values (1), (2), (3), (4) ")
print(duckdb.query("SELECT a from (select list_value(1) as a) as t").arrow())

