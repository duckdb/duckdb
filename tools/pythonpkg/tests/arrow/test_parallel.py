import duckdb
import os
import sys
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

# class TestArrowParallel(object):
#     def test_unsigned_roundtrip(self,duckdb_cursor):
#         if not can_run:
#             return
duckdb_conn = duckdb.connect()
duckdb_conn.execute("PRAGMA threads=4")
duckdb_conn.execute("PRAGMA force_parallelism")
data = (pyarrow.array(np.random.randint(800, size=1000000), type=pyarrow.int32()))
tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(10000))
rel = duckdb.from_arrow_table(tbl)
assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000000)

#   for i in [7, 51, 99, 100, 101, 500, 1000, 2000]:
#             userdata_parquet_table2 = pyarrow.Table.from_batches(userdata_parquet_table.to_batches(i))
#             assert userdata_parquet_table.equals(userdata_parquet_table2, check_metadata=True)

#             rel_from_arrow2 = duckdb.arrow(userdata_parquet_table2).project(cols).arrow()
#             rel_from_arrow2.validate(full=True)

#             assert rel_from_arrow.equals(rel_from_arrow2, check_metadata=True)
#             assert rel_from_arrow.equals(rel_from_duckdb, check_metadata=True)
# tbl = pyarrow.Table.from_arrays([data],['a'])
# rel = duckdb.from_arrow_table(tbl)
# assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000000)

        # np.random.randint(2, size=10)
        # parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','unsigned.parquet')
        # data = 

        # tbl = pyarrow.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        # pyarrow.parquet.write_table(tbl, parquet_filename)

        # cols = 'a, b, c, d'

        # unsigned_parquet_table = pyarrow.parquet.read_table(parquet_filename)
        # unsigned_parquet_table.validate(full=True)
        # rel_from_arrow = duckdb.arrow(unsigned_parquet_table).project(cols).arrow()
        # rel_from_arrow.validate(full=True)

        # rel_from_duckdb = duckdb.from_parquet(parquet_filename).project(cols).arrow()
        # rel_from_duckdb.validate(full=True)

        # assert rel_from_arrow.equals(rel_from_duckdb, check_metadata=True)

        # con = duckdb.connect()
        # con.execute("select NULL c_null, (c % 4 = 0)::bool c_bool, (c%128)::tinyint c_tinyint, c::smallint*1000 c_smallint, c::integer*100000 c_integer, c::bigint*1000000000000 c_bigint, c::float c_float, c::double c_double, 'c_' || c::string c_string from (select case when range % 2 == 0 then range else null end as c from range(-10000, 10000)) sq")
        # arrow_result = con.fetch_arrow_table()
        # arrow_result.validate(full=True)
        # arrow_result.combine_chunks()
        # arrow_result.validate(full=True)

        # round_tripping = duckdb.from_arrow_table(arrow_result).to_arrow_table()
        # round_tripping.validate(full=True)

        # assert round_tripping.equals(arrow_result, check_metadata=True)

