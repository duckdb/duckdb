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

class TestTPCHArrow(object):
     
    def test_q06(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CALL dbgen(sf=0.01);")
        duck_lineitem = duckdb_conn.table("lineitem")
        arrow_lineitem = duck_lineitem.arrow()
        lineitem =  duckdb_conn.from_arrow_table(arrow_lineitem)
        result = lineitem.filter('''l_shipdate >= CAST('1994-01-01' AS date)
            AND l_shipdate < CAST('1995-01-01' AS date)
            AND l_discount BETWEEN 0.05
            AND 0.07
            AND l_quantity < 24; ''').aggregate('sum(l_extendedprice * l_discount)')
        assert (result.execute().fetchone()[0] == 1193053.2253)


