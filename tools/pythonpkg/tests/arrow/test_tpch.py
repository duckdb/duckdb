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

    def test_q01(self, duckdb_cursor):
        if not can_run:
            return
        query_results = []
        query_results.append('A|F|380456|532348211.65|505822441.4861|526165934.000839|25.5751546114546921|35785.709306937349|0.05008133906964237698|14876')
        query_results.append('N|F|8971|12384801.37|11798257.2080|12282485.056933|25.7787356321839080|35588.509683908046|0.04775862068965517241|348')
        query_results.append('N|O|742802|1041502841.45|989737518.6346|1029418531.523350|25.4549878345498783|35691.129209074398|0.04993111956409992804|29181')
        query_results.append('R|F|381449|534594445.35|507996454.4067|528524219.358903|25.5971681653469333|35874.006532680177|0.04982753992752650651|14902')

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CALL dbgen(sf=0.01);")
        duck_lineitem = duckdb_conn.table("lineitem")
        arrow_lineitem = duck_lineitem.arrow()
        lineitem =  duckdb_conn.from_arrow_table(arrow_lineitem)
        result = lineitem.query('lineitem_arrow','''SELECT
                                    l_returnflag,
                                    l_linestatus,
                                    sum(l_quantity) AS sum_qty,
                                    sum(l_extendedprice) AS sum_base_price,
                                    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                                    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                                    avg(l_quantity) AS avg_qty,
                                    avg(l_extendedprice) AS avg_price,
                                    avg(l_discount) AS avg_disc,
                                    count(*) AS count_order
                                FROM
                                    lineitem_arrow
                                WHERE
                                    l_shipdate <= CAST('1998-09-02' AS date)
                                GROUP BY
                                    l_returnflag,
                                    l_linestatus
                                ORDER BY
                                    l_returnflag,
                                    l_linestatus;''')
        for q_res in query_results:
            db_result = result.fetchone()
            cq_results = q_res.split("|")
            i = 0
            for cq_res in cq_results:
                if cq_res != str(db_result[i]):
                    #AVG does floating point division, check only first 10 values
                    assert (cq_res[0:10] == str(db_result[i])[0:10])
                i = i+1






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


