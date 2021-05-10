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




    # def test_q03(self,duckdb_cursor):
    #     if not can_run:
    #         return
    #     query_results = []
    #     query_results.append('47714|267010.5894|1995-03-11|0')
    #     query_results.append('22276|266351.5562|1995-01-29|0')
    #     query_results.append('32965|263768.3414|1995-02-25|0')
    #     query_results.append('21956|254541.1285|1995-02-02|0')
    #     query_results.append('1637|243512.7981|1995-02-08|0')
    #     query_results.append('10916|241320.0814|1995-03-11|0')
    #     query_results.append('30497|208566.6969|1995-02-07|0')
    #     query_results.append('450|205447.4232|1995-03-05|0')
    #     query_results.append('47204|204478.5213|1995-03-13|0')
    #     query_results.append('9696|201502.2188|1995-02-20|0')
       
    #     duckdb_conn = duckdb.connect()
    #     duckdb_conn.execute("CALL dbgen(sf=0.01);")
        
    #     duck_orders= duckdb_conn.table("orders")
    #     arrow_orders = duck_orders.arrow()
    #     orders =  duckdb_conn.from_arrow_table(arrow_orders)
        
    #     duck_customers= duckdb_conn.table("customer")
    #     arrow_customers = duck_customers.arrow()
    #     customers =  duckdb_conn.from_arrow_table(arrow_customers)
    #     customers.create("customers_arrow")

    #     duck_lineitem = duckdb_conn.table("lineitem")
    #     arrow_lineitem = duck_lineitem.arrow()
    #     lineitem =  duckdb_conn.from_arrow_table(arrow_lineitem)
    #     lineitem.create("lineitem_arrow")
    #     result = orders.query('orders_arrow','''SELECT l_orderkey,
    #                                             sum(l_extendedprice * (1 - l_discount)) AS revenue,
    #                                             o_orderdate,
    #                                             o_shippriority
    #                                             FROM
    #                                                 customers_arrow,
    #                                                 orders_arrow,
    #                                                 lineitem_arrow
    #                                             WHERE
    #                                                 c_mktsegment = 'BUILDING'
    #                                                 AND c_custkey = o_custkey
    #                                                 AND l_orderkey = o_orderkey
    #                                                 AND o_orderdate < CAST('1995-03-15' AS date)
    #                                                 AND l_shipdate > CAST('1995-03-15' AS date)
    #                                             GROUP BY
    #                                                 l_orderkey,
    #                                                 o_orderdate,
    #                                                 o_shippriority
    #                                             ORDER BY
    #                                                 revenue DESC,
    #                                                 o_orderdate
    #                                             LIMIT 10;''')
    #     for q_res in query_results:
    #         db_result = result.fetchone()
    #         cq_results = q_res.split("|")
    #         i = 0
    #         for cq_res in cq_results:
    #             assert (cq_res == str(db_result[i]))
    #             i = i+1

    def test_q04(self, duckdb_cursor):
        if not can_run:
            return
        query_results = []
        query_results.append('1-URGENT|93')
        query_results.append('2-HIGH|103')
        query_results.append('3-MEDIUM|109')
        query_results.append('4-NOT SPECIFIED|102')
        query_results.append('5-LOW|128')

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CALL dbgen(sf=0.01);")
        duck_orders= duckdb_conn.table("orders")
        arrow_orders = duck_orders.arrow()
        orders =  duckdb_conn.from_arrow_table(arrow_orders)
        result = orders.query('orders_arrow','''SELECT
                                                    o_orderpriority,
                                                    count(*) AS order_count
                                                FROM
                                                    orders
                                                WHERE
                                                    o_orderdate >= CAST('1993-07-01' AS date)
                                                    AND o_orderdate < CAST('1993-10-01' AS date)
                                                    AND EXISTS (
                                                        SELECT
                                                            *
                                                        FROM
                                                            lineitem
                                                        WHERE
                                                            l_orderkey = o_orderkey
                                                            AND l_commitdate < l_receiptdate)
                                                GROUP BY
                                                    o_orderpriority
                                                ORDER BY
                                                    o_orderpriority;
                                                ''')
        for q_res in query_results:
            db_result = result.fetchone()
            cq_results = q_res.split("|")
            i = 0
            for cq_res in cq_results:
                assert (cq_res == str(db_result[i]))
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


    # def test_q14(self,duckdb_cursor):
    #     if not can_run:
    #         return
    #     duckdb_conn = duckdb.connect()
    #     duckdb_conn.execute("CALL dbgen(sf=0.01);")
        
    #     duck_lineitem = duckdb_conn.table("lineitem")
    #     arrow_lineitem = duck_lineitem.arrow()
    #     lineitem =  duckdb_conn.from_arrow_table(arrow_lineitem)


    #     duck_part = duckdb_conn.table("part")
    #     arrow_part = duck_part.arrow()
    #     part =  duckdb_conn.from_arrow_table(arrow_part)
    #     part.create("part_arrow")

    #     result = lineitem.query('lineitem_arrow',''' SELECT 100.00 * sum(
    #                                                     CASE WHEN p_type LIKE 'PROMO%' THEN
    #                                                         l_extendedprice * (1 - l_discount)
    #                                                     ELSE
    #                                                         0
    #                                                     END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
    #                                             FROM
    #                                                 lineitem_arrow,
    #                                                 part_arrow
    #                                             WHERE
    #                                                 l_partkey = p_partkey
    #                                                 AND l_shipdate >= date '1995-09-01'
    #                                                 AND l_shipdate < CAST('1995-10-01' AS date);
    #                                             ''')
    #     assert (result.execute().fetchone()[0] == 15.4865458122840715)
    #    