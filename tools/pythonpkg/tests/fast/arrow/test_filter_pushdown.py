import duckdb
import os
import pytest
import tempfile
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    import numpy as np
    import pandas as pd
    import re
    can_run = True
except:
    can_run = False

## DuckDB connection used in this test
duckdb_conn = duckdb.connect()

def numeric_operators(data_type, tbl_name):
        duckdb_conn.execute("CREATE TABLE " +tbl_name+ " (a "+data_type+", b "+data_type+", c "+data_type+")")
        duckdb_conn.execute("INSERT INTO  " +tbl_name+ " VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table(tbl_name)
        arrow_table = duck_tbl.arrow()
        print (arrow_table)

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a =1").fetchone()[0] == 1
        # Try >
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >1").fetchone()[0] == 2
        # Try >=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >=10").fetchone()[0] == 2
        # Try <
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <10").fetchone()[0] == 1
        # Try <=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <=10").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a=10 and b =1").fetchone()[0] == 0
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a =100 and b = 10 and c = 100").fetchone()[0] == 1

        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = 100 or b =1").fetchone()[0] == 2

        duckdb_conn.execute("EXPLAIN SELECT count(*) from testarrow where a = 100 or b =1")
        print(duckdb_conn.fetchall())


def numeric_check_or_pushdown(tbl_name):
    duck_tbl = duckdb_conn.table(tbl_name)
    arrow_table = duck_tbl.arrow()

    arrow_tbl_name = "testarrow_" + tbl_name
    duckdb_conn.register(arrow_tbl_name ,arrow_table)

    # Multiple column in the root OR node, don't push down
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a=1 OR b=2 AND (a>3 OR b<5)").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match

    # Single column in the root OR node
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a=1 OR a=10").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a=1 OR a=10.*|$", query_res[0][1])
    assert match

    # Single column + root OR node with AND
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a=1 OR (a>3 AND a<5)").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a=1 OR a>3 AND a<5.*|$", query_res[0][1])
    assert match

    # Single column multiple ORs
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a=1 OR a>3 OR a<5").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a=1 OR a>3 OR a<5.*|$", query_res[0][1])
    assert match

    # Testing not equal
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a!=1 OR a>3 OR a<2").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a!=1 OR a>3 OR a<2.*|$", query_res[0][1])
    assert match

    # Multiple OR filters connected with ANDs
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE (a<2 OR a>3) AND (a=1 OR a=4) AND (b=1 OR b<5)").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a<2 OR a>3 AND a=1|\n.*OR a=4.*\n.*b=2 OR b<5.*|$", query_res[0][1])
    assert match


def string_check_or_pushdown(tbl_name):
    duck_tbl = duckdb_conn.table(tbl_name)
    arrow_table = duck_tbl.arrow()

    arrow_tbl_name = "testarrow_varchar"
    duckdb_conn.register(arrow_tbl_name ,arrow_table)

    # Check string zonemap
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a>='1' OR a<='10'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a>=1 OR a<=10.*|$", query_res[0][1])
    assert match

    # No support for OR with is null
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a IS NULL or a='1'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match

    # No support for OR with is not null
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a IS NOT NULL OR a='1'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match

    # OR with the like operator
    query_res = duckdb_conn.execute("EXPLAIN SELECT * FROM " +arrow_tbl_name+ " WHERE a=1 OR a LIKE '10%'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match


class TestArrowFilterPushdown(object):
    def test_filter_pushdown_numeric(self,duckdb_cursor):
        if not can_run:
            return

        numeric_types = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
        'FLOAT', 'DOUBLE', 'HUGEINT']
        for data_type in numeric_types:
            tbl_name = "test_" + data_type
            numeric_operators(data_type, tbl_name)
            numeric_check_or_pushdown(tbl_name)

    def test_filter_pushdown_decimal(self,duckdb_cursor):
        if not can_run:
            return
        numeric_types = {'DECIMAL(4,1)': 'test_decimal_4_1', 'DECIMAL(9,1)': 'test_decimal_9_1',
                         'DECIMAL(18,4)': 'test_decimal_18_4','DECIMAL(30,12)': 'test_decimal_30_12'}
        for data_type in numeric_types:
            tbl_name = numeric_types[data_type]
            numeric_operators(data_type, tbl_name)
            numeric_check_or_pushdown(tbl_name)

    def test_filter_pushdown_varchar(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_varchar (a  VARCHAR, b VARCHAR, c VARCHAR)")
        duckdb_conn.execute("INSERT INTO  test_varchar VALUES ('1','1','1'),('10','10','10'),('100','10','100'),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_varchar")
        arrow_table = duck_tbl.arrow()

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='1'").fetchone()[0] == 1
        # Try >
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >'1'").fetchone()[0] == 2
        # Try >=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >='10'").fetchone()[0] == 2
        # Try <
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <'10'").fetchone()[0] == 1
        # Try <=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <='10'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a='10' and b ='1'").fetchone()[0] == 0
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='100' and b = '10' and c = '100'").fetchone()[0] == 1
        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = '100' or b ='1'").fetchone()[0] == 2

        # More complex tests for OR pushed down on string
        string_check_or_pushdown("test_varchar")


    def test_filter_pushdown_bool(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_bool (a  BOOL, b BOOL)")
        duckdb_conn.execute("INSERT INTO  test_bool VALUES (TRUE,TRUE),(TRUE,FALSE),(FALSE,TRUE),(NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_bool")
        arrow_table = duck_tbl.arrow()

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a =True").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a=True and b =True").fetchone()[0] == 1
        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = True or b =True").fetchone()[0] == 3

    def test_filter_pushdown_time(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_time (a  TIME, b TIME, c TIME)")
        duckdb_conn.execute("INSERT INTO  test_time VALUES ('00:01:00','00:01:00','00:01:00'),('00:10:00','00:10:00','00:10:00'),('01:00:00','00:10:00','01:00:00'),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_time")
        arrow_table = duck_tbl.arrow()

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='00:01:00'").fetchone()[0] == 1
        # Try >
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >'00:01:00'").fetchone()[0] == 2
        # Try >=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >='00:10:00'").fetchone()[0] == 2
        # Try <
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <'00:10:00'").fetchone()[0] == 1
        # Try <=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <='00:10:00'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a='00:10:00' and b ='00:01:00'").fetchone()[0] == 0
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='01:00:00' and b = '00:10:00' and c = '01:00:00'").fetchone()[0] == 1
        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = '01:00:00' or b ='00:01:00'").fetchone()[0] == 2

    def test_filter_pushdown_timestamp(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_timestamp (a  TIMESTAMP, b TIMESTAMP, c TIMESTAMP)")
        duckdb_conn.execute("INSERT INTO  test_timestamp VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_timestamp")
        arrow_table = duck_tbl.arrow()
        print (arrow_table)

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='2008-01-01 00:00:01'").fetchone()[0] == 1
        # Try >
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >'2008-01-01 00:00:01'").fetchone()[0] == 2
        # Try >=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >='2010-01-01 10:00:01'").fetchone()[0] == 2
        # Try <
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <'2010-01-01 10:00:01'").fetchone()[0] == 1
        # Try <=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <='2010-01-01 10:00:01'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a='2010-01-01 10:00:01' and b ='2008-01-01 00:00:01'").fetchone()[0] == 0
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='2020-03-01 10:00:01' and b = '2010-01-01 10:00:01' and c = '2020-03-01 10:00:01'").fetchone()[0] == 1
        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = '2020-03-01 10:00:01' or b ='2008-01-01 00:00:01'").fetchone()[0] == 2

    def test_filter_pushdown_timestamp_TZ(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_timestamptz (a  TIMESTAMPTZ, b TIMESTAMPTZ, c TIMESTAMPTZ)")
        duckdb_conn.execute("INSERT INTO  test_timestamptz VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_timestamptz")
        arrow_table = duck_tbl.arrow()
        print (arrow_table)

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='2008-01-01 00:00:01'").fetchone()[0] == 1
        # Try >
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >'2008-01-01 00:00:01'").fetchone()[0] == 2
        # Try >=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >='2010-01-01 10:00:01'").fetchone()[0] == 2
        # Try <
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <'2010-01-01 10:00:01'").fetchone()[0] == 1
        # Try <=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <='2010-01-01 10:00:01'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a='2010-01-01 10:00:01' and b ='2008-01-01 00:00:01'").fetchone()[0] == 0
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='2020-03-01 10:00:01' and b = '2010-01-01 10:00:01' and c = '2020-03-01 10:00:01'").fetchone()[0] == 1
        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = '2020-03-01 10:00:01' or b ='2008-01-01 00:00:01'").fetchone()[0] == 2


    def test_filter_pushdown_date(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_date (a  DATE, b DATE, c DATE)")
        duckdb_conn.execute("INSERT INTO  test_date VALUES ('2000-01-01','2000-01-01','2000-01-01'),('2000-10-01','2000-10-01','2000-10-01'),('2010-01-01','2000-10-01','2010-01-01'),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_date")
        arrow_table = duck_tbl.arrow()

        duckdb_conn.register("testarrow",arrow_table)
        # Try ==
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='2000-01-01'").fetchone()[0] == 1
        # Try >
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >'2000-01-01'").fetchone()[0] == 2
        # Try >=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a >='2000-10-01'").fetchone()[0] == 2
        # Try <
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <'2000-10-01'").fetchone()[0] == 1
        # Try <=
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a <='2000-10-01'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a='2000-10-01' and b ='2000-01-01'").fetchone()[0] == 0
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a ='2010-01-01' and b = '2000-10-01' and c = '2010-01-01'").fetchone()[0] == 1
        # Try Or
        assert duckdb_conn.execute("SELECT count(*) from testarrow where a = '2010-01-01' or b ='2000-01-01'").fetchone()[0] == 2


    def test_filter_pushdown_no_projection(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn.execute("CREATE TABLE test_int (a  INTEGER, b INTEGER, c INTEGER)")
        duckdb_conn.execute("INSERT INTO  test_int VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test_int")
        arrow_table = duck_tbl.arrow()
        duckdb_conn.register("testarrowtable",arrow_table)
        assert duckdb_conn.execute("SELECT * FROM  testarrowtable VALUES where a =1").fetchall() == [(1, 1, 1)]
        arrow_dataset = ds.dataset(arrow_table)
        duckdb_conn.register("testarrowdataset",arrow_dataset)
        assert duckdb_conn.execute("SELECT * FROM  testarrowdataset VALUES where a =1").fetchall() == [(1, 1, 1)]

    def test_filter_pushdown_2145(self,duckdb_cursor):
        if not can_run:
            return

        date1 = pd.date_range("2018-01-01", "2018-12-31", freq="B")
        df1 = pd.DataFrame(np.random.randn(date1.shape[0], 5), columns=list("ABCDE"))
        df1["date"] = date1

        date2 = pd.date_range("2019-01-01", "2019-12-31", freq="B")
        df2 = pd.DataFrame(np.random.randn(date2.shape[0], 5), columns=list("ABCDE"))
        df2["date"] = date2

        pq.write_table(pa.table(df1), "data1.parquet")
        pq.write_table(pa.table(df2), "data2.parquet")

        table = pq.ParquetDataset(["data1.parquet", "data2.parquet"]).read()

        con = duckdb.connect()
        con.register("testarrow",table)

        output_df = duckdb.arrow(table).filter("date > '2019-01-01'").df()
        expected_df = duckdb.from_parquet("data*.parquet").filter("date > '2019-01-01'").df()
        pd.testing.assert_frame_equal(expected_df, output_df)

        os.remove("data1.parquet")
        os.remove("data2.parquet")
