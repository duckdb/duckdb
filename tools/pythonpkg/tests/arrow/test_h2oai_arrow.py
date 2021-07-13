import duckdb
import os
import sys
try:
    import pyarrow
    import pyarrow.csv

    import numpy as np
    can_run = True
except:
    can_run = False

def group_by(con,parallel):
    if (parallel):
        con.execute("PRAGMA threads=4")
    else:
        con.execute("PRAGMA threads=1")

    # q1
    con.execute("CREATE TABLE ans AS SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1")
    res = con.execute("SELECT COUNT(*), sum(v1)::varchar AS v1 FROM ans").fetchall()
    assert res == [(96, '28498857')]
    con.execute("DROP TABLE ans")

    # q2
    con.execute("CREATE TABLE ans AS SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2;")
    res = con.execute("SELECT count(*), sum(v1) AS v1 FROM ans;").fetchall()
    assert res == [(9216, 28498857)]
    con.execute("DROP TABLE ans")

    # q3
    con.execute("CREATE TABLE ans AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3;")
    res = con.execute("SELECT COUNT(*), sum(v1) AS v1, sum(v3) AS v3 FROM ans;").fetchall()
    assert res[0][0] == 95001
    assert res[0][1] == 28498857
    assert round(res[0][2],2) == 4749467.63
    con.execute("DROP TABLE ans")

    # q4
    con.execute("CREATE TABLE ans AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4;")
    res = con.execute("SELECT COUNT(*), sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchall()
    assert res[0][0] == 96
    assert round(res[0][1],2) == 287.99
    assert round(res[0][2],2) == 767.85
    assert round(res[0][3],2) == 4799.87
    con.execute("DROP TABLE ans")

    # q5
    con.execute("CREATE TABLE ans AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6;")
    res = con.execute("SELECT COUNT(*), sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchall()
    assert res[0][0] == 95001
    assert res[0][1] == 28498857
    assert res[0][2] == 75988394
    assert round(res[0][3],2) == 474969574.05
    con.execute("DROP TABLE ans")

    # q6
    con.execute("CREATE TABLE ans AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x GROUP BY id4, id5;")
    res = con.execute("SELECT COUNT(*), sum(median_v3) AS median_v3, sum(sd_v3) AS sd_v3 FROM ans").fetchall()
    assert res[0][0] == 9216
    assert round(res[0][1],2) == 460771.22
    assert round(res[0][2],2) == 266006.90
    con.execute("DROP TABLE ans")

    # q7
    con.execute("CREATE TABLE ans AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x GROUP BY id3;")
    res = con.execute("SELECT count(*), sum(range_v1_v2) AS range_v1_v2 FROM ans;").fetchall()
    assert res[0][0] == 95001
    assert res[0][1] == 379850
    con.execute("DROP TABLE ans")

    # q8
    con.execute("CREATE TABLE ans AS SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2")
    res = con.execute("SELECT count(*), sum(largest2_v3) AS largest2_v3 FROM ans").fetchall()
    assert res[0][0] == 190002
    assert round(res[0][1],2) == 18700554.78
    con.execute("DROP TABLE ans")

    # q9
    con.execute("CREATE TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4;")
    res = con.execute("SELECT count(*), sum(r2) AS r2 FROM ans").fetchall()
    assert res[0][0] == 9216
    assert round(res[0][1],2) == 9.94
    con.execute("DROP TABLE ans")

    # q10
    con.execute("CREATE TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6;")
    res = con.execute("SELECT sum(v3) AS v3, sum(count) AS count FROM ans;").fetchall()
    assert round(res[0][0],0) == 474969574
    assert res[0][1] == 10000000
    con.execute("DROP TABLE ans")

def join(con,parallel):
    if (parallel):
        con.execute("PRAGMA threads=4")
    else:
        con.execute("PRAGMA threads=1")

    # q1
    con.execute("CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1);")
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 8998860
    assert round(res[0][1],2) == 450015153.58
    assert round(res[0][2],2) == 347720187.39
    con.execute("DROP TABLE ans")

    # q2
    con.execute("CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2);")
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 8998412
    assert round(res[0][1],2) == 449954076.03
    assert round(res[0][2],2) == 449999844.94
    con.execute("DROP TABLE ans")

    # q3
    con.execute("CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2);")
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 10000000
    assert round(res[0][1],2) == 500043740.75
    assert round(res[0][2],2) == 449999844.94
    con.execute("DROP TABLE ans")

    # q4
    con.execute("CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5);")
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 8998412
    assert round(res[0][1],2) == 449954076.03
    assert round(res[0][2],2) == 449999844.94
    con.execute("DROP TABLE ans")

    # q5
    con.execute("CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3);")
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 9000000
    assert round(res[0][1],2) ==  450032091.84
    assert round(res[0][2],2) == 449860428.62
    con.execute("DROP TABLE ans")

class TestH2OAIArrow(object):
    def test_group_by(self, duckdb_cursor):
        con = duckdb.connect()
        os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/G1_1e7_1e2_5_0.csv.gz')
        arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('G1_1e7_1e2_5_0.csv.gz').to_batches(2500000))
        con.register_arrow("x", arrow_table)
        os.system('rm G1_1e7_1e2_5_0.csv.gz')

        group_by(con,True)
        group_by(con,False)

    def test_join(self,duckdb_cursor):
        os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_NA_0_0.csv.gz')
        os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e1_0_0.csv.gz')
        os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e4_0_0.csv.gz')
        os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e7_0_0.csv.gz')
        
        con = duckdb.connect()
        arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('J1_1e7_NA_0_0.csv.gz').to_batches(2500000))
        con.register_arrow("x", arrow_table)

        arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('J1_1e7_1e1_0_0.csv.gz').to_batches(2500000))
        con.register_arrow("small", arrow_table)

        arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('J1_1e7_1e4_0_0.csv.gz').to_batches(2500000))
        con.register_arrow("medium", arrow_table)

        arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('J1_1e7_1e7_0_0.csv.gz').to_batches(2500000))
        con.register_arrow("big", arrow_table)

        os.system('rm J1_1e7_NA_0_0.csv.gz')
        os.system('rm J1_1e7_1e1_0_0.csv.gz')
        os.system('rm J1_1e7_1e4_0_0.csv.gz')
        os.system('rm J1_1e7_1e7_0_0.csv.gz')

        join(con,True)
        join(con,False)
        