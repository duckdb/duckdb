import duckdb
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False


def check_result(result,answers):
    for q_res in answers:
        db_result = result.fetchone()
        cq_results = q_res.split("|")
        i = 0
        for cq_res in cq_results:
            if cq_res != str(db_result[i]):
                if cq_res == '' and str(db_result[i]) == 'None':
                    continue
                #AVG does floating point division, check only first 6 values
                if (float(cq_res[0:6]) != float(str(db_result[i])[0:6])):
                    return False
            i = i+1
    return True

class TestTPCHArrow(object):

    def test_tpch_arrow(self,duckdb_cursor):
        if not can_run:
            return

        tpch_tables = ['part', 'partsupp', 'supplier', 'customer', 'lineitem', 'orders', 'nation', 'region']
        arrow_tables = []

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CALL dbgen(sf=0.01);")

        for tpch_table in tpch_tables:
            duck_tbl = duckdb_conn.table(tpch_table)
            arrow_tables.append(duck_tbl.arrow())
            duck_arrow_table = duckdb_conn.from_arrow_table(arrow_tables[-1])
            duckdb_conn.execute("DROP TABLE "+tpch_table)
            duck_arrow_table.create(tpch_table)

        for i in range (1,23):
            query = duckdb_conn.execute("select query from tpch_queries() where query_nr="+str(i)).fetchone()[0]
            answers = duckdb_conn.execute("select answer from tpch_answers() where scale_factor = 0.01 and query_nr="+str(i)).fetchone()[1:]
            result = duckdb_conn.execute(query)
            assert(check_result(result,answers))
            print ("Query " + str(i) + " works")

    def test_tpch_arrow_batch(self,duckdb_cursor):
        if not can_run:
            return

        tpch_tables = ['part', 'partsupp', 'supplier', 'customer', 'lineitem', 'orders', 'nation', 'region']
        arrow_tables = []

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CALL dbgen(sf=0.01);")

        for tpch_table in tpch_tables:
            duck_tbl = duckdb_conn.table(tpch_table)
            arrow_tables.append(pyarrow.Table.from_batches(duck_tbl.arrow().to_batches(10)))
            duck_arrow_table = duckdb_conn.from_arrow_table(arrow_tables[-1])
            duckdb_conn.execute("DROP TABLE "+tpch_table)
            duck_arrow_table.create(tpch_table)

        for i in range (1,23):
            query = duckdb_conn.execute("select query from tpch_queries() where query_nr="+str(i)).fetchone()[0]
            answers = duckdb_conn.execute("select answer from tpch_answers() where scale_factor = 0.01 and query_nr="+str(i)).fetchone()[1:]
            result = duckdb_conn.execute(query)
            assert(check_result(result,answers))
            print ("Query " + str(i) + " works")

        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")

        for i in range (1,23):
            query = duckdb_conn.execute("select query from tpch_queries() where query_nr="+str(i)).fetchone()[0]
            answers = duckdb_conn.execute("select answer from tpch_answers() where scale_factor = 0.01 and query_nr="+str(i)).fetchone()[1:]
            result = duckdb_conn.execute(query)
            assert(check_result(result,answers))
            print ("Query " + str(i) + " works (Parallel)")
