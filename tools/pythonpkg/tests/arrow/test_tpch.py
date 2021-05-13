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

def get_answer(answers_path,i):
    answer_path = ''
    answers = []
    if i < 10:
        answer_path = os.path.join(answers_path,'q0'+str(i)+'.csv')
    else:
        answer_path = os.path.join(answers_path,'q'+str(i)+'.csv')

    with open(answer_path, 'r') as file:
        answers = file.read()
    answers = answers.split("\n")[1:-1]
    return answers

def get_query(queries_path,i):
    query_path = ''
    query = ''
    if i < 10:
        query_path = os.path.join(queries_path,'q0'+str(i)+'.sql')
    else:
        query_path = os.path.join(queries_path,'q'+str(i)+'.sql')

    with open(query_path, 'r') as file:
        query = file.read()
    return query

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

        tpch_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'tpch')
        queries_path = os.path.join(tpch_path,'queries')
        answers_path = os.path.join(tpch_path,'answers')

        for i in range (1,23):
            query = get_query(queries_path,i)
            answers = get_answer(answers_path,i)
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

        tpch_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','..','..','..','extension','tpch','dbgen')
        queries_path = os.path.join(tpch_path,'queries')
        answers_path = os.path.join(tpch_path,'answers','sf0.01')

        for i in range (1,23):
            query = get_query(queries_path,i)
            answers = get_answer(answers_path,i)
            result = duckdb_conn.execute(query)
            assert(check_result(result,answers))
            print ("Query " + str(i) + " works")

        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")

        for i in range (1,23):
            query = get_query(queries_path,i)
            answers = get_answer(answers_path,i)
            result = duckdb_conn.execute(query)
            assert(check_result(result,answers))
            print ("Query " + str(i) + " works (Parallel)")
