import duckdb
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

def compare_results(query):
    true_answer = duckdb.query(query).fetchall()
    t = duckdb.query(query).arrow()
    from_arrow = duckdb.from_arrow_table(duckdb.query(query).arrow()).fetchall()

    assert true_answer == from_arrow

class TestArrowNested(object):
    def test_lists_basic(self,duckdb_cursor):
        if not can_run:
            return
        
        #Test Constant List
        query = duckdb.query("SELECT a from (select list_value(3,5,10) as a) as t").arrow()['a'].to_numpy() 
        assert query[0][0] == 3
        assert query[0][1] == 5
        assert query[0][2] == 10

        # Empty List
        query = duckdb.query("SELECT a from (select list_value() as a) as t").arrow()['a'].to_numpy() 
        assert len(query[0]) == 0

        #Test Constant List With Null
        query = duckdb.query("SELECT a from (select list_value(3,NULL) as a) as t").arrow()['a'].to_numpy() 
        assert query[0][0] == 3
        assert np.isnan(query[0][1])

    def test_lists_roundtrip(self,duckdb_cursor):
        if not can_run:
            return
        # Integers
        compare_results("SELECT a from (select list_value(3,5,10) as a) as t")
        compare_results("SELECT a from (select list_value(3,5,NULL) as a) as t")
        compare_results("SELECT a from (select list_value(NULL,NULL,NULL) as a) as t")
        compare_results("SELECT a from (select list_value() as a) as t")
        #Strings
        compare_results("SELECT a from (select list_value('test','test_one','test_two') as a) as t")
        compare_results("SELECT a from (select list_value('test','test_one',NULL) as a) as t")
        #Big Lists
        compare_results("SELECT a from (SELECT LIST(i) as a FROM range(10000) tbl(i)) as t")
        #Multiple Lists
        compare_results("SELECT a from (SELECT LIST(i) as a FROM range(10000) tbl(i) group by i%10) as t")
        #Unique Constants
        compare_results("SELECT a from (SELECT list_value(1) as a FROM range(10) tbl(i)) as t")
        #Nested Lists
        compare_results("SELECT LIST(le) FROM (SELECT LIST(i) le from range(100) tbl(i) group by i%10) as t")
      
        compare_results('''SELECT grp,lst,cs FROM (select grp, lst, case when grp>1 then lst else list_value(null) end as cs
                        from (SELECT a%4 as grp, list(a) as lst FROM range(7) tbl(a) group by grp) as lst_tbl) as T;''')
        


