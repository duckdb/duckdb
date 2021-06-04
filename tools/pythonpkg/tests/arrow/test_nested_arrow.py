import duckdb
try:
    import pyarrow as pa
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

    def test_list_types(self,duckdb_cursor):
        if not can_run:
            return

        #Large Lists
        data = pyarrow.array([[1],None, [2]], type=pyarrow.large_list(pyarrow.int64()))
        arrow_table = pa.Table.from_arrays([data],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        res = rel.execute().fetchall()
        assert res == [([1],), (None,), ([2],)]

        #Fixed Size Lists
        data = pyarrow.array([[1],None, [2]], type=pyarrow.list_(pyarrow.int64(),1))
        arrow_table = pa.Table.from_arrays([data],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        res = rel.execute().fetchall()
        assert res == [([1],), (None,), ([2],)]
        
        #Complex nested structures with different list types
        data = [pyarrow.array([[1],None, [2]], type=pyarrow.list_(pyarrow.int64(),1)),pyarrow.array([[1],None, [2]], type=pyarrow.large_list(pyarrow.int64())),pyarrow.array([[1,2,3],None, [2,1]], type=pyarrow.list_(pyarrow.int64()))]
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2]],['a','b','c'])
        rel = duckdb.from_arrow_table(arrow_table)
        res = rel.project('a').execute().fetchall()
        assert res == [([1],), (None,), ([2],)]
        res = rel.project('b').execute().fetchall()
        assert res == [([1],), (None,), ([2],)]
        res = rel.project('c').execute().fetchall()
        assert res == [([1,2,3],), (None,), ([2,1],)]

        
   

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

        #LIST[LIST[LIST[LIST[LIST[INTEGER]]]]]]
        compare_results("SELECT list (lllle) llllle from (SELECT list (llle) lllle from (SELECT list(lle) llle from (SELECT LIST(le) lle FROM (SELECT LIST(i) le from range(100) tbl(i) group by i%10) as t) as t1) as t2) as t3")
                
        compare_results('''SELECT grp,lst,cs FROM (select grp, lst, case when grp>1 then lst else list_value(null) end as cs
                        from (SELECT a%4 as grp, list(a) as lst FROM range(7) tbl(a) group by grp) as lst_tbl) as T;''')

        #Tests for converting multiple lists to/from Arrow with NULL values and/or strings
        compare_results("SELECT list(st) from (select i, case when i%10 then NULL else i::VARCHAR end as st from range(1000) tbl(i)) as t group by i%5")