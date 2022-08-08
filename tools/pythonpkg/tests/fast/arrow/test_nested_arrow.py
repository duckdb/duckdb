import duckdb
try:
    import pyarrow as pa
    import pyarrow.parquet
    import numpy as np
    import pandas as pd
    import pytest
    can_run = True
except:
    can_run = False

def compare_results(query):
    true_answer = duckdb.query(query).fetchall()
    from_arrow = duckdb.from_arrow(duckdb.query(query).arrow()).fetchall()
    assert true_answer == from_arrow

def arrow_to_pandas(query):
    return duckdb.query(query).arrow().to_pandas()['a'].values.tolist()

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
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [([1],), (None,), ([2],)]

        #Fixed Size Lists
        data = pyarrow.array([[1],None, [2]], type=pyarrow.list_(pyarrow.int64(),1))
        arrow_table = pa.Table.from_arrays([data],['a'])
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [([1],), (None,), ([2],)]

        #Complex nested structures with different list types
        data = [pyarrow.array([[1],None, [2]], type=pyarrow.list_(pyarrow.int64(),1)),pyarrow.array([[1],None, [2]], type=pyarrow.large_list(pyarrow.int64())),pyarrow.array([[1,2,3],None, [2,1]], type=pyarrow.list_(pyarrow.int64()))]
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2]],['a','b','c'])
        rel = duckdb.from_arrow(arrow_table)
        res = rel.project('a').execute().fetchall()
        assert res == [([1],), (None,), ([2],)]
        res = rel.project('b').execute().fetchall()
        assert res == [([1],), (None,), ([2],)]
        res = rel.project('c').execute().fetchall()
        assert res == [([1,2,3],), (None,), ([2,1],)]

        #Struct Holding different List Types
        struct = [pa.StructArray.from_arrays( data,['fixed', 'large','normal'])]
        arrow_table = pa.Table.from_arrays(struct,['a'])
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [({'fixed': [1], 'large': [1], 'normal': [1, 2, 3]},), ({'fixed': None, 'large': None, 'normal': None},), ({'fixed': [2], 'large': [2], 'normal': [2, 1]},)]


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

    def test_struct_roundtrip(self,duckdb_cursor):
        if not can_run:
            return
        compare_results("SELECT a from (SELECT STRUCT_PACK(a := 42, b := 43) as a) as t")
        compare_results("SELECT a from (SELECT STRUCT_PACK(a := NULL, b := 43) as a) as t")
        compare_results("SELECT a from (SELECT STRUCT_PACK(a := NULL) as a) as t")
        compare_results("SELECT a from (SELECT STRUCT_PACK(a := i, b := i) as a FROM range(10000) tbl(i)) as t")
        compare_results("SELECT a from (SELECT STRUCT_PACK(a := LIST_VALUE(1,2,3), b := i) as a FROM range(10000) tbl(i)) as t")

    def test_map_roundtrip(self,duckdb_cursor):
        if not can_run:
            return
        compare_results("SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as a) as t")

        compare_results("SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as a) as t")

        compare_results("SELECT a from (select MAP(LIST_VALUE(),LIST_VALUE()) as a) as t")
        compare_results("SELECT a from (select MAP(LIST_VALUE('Jon Lajoie', 'Backstreet Boys', 'Tenacious D'),LIST_VALUE(10,9,10)) as a) as t")
        compare_results("SELECT a from (select MAP(LIST_VALUE('Jon Lajoie','Tenacious D'),LIST_VALUE(10,10)) as a) as t")
        compare_results("SELECT m from (select MAP(list_value(1), list_value(2)) from range(5) tbl(i)) tbl(m)")
        compare_results("SELECT m from (select MAP(lsta,lstb) as m from (SELECT list(i) as lsta, list(i) as lstb from range(10000) tbl(i) group by i%5) as lst_tbl) as T")

    def test_map_arrow_to_duckdb(self, duckdb_cursor):
        if not can_run:
            return
        map_type = pa.map_(pa.int32(), pa.int32())
        values = [
            [
                (3, 12),
                (3, 21)
            ],
            [
                (5, 42)
            ]
        ]
        arrow_table = pa.table(
            {'detail': pa.array(values, map_type)}
        )
        with pytest.raises(Exception, match="Invalid Input Error: Arrow map contains duplicate key, which isn't supported by DuckDB map type"):
            rel = duckdb.from_arrow(arrow_table).fetchall()

    def test_map_arrow_to_pandas(self,duckdb_cursor):
        if not can_run:
            return
        assert arrow_to_pandas("SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as a) as t") == [[(1, 10), (2, 9), (3, 8), (4, 7)]]
        assert arrow_to_pandas("SELECT a from (select MAP(LIST_VALUE(),LIST_VALUE()) as a) as t") == [[]]
        assert arrow_to_pandas("SELECT a from (select MAP(LIST_VALUE('Jon Lajoie', 'Backstreet Boys', 'Tenacious D'),LIST_VALUE(10,9,10)) as a) as t") == [[('Jon Lajoie', 10), ('Backstreet Boys', 9), ('Tenacious D', 10)]]
        assert arrow_to_pandas("SELECT a from (select MAP(list_value(1), list_value(2)) from range(5) tbl(i)) tbl(a)") == [[(1, 2)], [(1, 2)], [(1, 2)], [(1, 2)], [(1, 2)]]
        assert arrow_to_pandas("SELECT MAP(LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}),LIST_VALUE({'i':1,'j':2},{'i':3,'j':4})) as a") == [[({'i': 1, 'j': 2}, {'i': 1, 'j': 2}), ({'i': 3, 'j': 4}, {'i': 3, 'j': 4})]]


    def test_frankstein_nested(self,duckdb_cursor):
        if not can_run:
            return
        # List of structs W/ Struct that is NULL entirely
        compare_results("SELECT [{'i':1,'j':2},NULL,{'i':2,'j':NULL}]")

        # Lists of structs with lists
        compare_results("SELECT [{'i':1,'j':[2,3]},NULL]")

        # Maps embedded in a struct
        compare_results("SELECT {'i':mp,'j':mp2} FROM (SELECT MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as mp, MAP(LIST_VALUE(1, 2, 3, 5),LIST_VALUE(10, 9, 8, 7)) as mp2) as t")

        # List of maps
        compare_results("SELECT [mp,mp2] FROM (SELECT MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as mp, MAP(LIST_VALUE(1, 2, 3, 5),LIST_VALUE(10, 9, 8, 7)) as mp2) as t")

        # Map with list as key and/or value
        compare_results("SELECT MAP(LIST_VALUE([1,2],[3,4],[5,4]),LIST_VALUE([1,2],[3,4],[5,4]))")

        # Map with struct as key and/or value
        compare_results("SELECT MAP(LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}),LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}))")

        # Struct that is NULL entirely
        compare_results("SELECT * FROM (VALUES ({'i':1,'j':2}), (NULL), ({'i':1,'j':2}), (NULL)) as a")

        # Null checks on lists with structs
        compare_results("SELECT [{'i':1,'j':[2,3]},NULL,{'i':1,'j':[2,3]}]")

        # MAP that is NULL entirely
        compare_results("SELECT * FROM (VALUES (MAP(LIST_VALUE(1,2),LIST_VALUE(3,4))),(NULL), (MAP(LIST_VALUE(1,2),LIST_VALUE(3,4))), (NULL)) as a")
