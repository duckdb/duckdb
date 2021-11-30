import pytest
import duckdb
import pandas as pd

def compare_results(query, list_values):
    df_duck = duckdb.query(query).df()
    counter = 0
    duck_values = df_duck['a']
    # print(duck_values)
    # print(duckdb.query(query).fetchall())
    for duck_value in duck_values:
        assert duck_value == list_values[counter]
        counter+=1

class TestFetchNested(object):

    def test_fetch_df_list(self, duckdb_cursor):
        # Integers
        compare_results("SELECT a from (select list_value(3,5,10) as a) as t",[[3,5,10]])
        compare_results("SELECT a from (select list_value(3,5,NULL) as a) as t",[[3,5,None]])
        compare_results("SELECT a from (select list_value(NULL,NULL,NULL) as a) as t",[[None,None,None]])
        compare_results("SELECT a from (select list_value() as a) as t",[[]])
        
        #Strings
        compare_results("SELECT a from (select list_value('test','test_one','test_two') as a) as t",[['test','test_one','test_two']])
        compare_results("SELECT a from (select list_value('test','test_one',NULL) as a) as t",[['test','test_one',None]])
        
        #Big Lists
        compare_results("SELECT a from (SELECT LIST(i) as a FROM range(10000) tbl(i)) as t",[list(range(0, 10000))])
        
        # #Multiple Lists
        compare_results("SELECT a from (SELECT LIST(i) as a FROM range(5) tbl(i) group by i%2) as t",[[0,2,4],[1,3]])
        
        #Unique Constants
        compare_results("SELECT a from (SELECT list_value(1) as a FROM range(5) tbl(i)) as t",[[1],[1],[1],[1],[1]])
        
        #Nested Lists
        compare_results("SELECT LIST(le) as a FROM (SELECT LIST(i) le from range(5) tbl(i) group by i%2) as t",[[[0, 2, 4], [1, 3]]])

        #LIST[LIST[LIST[LIST[LIST[INTEGER]]]]]]
        compare_results("SELECT list (lllle)  as a from (SELECT list (llle) lllle from (SELECT list(lle) llle from (SELECT LIST(le) lle FROM (SELECT LIST(i) le from range(5) tbl(i) group by i%2) as t) as t1) as t2) as t3",[[[[[[0, 2, 4], [1, 3]]]]] ])

        compare_results('''SELECT grp,lst,a FROM (select grp, lst, case when grp>1 then lst else list_value(null) end as a
                         from (SELECT a_1%4 as grp, list(a_1) as lst FROM range(7) tbl(a_1) group by grp) as lst_tbl) as T;''',[[None],[None],[2, 6],[3]])

        #Tests for converting multiple lists to/from Pandas with NULL values and/or strings
        compare_results("SELECT list(st) as a from (select i, case when i%5 then NULL else i::VARCHAR end as st from range(10) tbl(i)) as t group by i%2",[['0', None, None, None, None],[None, None, '5', None, None]])


