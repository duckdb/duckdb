import duckdb
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

class TestArrowNested(object):
    def test_lists(self,duckdb_cursor):
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
        assert query[0][1] == None

        #Test Constant List With only Null
        query = duckdb.query("SELECT a from (select list_value(NULL,NULL) as a) as t").arrow()['a'].to_numpy() 
        assert query[0][0] == None
        assert query[0][1] == None
