import duckdb
try:
    import pyarrow as pa
    import pyarrow.parquet
    import numpy as np
    from pandas import Timestamp
    import datetime
    import pandas as pd
    can_run = True
except:
    can_run = False

class TestArrowDictionary(object):

    def test_dictionary(self,duckdb_cursor):
        if not can_run:
            return
        indices = pa.array([0, 1, 0, 1, 2, 1, 0, 2])
        dictionary = pa.array([10, 100, None])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        
        assert rel.execute().fetchall() == [(10,), (100,), (10,), (100,), (None,), (100,), (10,), (None,)]

        # Bigger than Vector Size
        indices_list = [0, 1, 0, 1, 2, 1, 0, 2,3] * 10000
        indices = pa.array(indices_list)
        dictionary = pa.array([10, 100, None,999999])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        result = [(10,), (100,), (10,), (100,), (None,), (100,), (10,), (None,), (999999,)] * 10000
        assert rel.execute().fetchall() == result

        #Table with dictionary and normal array

        arrow_table = pa.Table.from_arrays([dict_array,pa.array(indices_list)],['a','b'])
        rel = duckdb.from_arrow_table(arrow_table)
        result = [(10,0), (100,1), (10,0), (100,1), (None,2), (100,1), (10,0), (None,2), (999999,3)] * 10000
        assert rel.execute().fetchall() == result

    def test_dictionary_null_index(self,duckdb_cursor):
        if not can_run:
            return
        indices = pa.array([None, 1, 0, 1, 2, 1, 0, 2])
        dictionary = pa.array([10, 100, None])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        
        assert rel.execute().fetchall() == [(None,), (100,), (10,), (100,), (None,), (100,), (10,), (None,)]

        indices = pa.array([None, 1, None, 1, 2, 1, 0])
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        print (rel.execute().fetchall())
        assert rel.execute().fetchall() == [(None,), (100,), (None,), (100,), (100,), (100,), (10,)]
        
        # Test Big Vector
        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 1000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 1000
        assert rel.execute().fetchall() == result
        
        #Table with dictionary and normal array
        arrow_table = pa.Table.from_arrays([dict_array,indices],['a','b'])
        rel = duckdb.from_arrow_table(arrow_table)
        result = [(None,None), (100,1), (None,None), (100,1), (100,2), (100,1), (10,0)] * 1000
        assert rel.execute().fetchall() == result

    def test_dictionary_batches(self,duckdb_cursor):
        if not can_run:
            return
        
        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 10000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        batch_arrow_table = pyarrow.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb.from_arrow_table(batch_arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
        assert rel.execute().fetchall() == result

        #Table with dictionary and normal array
        arrow_table = pa.Table.from_arrays([dict_array,indices],['a','b'])
        batch_arrow_table = pyarrow.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb.from_arrow_table(batch_arrow_table)
        result = [(None,None), (100,1), (None,None), (100,1), (100,2), (100,1), (10,0)] * 10000
        assert rel.execute().fetchall() == result

    def test_dictionary_batches_parallel(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")

        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 10000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        batch_arrow_table = pyarrow.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb_conn.from_arrow_table(batch_arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
        assert rel.execute().fetchall() == result

        #Table with dictionary and normal array
        arrow_table = pa.Table.from_arrays([dict_array,indices],['a','b'])
        batch_arrow_table = pyarrow.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb_conn.from_arrow_table(batch_arrow_table)
        result = [(None,None), (100,1), (None,None), (100,1), (100,2), (100,1), (10,0)] * 10000
        assert rel.execute().fetchall() == result

    def test_dictionary_index_types(self,duckdb_cursor):
        if not can_run:
            return
        indices_list = [None, 1, None, 1, 2, 1, 0]
        dictionary = pa.array([10, 100, 100], type=pyarrow.uint8())
        index_types = []
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.uint8()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.uint16()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.uint32()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.uint64()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.int8()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.int16()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.int32()))
        index_types.append(pa.array(indices_list * 10000, type=pyarrow.int64()))

        for index_type in index_types:
            dict_array = pa.DictionaryArray.from_arrays(index_type, dictionary)
            arrow_table = pa.Table.from_arrays([dict_array],['a'])
            rel = duckdb.from_arrow_table(arrow_table)
            result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)]* 10000
            assert rel.execute().fetchall() == result

    
    def test_dictionary_strings(self,duckdb_cursor):
        if not can_run:
            return

        indices_list = [None, 0, 1, 2, 3, 4, None]
        indices = pa.array(indices_list * 1000)
        dictionary = pa.array(['Matt Daaaaaaaaamon', 'Alec Baldwin', 'Sean Penn', 'Tim Robbins', 'Samuel L. Jackson'])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        result = [(None,), ('Matt Daaaaaaaaamon',), ( 'Alec Baldwin',), ('Sean Penn',), ('Tim Robbins',), ('Samuel L. Jackson',), (None,)] * 1000
        assert rel.execute().fetchall() == result

    def test_dictionary_timestamps(self,duckdb_cursor):
        if not can_run:
            return
        indices_list = [None, 0, 1, 2, None]
        indices = pa.array(indices_list * 1000)
        dictionary = pa.array([Timestamp(year=2001, month=9, day=25),Timestamp(year=2006, month=11, day=14),Timestamp(year=2012, month=5, day=15),Timestamp(year=2018, month=11, day=2)])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow_table(arrow_table)
        print (rel.execute().fetchall())
        result = [(None,), (datetime.datetime(2001, 9, 25, 0, 0),), (datetime.datetime(2006, 11, 14, 0, 0),), (datetime.datetime(2012, 5, 15, 0, 0),), (None,)] * 1000
        assert rel.execute().fetchall() == result
     