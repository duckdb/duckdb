import duckdb
try:
    import pyarrow as pa
    import pyarrow.parquet
    import numpy as np
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

    def test_dictionary_batches(self,duckdb_cursor):
        if not can_run:
            return
        
        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 10000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array],['a'])
        batch_arrow_table = pyarrow.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb.from_arrow_table(arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
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
        rel = duckdb_conn.from_arrow_table(arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
        assert rel.execute().fetchall() == result