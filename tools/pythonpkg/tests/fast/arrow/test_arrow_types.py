import duckdb
try:
    import pyarrow as pa
    import pyarrow.dataset as ds
    can_run = True
except:
    can_run = False

class TestArrowTypes(object):
   
    def test_null_type(self, duckdb_cursor):
        if not can_run:
            return
        schema = pa.schema([("data", pa.null())])
        inputs = [pa.array([None,None,None], type=pa.null())]
        arrow_table = pa.Table.from_arrays(inputs, schema=schema)
        duckdb_conn = duckdb.connect()
        duckdb_conn.register("testarrow",arrow_table)
        rel = duckdb.from_arrow(arrow_table).arrow()
        # We turn it to an array of int32 nulls
        schema = pa.schema([("data", pa.int32())])
        inputs = [pa.array([None,None,None], type=pa.null())]
        arrow_table = pa.Table.from_arrays(inputs, schema=schema)

        assert rel['data'] == arrow_table['data']

