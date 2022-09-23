import duckdb
import os
try:
    import pyarrow
    import pyarrow.parquet
    import pyarrow.dataset
    import numpy as np
    can_run = True
except:
    can_run = False

class TestArrowDataset(object):

    def test_parallel_dataset(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        userdata_parquet_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        rel = duckdb_conn.from_arrow(userdata_parquet_dataset)

        assert rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12

    def test_parallel_dataset_register(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        userdata_parquet_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        rel = duckdb_conn.register("dataset",userdata_parquet_dataset)

        assert duckdb_conn.execute("Select count(*) from dataset where first_name = 'Jose' and salary > 134708.82").fetchone()[0] == 12

    def test_parallel_dataset_roundtrip(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        userdata_parquet_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        rel = duckdb_conn.register("dataset",userdata_parquet_dataset)

        query = duckdb_conn.execute("SELECT * FROM dataset order by id" )
        record_batch_reader = query.fetch_record_batch(2048)

        arrow_table = record_batch_reader.read_all()
        # reorder since order of rows isn't deterministic
        df = userdata_parquet_dataset.to_table().to_pandas().sort_values('id').reset_index(drop=True)
        # turn it into an arrow table
        arrow_table_2 = pyarrow.Table.from_pandas(df)

        assert arrow_table.equals(arrow_table_2)