import duckdb
import pytest
import os
import pandas as pd
try:
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    can_run = True
except:
    can_run = False

class TestArrowReplacementScan(object):
    def test_arrow_table_replacement_scan(self, duckdb_cursor):
        if not can_run:
            return

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')
        userdata_parquet_table = pq.read_table(parquet_filename)
        con = duckdb.connect()
        assert con.execute("select count(*) from userdata_parquet_table").fetchone() ==  (1000,)

    def test_arrow_dataset_replacement_scan(self, duckdb_cursor):
        if not can_run:
            return
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')
        userdata_parquet_table = pq.read_table(parquet_filename)
        userdata_parquet_dataset= ds.dataset(parquet_filename)

        con = duckdb.connect()
        assert con.execute("select count(*) from userdata_parquet_dataset").fetchone() ==  (1000,)

    def test_replacement_scan_fail(self, duckdb_cursor):
        if not can_run:
            return

        random_object = "I love salmiak rondos"
        with pytest.raises(Exception):
            con.execute("select count(*) from random_object").fetchone()