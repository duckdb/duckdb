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


class TestArrowRecordBatchReader(object):
    def test_parallel_reader(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        rel = duckdb_conn.from_arrow(reader)

        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12
        )
        # The reader is already consumed so this should be 0
        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 0
        )

    def test_parallel_reader_replacement_scans(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        assert (
            duckdb_conn.execute(
                "select count(*) from reader where first_name=\'Jose\' and salary > 134708.82"
            ).fetchone()[0]
            == 12
        )
        assert (
            duckdb_conn.execute(
                "select count(*) from reader where first_name=\'Jose\' and salary > 134708.82"
            ).fetchone()[0]
            == 0
        )

    def test_parallel_reader_register(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        duckdb_conn.register("bla", reader)

        assert (
            duckdb_conn.execute("select count(*) from bla where first_name=\'Jose\' and salary > 134708.82").fetchone()[
                0
            ]
            == 12
        )
        assert (
            duckdb_conn.execute("select count(*) from bla where first_name=\'Jose\' and salary > 134708.82").fetchone()[
                0
            ]
            == 0
        )

    def test_parallel_reader_default_conn(self, duckdb_cursor):
        if not can_run:
            return

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        rel = duckdb.from_arrow(reader)

        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12
        )
        # The reader is already consumed so this should be 0
        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 0
        )
