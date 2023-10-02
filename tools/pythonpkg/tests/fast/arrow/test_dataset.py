import duckdb
import os
import pytest

pyarrow = pytest.importorskip("pyarrow")
np = pytest.importorskip("numpy")
pyarrow.parquet = pytest.importorskip("pyarrow.parquet")
pyarrow.dataset = pytest.importorskip("pyarrow.dataset")


class TestArrowDataset(object):
    def test_parallel_dataset(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        rel = duckdb_conn.from_arrow(userdata_parquet_dataset)

        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12
        )

    def test_parallel_dataset_register(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        rel = duckdb_conn.register("dataset", userdata_parquet_dataset)

        assert (
            duckdb_conn.execute(
                "Select count(*) from dataset where first_name = 'Jose' and salary > 134708.82"
            ).fetchone()[0]
            == 12
        )

    def test_parallel_dataset_roundtrip(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        rel = duckdb_conn.register("dataset", userdata_parquet_dataset)

        query = duckdb_conn.execute("SELECT * FROM dataset order by id")
        record_batch_reader = query.fetch_record_batch(2048)

        arrow_table = record_batch_reader.read_all()
        # reorder since order of rows isn't deterministic
        df = userdata_parquet_dataset.to_table().to_pandas().sort_values('id').reset_index(drop=True)
        # turn it into an arrow table
        arrow_table_2 = pyarrow.Table.from_pandas(df)
        result_1 = duckdb_conn.execute("select * from arrow_table order by all").fetchall()

        result_2 = duckdb_conn.execute("select * from arrow_table_2 order by all").fetchall()

        assert result_1 == result_2

    def test_ducktyping(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        dataset = CustomDataset()
        query = duckdb_conn.execute("SELECT b FROM dataset WHERE a < 5")
        record_batch_reader = query.fetch_record_batch(2048)
        arrow_table = record_batch_reader.read_all()
        assert arrow_table.equals(CustomDataset.DATA[:5].select(['b']))


class CustomDataset(pyarrow.dataset.Dataset):
    # For testing duck-typing of dataset/scanner https://github.com/duckdb/duckdb/pull/5998
    SCHEMA = pyarrow.schema([pyarrow.field("a", pyarrow.int64(), True), pyarrow.field("b", pyarrow.float64(), True)])
    DATA = pyarrow.Table.from_arrays([pyarrow.array(range(100)), pyarrow.array(np.arange(100) * 1.0)], schema=SCHEMA)

    def __init__(self):
        pass

    def scanner(self, **kwargs):
        return CustomScanner(**kwargs)

    @property
    def schema(self):
        return CustomDataset.SCHEMA


class CustomScanner(pyarrow.dataset.Scanner):
    def __init__(self, filter=None, columns=None, **kwargs):
        self.filter = filter
        self.columns = columns
        self.kwargs = kwargs

    @property
    def projected_schema(self):
        if self.columns is None:
            return CustomDataset.SCHEMA
        else:
            return pyarrow.schema([f for f in CustomDataset.SCHEMA.fields if f.name in self.columns])

    def to_reader(self):
        return pyarrow.dataset.dataset(CustomDataset.DATA).scanner(filter=self.filter, columns=self.columns).to_reader()
