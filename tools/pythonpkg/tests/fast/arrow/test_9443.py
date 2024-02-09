import duckdb
import pytest

pq = pytest.importorskip("pyarrow.parquet")
pa = pytest.importorskip("pyarrow")

from datetime import time
from tempfile import NamedTemporaryFile


class Test9443(object):
    def test_9443(self, tmp_path, duckdb_cursor):
        arrow_table = pa.Table.from_pylist(
            [
                {"col1": time(1, 2, 3)},
            ]
        )  # col1: time64[us]

        print(arrow_table)

        temp_file = tmp_path / "temp_file.parquet"
        # Perform operations using temp_file
        with temp_file.open("wb") as file:
            pq.write_table(arrow_table, temp_file)

            sql = f'SELECT * FROM "{temp_file}"'

            duckdb_cursor.execute(sql)
            with pytest.raises(Exception, match='Invalid Error'):
                duckdb_cursor.fetch_record_batch()
