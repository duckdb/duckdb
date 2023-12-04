import duckdb
import os
import pytest
import tempfile

pa = pytest.importorskip("pyarrow", minversion="11")
pq = pytest.importorskip("pyarrow.parquet", minversion="11")


class Test7652(object):
    def test_7652(self):
        temp_file_name = tempfile.NamedTemporaryFile(suffix='.parquet').name
        # Generate a list of values that aren't uniform in changes.
        generated_list = [1, 0, 2]

        print("Generated values:", generated_list)
        print(f"Min value: {min(generated_list)} max value: {max(generated_list)}")

        # Convert list of values to a PyArrow table with a single column.
        fake_table = pa.Table.from_arrays([pa.array(generated_list, pa.int64())], names=['n0'])

        # Write that column with DELTA_BINARY_PACKED encoding
        with pq.ParquetWriter(
            temp_file_name, fake_table.schema, column_encoding={"n0": "DELTA_BINARY_PACKED"}, use_dictionary=False
        ) as writer:
            writer.write_table(fake_table)

        # Check to make sure that PyArrow can read the file and retrieve the expected values.
        # Assert the values read from PyArrow are the same
        read_table = pq.read_table(temp_file_name, use_threads=False)

        read_list = read_table["n0"].to_pylist()
        assert min(read_list) == min(generated_list)
        assert max(read_list) == max(generated_list)
        assert read_list == generated_list

        # Attempt to perform the same thing with duckdb.
        print("Retrieving from duckdb")
        duckdb_result = list(map(lambda v: v[0], duckdb.query(f"select * from '{temp_file_name}'").fetchall()))

        print("DuckDB result:", duckdb_result)
        assert min(duckdb_result) == min(generated_list)
        assert max(duckdb_result) == max(generated_list)
        assert duckdb_result == generated_list
