import duckdb
import pytest
pa = pytest.importorskip("pyarrow", minversion='12.0.1')
ds = pytest.importorskip("pyarrow.dataset")
# First version that supports run_end_encode
pc = pytest.importorskip("pyarrow.compute")

class TestArrowREE(object):
    def test_timestamp_types(self):
        con = duckdb.connect()

        # Create a PyArrow array
        data = [0, 1, 0, 0, 1, 1, 0]
        array = pa.array(data)

        # Run the run-end encoding on the array
        encoded_array = pc.run_end_encode(array)
        ree_table = pa.Table.from_arrays([encoded_array], names=['encoded_data'])

        # Print the encoded array
        print(ree_table)
        res = duckdb.sql('select * from ree_table').fetchall()
        print(res)