import duckdb
import pytest
from packaging import version

pa = pytest.importorskip('pyarrow')

pytestmark = pytest.mark.skipif(
    version.parse(pa.__version__) <= version.parse("15.0"), reason="requires pyarrow version 16 or higher"
)


def RoundTripStringView(query, array):
    con = duckdb.connect()
    con.execute("SET produce_arrow_string_view=True")
    arrow_tbl = con.execute(query).arrow()
    # Assert that we spit the same as the defined array
    assert arrow_tbl[0].combine_chunks().tolist() == array.tolist()

    # Generate an arrow table
    # Create a field for the array with a specific data type
    field = pa.field('str_val', pa.string_view())

    # Create a schema for the table using the field
    schema = pa.schema([field])

    # Create a table using the schema and the array
    gt_table = pa.Table.from_arrays([array], schema=schema)
    arrow_table = con.execute("select * from gt_table").arrow()
    assert arrow_tbl[0].combine_chunks().tolist() == array.tolist()


class TestArrowReplacementScan(object):
    # Test Small Inlined String View
    def test_inlined_string_view(self, duckdb_cursor):
        RoundTripStringView(
            "SELECT (i*10^i)::varchar str FROM range(5) tbl(i) ",
            pa.array(["0.0", "10.0", "200.0", "3000.0", "40000.0"], type=pa.string_view()),
        )


# Test Small Inlined String View

# Test Small Inlined String View With Nulls

# Test Large Inlined String View
