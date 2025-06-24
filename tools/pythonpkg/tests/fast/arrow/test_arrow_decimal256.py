import duckdb
import pytest
from decimal import Decimal

pa = pytest.importorskip("pyarrow")


class TestArrowDecimal256(object):
    def test_decimal_256_throws(self, duckdb_cursor):
        with duckdb.connect() as conn:
            pa_decimal256 = pa.Table.from_pylist(
                [{"data": Decimal("100.00")} for _ in range(4)],
                pa.schema([("data", pa.decimal256(12, 4))]),
            )
            with pytest.raises(
                duckdb.NotImplementedException, match="Unsupported Internal Arrow Type for Decimal d:12,4,256"
            ):
                conn.execute("select * from pa_decimal256;").arrow().to_pylist()
