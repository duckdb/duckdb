import duckdb
import pytest
from decimal import Decimal

pa = pytest.importorskip("pyarrow")


class TestArrowDecimalTypes(object):
    def test_v1_5(self, duckdb_cursor):
        duckdb_cursor = duckdb.connect()
        decimal_32 = pa.Table.from_pylist(
            [
                {"data": Decimal("100.20")},
                {"data": Decimal("110.21")},
                {"data": Decimal("31.20")},
                {"data": Decimal("500.20")},
            ],
            pa.schema([("data", pa.decimal32(5, 2))]),
        )
        col_type = duckdb_cursor.execute("FROM decimal_32").arrow().schema.field("data").type
        assert col_type.bit_width == 32 and pa.types.is_decimal(col_type)

        decimal_64 = pa.Table.from_pylist(
            [
                {"data": Decimal("1000.231")},
                {"data": Decimal("1100.231")},
                {"data": Decimal("999999999999.231")},
                {"data": Decimal("500.20")},
            ],
            pa.schema([("data", pa.decimal64(16, 3))]),
        )
        col_type = duckdb_cursor.execute("FROM decimal_64").arrow().schema.field("data").type
        assert col_type.bit_width == 64 and pa.types.is_decimal(col_type)
        for version in ['1.0', '1.1', '1.2', '1.3', '1.4']:
            duckdb_cursor.execute(f"SET arrow_output_version = {version}")
            result = duckdb_cursor.execute("FROM decimal_32").arrow()
            col_type = result.schema.field("data").type
            assert col_type.bit_width == 128 and pa.types.is_decimal(col_type)
            assert result.to_pydict() == {
                'data': [Decimal('100.20'), Decimal('110.21'), Decimal('31.20'), Decimal('500.20')]
            }

            result = duckdb_cursor.execute("FROM decimal_64").arrow()
            col_type = result.schema.field("data").type
            assert col_type.bit_width == 128 and pa.types.is_decimal(col_type)
            assert result.to_pydict() == {
                'data': [Decimal('1000.231'), Decimal('1100.231'), Decimal('999999999999.231'), Decimal('500.200')]
            }
