import duckdb
import pytest
from decimal import Decimal

pa = pytest.importorskip("pyarrow")


# class TestArrowDecimal256(object):
#     def test_decimal_32(self, duckdb_cursor):
duckdb_cursor = duckdb.connect()
decimal_32 = pa.Table.from_pylist(
    [{"data": Decimal("100.20")}, {"data": Decimal("110.21")}, {"data": Decimal("31.20")}, {"data": Decimal("500.20")}],
    pa.schema([("data", pa.decimal32(5, 2))]),
)
# Test scan
assert duckdb_cursor.execute("FROM decimal_32").fetchall() == [
    (Decimal('100.20'),),
    (Decimal('110.21'),),
    (Decimal('31.20'),),
    (Decimal('500.20'),),
]
# Test filter pushdown
assert duckdb_cursor.execute("SELECT COUNT(*) FROM decimal_32 where data > 100 and data < 200 ").fetchall() == [(2,)]

# Test write


duckdb_cursor = duckdb.connect()
decimal_64 = pa.Table.from_pylist(
    [
        {"data": Decimal("1000.231")},
        {"data": Decimal("1100.231")},
        {"data": Decimal("2000.231")},
        {"data": Decimal("500.20")},
    ],
    pa.schema([("data", pa.decimal64(8, 3))]),
)

# Test scan
assert duckdb_cursor.execute("FROM decimal_64").fetchall() == [
    (Decimal('1000.231'),),
    (Decimal('1100.231'),),
    (Decimal('2000.231'),),
    (Decimal('500.200'),),
]


# Test Filter pushdown
assert duckdb_cursor.execute("SELECT COUNT(*) FROM decimal_64 WHERE data > 1000 and data < 1200").fetchall() == [(2,)]
