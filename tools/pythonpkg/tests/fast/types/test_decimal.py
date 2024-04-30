import numpy
import pandas
from decimal import *


class TestDecimal(object):
    def test_decimal(self, duckdb_cursor):
        duckdb_cursor.execute(
            'SELECT 1.2::DECIMAL(4,1), 100.3::DECIMAL(9,1), 320938.4298::DECIMAL(18,4), 49082094824.904820482094::DECIMAL(30,12), NULL::DECIMAL'
        )
        result = duckdb_cursor.fetchall()
        assert result == [
            (Decimal('1.2'), Decimal('100.3'), Decimal('320938.4298'), Decimal('49082094824.904820482094'), None)
        ]

    def test_decimal_numpy(self, duckdb_cursor):
        duckdb_cursor.execute(
            'SELECT 1.2::DECIMAL(4,1) AS a, 100.3::DECIMAL(9,1) AS b, 320938.4298::DECIMAL(18,4) AS c, 49082094824.904820482094::DECIMAL(30,12) AS d'
        )
        result = duckdb_cursor.fetchnumpy()
        assert result == {
            'a': numpy.array([Decimal('1.2')], dtype=object),
            'b': numpy.array([Decimal('100.3')], dtype=object),
            'c': numpy.array([Decimal('320938.4298')], dtype=object),
            'd': numpy.array([Decimal('49082094824.904820482094')], dtype=object),
        }
