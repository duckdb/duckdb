import duckdb
from decimal import Decimal
import pytest

class TestRAPISum(object):
    def test_sum(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("Create Table bla (i integer, j decimal(5,2), k varchar)")
        con.execute("insert into bla values (1,2.1,'a'), (2,3.2,'b'), (NULL, NULL, NULL)")
        rel = con.table('bla')

        # Check single column Sum
        assert rel.sum('i').execute().fetchone() == (3,)

        # Check multi column Sum
        assert rel.sum('i,j').execute().fetchone() == (3, Decimal('5.30'))

        # Check column that can't be summed
        with pytest.raises(Exception, match='No function matches the given name'):
            rel.sum('k').execute().fetchone()

        # Check empty
        with pytest.raises(Exception, match='incompatible function arguments'):
            rel.sum().execute().fetchone()
        # Check Null
        with pytest.raises(Exception, match='incompatible function arguments'):
            rel.sum(None).execute().fetchone()
            
        # Check broken
        with pytest.raises(Exception, match='Referenced column "bla" not found'):
            rel.sum('bla').execute().fetchone()