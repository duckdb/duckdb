import duckdb
from decimal import Decimal
import pytest

def initialize(con):
    con.execute("Create Table bla (i integer, j decimal(5,2), k varchar)")
    con.execute("insert into bla values (1,2.1,'a'), (2,3.2,'b'), (NULL, NULL, NULL)")
    return con.table('bla')


def aggregation_generic(aggregation_function,assertion_answers):
    assert len(assertion_answers) >=2


    # Check single column
    assert aggregation_function('i').execute().fetchone() == assertion_answers[0]

    # Check multi column
    assert aggregation_function('i,j').execute().fetchone() == assertion_answers[1]

    if len(assertion_answers) < 3:
        # Shouldn't be able to aggregate on string
        with pytest.raises(Exception, match='No function matches the given name'):
            aggregation_function('k').execute().fetchone()
    else:
        assert  rel.sum('k').execute().fetchone() == assertion_answers[2]
    # Check empty
    with pytest.raises(Exception, match='incompatible function arguments'):
        aggregation_function().execute().fetchone()
    # Check Null
    with pytest.raises(Exception, match='incompatible function arguments'):
        aggregation_function(None).execute().fetchone()
        
    # Check broken
    with pytest.raises(Exception, match='Referenced column "bla" not found'):
        aggregation_function('bla').execute().fetchone()

class TestRAPIAggregations(object):
    def test_sum(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.sum,[(3,), (3, Decimal('5.30'))])
