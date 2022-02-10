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
    print(aggregation_function('i').execute().fetchone())
    assert aggregation_function('i').execute().fetchone() == assertion_answers[0]

    # Check multi column
    print(aggregation_function('i,j').execute().fetchone() )
    assert aggregation_function('i,j').execute().fetchone() == assertion_answers[1]

    if len(assertion_answers) < 3:
        # Shouldn't be able to aggregate on string
        with pytest.raises(Exception, match='No function matches the given name'):
            aggregation_function('k').execute().fetchone()
    else:
        print (aggregation_function('k').execute().fetchone())
        assert  aggregation_function('k').execute().fetchone() == assertion_answers[2]
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

    def test_count(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.count,[(2,), (2,2), (2,)])

    def test_median(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        # is this supposed to accept strings?
        aggregation_generic(rel.median,[(1.5,), (1.5, Decimal('2.10')), ('a',)])

    def test_min(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.min,[(1,), (1, Decimal('2.10')), ('a',)])

    def test_max(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.max,[(2,), (2, Decimal('3.2')), ('b',)])

    def test_mean(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.mean,[(1.5,), (1.5, 2.65)])

    def test_var(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.var,[(0.25,), (0.25, 0.30249999999999994)])

    def test_std(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.std,[(0.5,), (0.5, 0.5499999999999999)])

    def test_apply(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        rel.apply('sum', 'i').execute().fetchone() == (3,)

    def test_quantile(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        extra_param = '0.5'
        aggregation_function = rel.quantile
        # Check single column
        assert aggregation_function(extra_param,'i').execute().fetchone() == (1,)

        # Check multi column
        # with pytest.raises(Exception, match='No function matches the given name'):
        assert aggregation_function(extra_param,'i,j').execute().fetchone() == (1, Decimal('2.10'))

        assert aggregation_function(extra_param,'k').execute().fetchone() == ('a',)

        # Check empty
        with pytest.raises(Exception, match='incompatible function arguments'):
            aggregation_function().execute().fetchone()
        # Check Null
        with pytest.raises(Exception, match='incompatible function arguments'):
            aggregation_function(None).execute().fetchone()
    
        # Check broken
        with pytest.raises(Exception, match='incompatible function arguments.'):
            aggregation_function('bla').execute().fetchone()


