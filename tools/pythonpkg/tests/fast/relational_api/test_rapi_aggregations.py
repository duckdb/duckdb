import duckdb
from decimal import Decimal
import pytest


def initialize(con):
    con.execute("Create Table bla (i integer, j decimal(5,2), k varchar)")
    con.execute("insert into bla values (1,2.1,'a'), (2,3.2,'b'), (NULL, NULL, NULL)")
    return con.table('bla')

def munge(cell):
    try:
        cell = round(float(cell), 2)
    except (ValueError, TypeError):
        cell = str(cell)
    return cell

def munge_compare(left_list, right_list):
    assert len(left_list) == len(right_list)
    for i in range (len(left_list)):
        tpl_left = left_list[i]
        tpl_right = right_list[i]
        assert len(tpl_left) == len(tpl_right)
        for j in range (len(tpl_left)):
            left_cell = munge(tpl_left[j])
            right_cell = munge(tpl_right[j])
            assert left_cell == right_cell


def aggregation_generic(aggregation_function,assertion_answers):
    assert len(assertion_answers) >=2
     # Check single column
    print(aggregation_function('i').execute().fetchall())
    munge_compare(aggregation_function('i').execute().fetchall(), assertion_answers[0])

    # Check multi column
    print(aggregation_function('i,j').execute().fetchall() )
    munge_compare(aggregation_function('i,j').execute().fetchall(), assertion_answers[1])

    if len(assertion_answers) < 3:
        # Shouldn't be able to aggregate on string
        with pytest.raises(Exception, match='No function matches the given name'):
            aggregation_function('k').execute().fetchall()
    else:
        print (aggregation_function('k').execute().fetchall())
        munge_compare( aggregation_function('k').execute().fetchall(), assertion_answers[2])
    # Check empty
    with pytest.raises(Exception, match='incompatible function arguments'):
        aggregation_function().execute().fetchall()
    # Check Null
    with pytest.raises(Exception, match='incompatible function arguments'):
        aggregation_function(None).execute().fetchall()
        
    # Check broken
    with pytest.raises(Exception, match='Referenced column "bla" not found'):
        aggregation_function('bla').execute().fetchall()

class TestRAPIAggregations(object):
    def test_sum(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.sum,[[(3,)], [(3, Decimal('5.30'))]])

    def test_count(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.count,[[(2,)], [(2,2)], [(2,)]])

    def test_median(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        # is this supposed to accept strings?
        aggregation_generic(rel.median,[[(1.5,)], [(1.5, Decimal('2.10'))], [('a',)]])

    def test_min(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.min,[[(1,)], [(1, Decimal('2.10'))], [('a',)]])

    def test_max(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.max,[[(2,)], [(2, Decimal('3.2'))], [('b',)]])

    def test_mean(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.mean,[[(1.5,)], [(1.5, 2.65)]])

    def test_var(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.var,[[(0.25,)], [(0.25, 0.30249999999999994)]])

    def test_std(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.std,[[(0.5,)], [(0.5, 0.5499999999999999)]])

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

    def test_value_counts(self, duckdb_cursor):
        con = duckdb.connect()
        rel = initialize(con)
        con.execute("insert into bla values (1,2.1,'a'), (NULL, NULL, NULL)")
        munge_compare(rel.value_counts('i').execute().fetchall(),[(None, 0), (1, 2), (2, 1)])
        with pytest.raises(Exception, match='Only one column is accepted'):
            rel.value_counts('i,j').execute().fetchall()

    def test_length(self, duckdb_cursor):
        con = duckdb.connect()
        rel = initialize(con)
        assert len(rel) == 3
        
    def test_shape(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        assert rel.shape == (3, 3)
        
    def test_unique(self, duckdb_cursor):
        rel = initialize(duckdb_cursor)
        aggregation_generic(rel.unique,[[(1,), (2,), (None,)], [(1, Decimal('2.10')), (2, Decimal('3.20')), (None, None)],[('a',), ('b',), (None,)]])

