
import duckdb
import pytest

class TestDuckDBQuery(object):
    def test_duckdb_query(self, duckdb_cursor):
        # we can use duckdb.query to run both DDL statements and select statements
        duckdb.query('create view v1 as select 42 i')
        rel = duckdb.query('select * from v1')
        assert rel.fetchall()[0][0] == 42;

        # also multiple statements
        duckdb.query('create view v2 as select i*2 j from v1; create view v3 as select j * 2 from v2;')
        rel = duckdb.query('select * from v3')
        assert rel.fetchall()[0][0] == 168;

        # we can run multiple select statements, but we get no result
        res = duckdb.query('select 42; select 84;');
        assert res is None

    def test_duckdb_from_query(self, duckdb_cursor):
        # duckdb.from_query cannot be used to run arbitrary queries
        with pytest.raises(Exception) as e_info:
            duckdb.from_query('create view v1 as select 42 i')
        assert 'duckdb.query' in str(e_info.value)
        # ... or multiple select statements
        with pytest.raises(Exception) as e_info:
            duckdb.from_query('select 42; select 84;')
        assert 'duckdb.query' in str(e_info.value)
