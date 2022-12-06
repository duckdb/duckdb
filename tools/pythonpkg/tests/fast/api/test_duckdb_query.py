
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
        with pytest.raises(duckdb.ParserException, match='duckdb.from_query cannot be used to run arbitrary SQL queries'):
            duckdb.from_query('create view v1 as select 42 i')
        # ... or multiple select statements
        with pytest.raises(duckdb.ParserException, match='duckdb.from_query cannot be used to run arbitrary SQL queries'):
            duckdb.from_query('select 42; select 84;')

    def test_named_param(self):
        con = duckdb.connect()

        original_res = con.execute(
        """
            select
                count(*) FILTER (WHERE i >= $1),
                sum(i) FILTER (WHERE i < $2),
                avg(i) FILTER (WHERE i < $1)
            from
                range(100) tbl(i)
        """,
        [5, 10]
        ).fetchall()

        res = con.execute(
        """
            select
                count(*) FILTER (WHERE i >= $param),
                sum(i) FILTER (WHERE i < $other_param),
                avg(i) FILTER (WHERE i < $param)
            from
                range(100) tbl(i)
        """,
        {
            'param': 5,
            'other_param': 10
        }
        ).fetchall()

        assert(res == original_res)

    def test_named_param_not_dict(self):
        con = duckdb.connect()

        with pytest.raises(duckdb.InvalidInputException, match="Named parameters found, but param is not of type 'dict'"):
            con.execute("select $name1, $name2, $name3", ['name1', 'name2', 'name3'])

    def test_named_param_basic(self):
        con = duckdb.connect()

        res = con.execute("select $name1, $name2, $name3", {'name1': 5, 'name2': 3, 'name3': 'a'}).fetchall()
        assert res == [(5,3,'a'),]

    def test_named_param_not_exhaustive(self):
        con = duckdb.connect()

        with pytest.raises(duckdb.InvalidInputException, match="Not all named parameters have been located, missing: name3"):
            con.execute("select $name1, $name2, $name3", {'name1': 5, 'name2': 3})

    def test_named_param_excessive(self):
        con = duckdb.connect()

        with pytest.raises(duckdb.InvalidInputException, match="Named parameters could not be transformed, because query string is missing named parameter 'not_a_named_param'"):
            con.execute("select $name1, $name2, $name3", {'name1': 5, 'name2': 3, 'not_a_named_param': 5})

    def test_named_param_not_named(self):
        con = duckdb.connect()

        with pytest.raises(duckdb.InvalidInputException, match="Invalid Input Error: Param is of type 'dict', but no named parameters were found in the query"):
            con.execute("select $1, $1, $2", {'name1': 5, 'name2': 3})

    def test_named_param_mixed(self):
        con = duckdb.connect()

        with pytest.raises(duckdb.NotImplementedException, match="Mixing positional and named parameters is not supported yet"):
            con.execute("select $name1, $1, $2", {'name1': 5, 'name2': 3})

    def test_named_param_strings_with_dollarsign(self):
        con = duckdb.connect()

        res = con.execute("select '$name1', $name1, $name1, '$name1'", {'name1': 5}).fetchall()
        assert res == [('$name1', 5, 5, '$name1')]
