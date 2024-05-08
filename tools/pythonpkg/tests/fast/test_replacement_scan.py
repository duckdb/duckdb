import duckdb
import os
import pytest

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")


def using_table(con, to_scan, object_name):
    exec(f"{object_name} = to_scan")
    return con.table(object_name)


def using_sql(con, to_scan, object_name):
    exec(f"{object_name} = to_scan")
    return con.sql(f"select * from to_scan")


# Fetch methods


def fetch_polars(rel):
    return rel.pl()


def fetch_df(rel):
    return rel.df()


def fetch_arrow(rel):
    return rel.arrow()


def fetch_arrow_table(rel):
    return rel.fetch_arrow_table()


def fetch_arrow_record_batch(rel):
    # Note: this has to executed first, otherwise we'll create a deadlock
    # Because it will try to execute the input at the same time as executing the relation
    # On the same connection (that's the core of the issue)
    return rel.execute().record_batch()


def fetch_relation(rel):
    return rel


class TestReplacementScan(object):
    def test_csv_replacement(self):
        con = duckdb.connect()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'integers.csv')
        res = con.execute("select count(*) from '%s'" % (filename))
        assert res.fetchone()[0] == 2

    def test_parquet_replacement(self):
        con = duckdb.connect()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'binary_string.parquet')
        res = con.execute("select count(*) from '%s'" % (filename))
        assert res.fetchone()[0] == 3

    @pytest.mark.parametrize('get_relation', [using_table, using_sql])
    @pytest.mark.parametrize(
        'fetch_method',
        [fetch_polars, fetch_df, fetch_arrow, fetch_arrow_table, fetch_arrow_record_batch, fetch_relation],
    )
    @pytest.mark.parametrize('object_name', ['tbl', 'table', 'select', 'update'])
    def test_table_replacement_scans(self, duckdb_cursor, get_relation, fetch_method, object_name):
        base_rel = duckdb_cursor.values([1, 2, 3])
        to_scan = fetch_method(base_rel)
        exec(f"{object_name} = to_scan")

        rel = get_relation(duckdb_cursor, to_scan, object_name)
        res = rel.fetchall()
        assert res == [(1, 2, 3)]

    def test_replacement_scan_relapi(self):
        con = duckdb.connect()
        pyrel1 = con.query('from (values (42), (84), (120)) t(i)')
        assert isinstance(pyrel1, duckdb.DuckDBPyRelation)
        assert pyrel1.fetchall() == [(42,), (84,), (120,)]

        pyrel2 = con.query('from pyrel1 limit 2')
        assert isinstance(pyrel2, duckdb.DuckDBPyRelation)
        assert pyrel2.fetchall() == [(42,), (84,)]

        pyrel3 = con.query('select i + 100 from pyrel2')
        assert type(pyrel3) == duckdb.DuckDBPyRelation
        assert pyrel3.fetchall() == [(142,), (184,)]

    def test_replacement_scan_alias(self):
        con = duckdb.connect()
        pyrel1 = con.query('from (values (1, 2)) t(i, j)')
        pyrel2 = con.query('from (values (1, 10)) t(i, k)')
        pyrel3 = con.query('from pyrel1 join pyrel2 using(i)')
        assert type(pyrel3) == duckdb.DuckDBPyRelation
        assert pyrel3.fetchall() == [(1, 2, 10)]

    def test_replacement_scan_pandas_alias(self):
        con = duckdb.connect()
        df1 = con.query('from (values (1, 2)) t(i, j)').df()
        df2 = con.query('from (values (1, 10)) t(i, k)').df()
        df3 = con.query('from df1 join df2 using(i)')
        assert df3.fetchall() == [(1, 2, 10)]

    def test_replacement_scan_after_creation(self, duckdb_cursor):
        duckdb_cursor.execute("create table df (a varchar)")
        rel = duckdb_cursor.sql("select * from df")

        duckdb_cursor.execute("drop table df")
        df = pd.DataFrame({'b': [1, 2, 3]})
        with pytest.raises(
            duckdb.InvalidInputException,
            match=r'Tables or Views were removed inbetween creation and execution of this relation!',
        ):
            res = rel.fetchall()

    def test_replacement_scan_caching(self, duckdb_cursor):
        def return_rel(conn):
            df = pd.DataFrame({'a': [1, 2, 3]})
            rel = conn.sql("select * from df")
            return rel

        rel = return_rel(duckdb_cursor)
        duckdb_cursor.execute("create table df as select * from unnest([4,5,6])")
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

    def test_replacement_scan_fail(self):
        random_object = "I love salmiak rondos"
        con = duckdb.connect()
        with pytest.raises(
            duckdb.InvalidInputException,
            match=r'Python Object "random_object" of type "str" found on line .* not suitable for replacement scans.',
        ):
            con.execute("select count(*) from random_object").fetchone()

    def test_cte(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3]})
        rel = duckdb_cursor.sql("with cte as (select * from df) select * from cte")
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

        query = """
            WITH cte as (select * from df)
            select * from (
                WITH cte as (select * from df)
                select * from (
                    WITH cte as (select * from df)
                    select (
                        select * from cte
                    ) from cte
                )
            )
        """
        rel = duckdb_cursor.sql(query)
        res = rel.fetchall()
        """
        select (
            select * from cte
        ) from cte
        This will select the first row from the cte, and do this 3 times since we added 'from cte', and cte has 3 tuples
        """

        assert res == [(1,), (1,), (1,)]

    def test_cte_with_joins(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3]})
        query = """
            WITH cte1 AS (
                SELECT * FROM df
            ),
            cte2 AS (
                SELECT * FROM df
                WHERE a > 1
            ),
            cte3 AS (
                SELECT * FROM df
                WHERE a < 3
            )
            SELECT * FROM (
                SELECT 
                    cte1.*, 
                    cte2.a AS cte2_a,
                    subquery.a AS cte3_a
                FROM cte1
                JOIN cte2 ON cte1.a = cte2.a
                JOIN (
                    SELECT 
                        df.*, 
                        cte3.a AS cte3_a
                    FROM df
                    JOIN cte3 ON df.a = cte3.a
                ) AS subquery ON cte1.a = subquery.a
            ) AS main_query
            WHERE main_query.a = 2
        """
        rel = duckdb_cursor.sql(query)
        res = rel.fetchall()
        assert res == [(2, 2, 2)]

    def test_cte_at_different_levels(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3]})
        query = """
            SELECT * FROM (
                WITH cte1 AS (
                    SELECT * FROM df
                )
                SELECT 
                    cte1.*, 
                    cte2.a AS cte2_a,
                    subquery.a AS cte3_a
                FROM cte1
                JOIN (
                    WITH cte2 AS (
                        SELECT * FROM df
                        WHERE a > 1
                    )
                    SELECT * FROM cte2
                ) AS cte2 ON cte1.a = cte2.a
                JOIN (
                    WITH cte3 AS (
                        SELECT * FROM df
                        WHERE a < 3
                    )
                    SELECT 
                        df.*, 
                        cte3.a AS cte3_a
                    FROM (
                        SELECT * FROM df
                    ) AS df
                    JOIN cte3 ON df.a = cte3.a
                ) AS subquery ON cte1.a = subquery.a
            ) AS main_query
            WHERE main_query.a = 2
        """
        rel = duckdb_cursor.sql(query)
        res = rel.fetchall()
        assert res == [(2, 2, 2)]
