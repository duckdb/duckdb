import duckdb
import os
import pytest

pa = pytest.importorskip("pyarrow")
pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")


def using_table(con, to_scan, object_name):
    local_scope = {'con': con, object_name: to_scan, 'object_name': object_name}
    exec(f"result = con.table(object_name)", globals(), local_scope)
    return local_scope["result"]


def using_sql(con, to_scan, object_name):
    local_scope = {'con': con, object_name: to_scan, 'object_name': object_name}
    exec(f"result = con.sql('select * from \"{object_name}\"')", globals(), local_scope)
    return local_scope["result"]


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


global_polars_df = pl.DataFrame(
    {
        "A": [1],
        "fruits": ["banana"],
        "B": [5],
        "cars": ["beetle"],
    }
)


def from_pandas():
    df = pd.DataFrame({'a': [1, 2, 3]})
    return df


def from_arrow():
    schema = pa.schema([('field_1', pa.int64())])
    df = pa.RecordBatchReader.from_batches(schema, [pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)])
    return df


def create_relation(conn, query: str) -> duckdb.DuckDBPyRelation:
    df = pd.DataFrame({'a': [1, 2, 3]})
    return conn.sql(query)


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

    def test_scan_global(self, duckdb_cursor):
        duckdb_cursor.execute("set python_enable_replacements=false")
        with pytest.raises(duckdb.CatalogException, match='Table with name global_polars_df does not exist'):
            # We set the depth to look for global variables to 0 so it's never found
            duckdb_cursor.sql("select * from global_polars_df")
        duckdb_cursor.execute("set python_enable_replacements=true")
        # Now the depth is 1, which is enough to locate the variable
        rel = duckdb_cursor.sql("select * from global_polars_df")
        res = rel.fetchone()
        assert res == (1, 'banana', 5, 'beetle')

    def test_scan_local(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3]})

        def inner_func(duckdb_cursor):
            duckdb_cursor.execute("set python_enable_replacements=false")
            with pytest.raises(duckdb.CatalogException, match='Table with name df does not exist'):
                # We set the depth to look for local variables to 0 so it's never found
                duckdb_cursor.sql("select * from df")
            duckdb_cursor.execute("set python_enable_replacements=true")
            with pytest.raises(duckdb.CatalogException, match='Table with name df does not exist'):
                # Here it's still not found, because it's not visible to this frame
                duckdb_cursor.sql("select * from df")

            df = pd.DataFrame({'a': [4, 5, 6]})
            duckdb_cursor.execute("set python_enable_replacements=true")
            # We can find the newly defined 'df' with depth 1
            rel = duckdb_cursor.sql("select * from df")
            res = rel.fetchall()
            assert res == [(4,), (5,), (6,)]

        inner_func(duckdb_cursor)

    def test_scan_local_unlimited(self, duckdb_cursor):
        df = pd.DataFrame({'a': [1, 2, 3]})

        def inner_func(duckdb_cursor):
            duckdb_cursor.execute("set python_enable_replacements=true")
            with pytest.raises(duckdb.CatalogException, match='Table with name df does not exist'):
                # We set the depth to look for local variables to 1 so it's still not found because it wasn't defined in this function
                duckdb_cursor.sql("select * from df")
            duckdb_cursor.execute("set python_scan_all_frames=true")
            # Now we can find 'df' because we also scan the previous frame(s)
            rel = duckdb_cursor.sql("select * from df")

            res = rel.fetchall()
            assert res == [(1,), (2,), (3,)]

        inner_func(duckdb_cursor)

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

    def test_replacement_scan_not_found(self):
        con = duckdb.connect()
        con.execute("set python_scan_all_frames=true")
        with pytest.raises(duckdb.CatalogException, match='Table with name non_existant does not exist'):
            res = con.sql("select * from non_existant").fetchall()

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
        duckdb_cursor.execute("insert into df values (4), (5), (6)")
        rel = duckdb_cursor.sql("select * from df")

        duckdb_cursor.execute("drop table df")
        df = pd.DataFrame({'b': [1, 2, 3]})
        res = rel.fetchall()
        # FIXME: this should error instead, the 'df' table we relied on has been removed and replaced with a replacement scan
        assert res == [(1,), (2,), (3,)]

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

    @pytest.mark.parametrize(
        'df_create',
        [
            from_pandas,
            from_arrow,
        ],
    )
    def test_cte(self, duckdb_cursor, df_create):
        df = df_create()
        rel = duckdb_cursor.sql("with cte as (select * from df) select * from cte")
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

        df = df_create()
        query = """
            WITH cte as (select * from df)
            select * from (
                WITH cte as (select * from df)
                select * from (
                    WITH cte as (select * from df)
                    select array(
                        select * from cte
                    ) from cte
                )
            )
        """
        rel = duckdb_cursor.sql(query)
        duckdb_cursor.execute("create table df as select * from range(4, 7)")
        res = rel.fetchall()
        """
        select (
            select * from cte
        ) from cte
        This will select the first row from the cte, and do this 3 times since we added 'from cte', and cte has 3 tuples
        """

        if df_create == from_arrow:
            # Because the RecordBatchReader is destructive, it's empty after the first scan
            # But we reference it multiple times, so the subsequent reads have no data to read
            # FIXME: this should probably throw an error...
            assert len(res) >= 0
        else:
            assert res == [([1, 2, 3],), ([1, 2, 3],), ([1, 2, 3],)]

    def test_cte_with_scalar_subquery(self, duckdb_cursor):
        query = """
            WITH cte1 AS (
                select array(select * from df)
            )
            select * from cte1;
        """
        rel = create_relation(duckdb_cursor, query)
        res = rel.fetchall()
        assert res == [([1, 2, 3],)]

    def test_cte_with_joins(self, duckdb_cursor):
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
        rel = create_relation(duckdb_cursor, query)
        duckdb_cursor.execute("create table df as select * from range(4, 7)")
        res = rel.fetchall()
        assert res == [(2, 2, 2)]

    def test_same_name_cte(self, duckdb_cursor):
        query = """
            WITH df AS (
                SELECT a+1 FROM df
            )
            SELECT * FROM df;
        """
        rel = create_relation(duckdb_cursor, query)
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

        query = """
            WITH RECURSIVE df AS (
                SELECT a+1 FROM df
            )
            SELECT * FROM df;
        """
        rel = create_relation(duckdb_cursor, query)
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

    def test_use_with_view(self, duckdb_cursor):
        rel = create_relation(duckdb_cursor, "select * from df")
        rel.create_view('v1')

        del rel
        rel = duckdb_cursor.sql("select * from v1")
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]
        duckdb_cursor.execute("drop view v1")

        def create_view_in_func(con):
            df = pd.DataFrame({"a": [1, 2, 3]})
            con.execute('CREATE VIEW v1 AS SELECT * FROM df')

        create_view_in_func(duckdb_cursor)

        # FIXME: this should be fixed in the future, likely by unifying the behavior of .sql and .execute
        with pytest.raises(duckdb.CatalogException, match='Table with name df does not exist'):
            rel = duckdb_cursor.sql("select * from v1")

    def test_recursive_cte(self, duckdb_cursor):
        query = """
            WITH RECURSIVE
            RecursiveCTE AS (
            SELECT Number from df t(Number)
            UNION ALL
            SELECT Number + (select a from df offset 2 limit 1) + 1 as new
            FROM RecursiveCTE
            WHERE new < 10
            )
            select * from RecursiveCTE;
        """
        rel = create_relation(duckdb_cursor, query)
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,), (5,), (6,), (7,), (9,)]

        # RecursiveCTE references another CTE which references the 'df'
        query = """
            WITH RECURSIVE
            other_cte as (
                select * from df t(c)
            ),
            RecursiveCTE AS (
            SELECT Number from other_cte t(Number)
            UNION ALL
            SELECT Number + (select c from other_cte offset 2 limit 1) + 1 as new
            FROM RecursiveCTE
            WHERE new < 10
            )
            select * from RecursiveCTE;
        """
        rel = create_relation(duckdb_cursor, query)
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,), (5,), (6,), (7,), (9,)]

    def test_multiple_replacements(self, duckdb_cursor):
        # Sample data for Employees table
        employees_data = [
            {"EmployeeID": 1, "EmployeeName": "Alice", "ManagerID": None},
            {"EmployeeID": 2, "EmployeeName": "Bob", "ManagerID": 1},
            {"EmployeeID": 3, "EmployeeName": "Charlie", "ManagerID": 1},
            {"EmployeeID": 4, "EmployeeName": "David", "ManagerID": 2},
            {"EmployeeID": 5, "EmployeeName": "Eve", "ManagerID": 2},
        ]

        # Convert list of dictionaries to pandas DataFrame
        employees_df = pd.DataFrame(employees_data)
        # First mention of `employees_df` has an alias, second doesn't
        query = """
            SELECT
                e1.EmployeeID,
                e1.EmployeeName,
                employees_df.ManagerID
            FROM employees_df e1
            JOIN employees_df ON e1.ManagerID = employees_df.EmployeeID
            ORDER BY ALL;
        """
        rel = duckdb_cursor.sql(query)
        res = rel.fetchall()
        assert res == [(2, 'Bob', None), (3, 'Charlie', None), (4, 'David', 1.0), (5, 'Eve', 1.0)]

    def test_cte_at_different_levels(self, duckdb_cursor):
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
        rel = create_relation(duckdb_cursor, query)
        duckdb_cursor.execute("create table df as select * from range(4, 7)")
        res = rel.fetchall()
        assert res == [(2, 2, 2)]

    def test_replacement_disabled(self):
        # Create regular connection, not disabled
        con = duckdb.connect()
        rel = create_relation(con, "select * from df")
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

        ## disable external access
        con.execute("set enable_external_access=false")
        with pytest.raises(duckdb.CatalogException, match='Table with name df does not exist!'):
            rel = create_relation(con, "select * from df")
            res = rel.fetchall()
        with pytest.raises(
            duckdb.InvalidInputException, match='Cannot change enable_external_access setting while database is running'
        ):
            con.execute("set enable_external_access=true")

        # Create connection with external access disabled
        con = duckdb.connect(config={'enable_external_access': False})
        with pytest.raises(duckdb.CatalogException, match='Table with name df does not exist!'):
            rel = create_relation(con, "select * from df")
            res = rel.fetchall()

        # Create regular connection, disable inbetween creation and execution
        con = duckdb.connect()
        rel = create_relation(con, "select * from df")

        con.execute("set enable_external_access=false")

        # Since we cache the replacement scans as CTEs, disabling the external access inbetween creation
        # and execution has no effect, we might want to change that by keeping track of which CTEs we have added
        # and removing them if `enable_external_access` is set
        res = rel.fetchall()
        assert res == [(1,), (2,), (3,)]

    def test_replacement_of_cross_connection_relation(self):
        con1 = duckdb.connect(':memory:')
        con2 = duckdb.connect(':memory:')
        con1.query('create table integers(i int)')
        con2.query('create table integers(v varchar)')
        con1.query('insert into integers values (42)')
        con2.query('insert into integers values (\'xxx\')')
        rel1 = con1.query('select * from integers')
        with pytest.raises(
            duckdb.InvalidInputException,
            match=r'The object was created by another Connection and can therefore not be used by this Connection.',
        ):
            con2.query('from rel1')

        del con1

        with pytest.raises(
            duckdb.InvalidInputException,
            match=r'The object was created by another Connection and can therefore not be used by this Connection.',
        ):
            con2.query('from rel1')
