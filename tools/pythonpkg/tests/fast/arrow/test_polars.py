import duckdb
import pytest
import sys

pl = pytest.importorskip("polars")
arrow = pytest.importorskip("pyarrow")
pl_testing = pytest.importorskip("polars.testing")


class TestPolars(object):
    def test_polars(self, duckdb_cursor):
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        # scan plus return a polars dataframe
        polars_result = duckdb_cursor.sql('SELECT * FROM df').pl()
        pl_testing.assert_frame_equal(df, polars_result)

        # now do the same for a lazy dataframe
        lazy_df = df.lazy()
        lazy_result = duckdb_cursor.sql('SELECT * FROM lazy_df').pl()
        pl_testing.assert_frame_equal(df, lazy_result)

        con = duckdb.connect()
        con_result = con.execute('SELECT * FROM df').pl()
        pl_testing.assert_frame_equal(df, con_result)

    def test_execute_polars(self, duckdb_cursor):
        res1 = duckdb_cursor.execute("SELECT 1 AS a, 2 AS a").pl()
        assert res1.columns == ['a', 'a_1']

    def test_register_polars(self, duckdb_cursor):
        con = duckdb.connect()
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        # scan plus return a polars dataframe
        con.register('polars_df', df)
        polars_result = con.execute('select * from polars_df').pl()
        pl_testing.assert_frame_equal(df, polars_result)
        con.unregister('polars_df')
        with pytest.raises(duckdb.CatalogException, match='Table with name polars_df does not exist'):
            con.execute("SELECT * FROM polars_df;").pl()

        con.register('polars_df', df.lazy())
        polars_result = con.execute('select * from polars_df').pl()
        pl_testing.assert_frame_equal(df, polars_result)

    def test_empty_polars_dataframe(self, duckdb_cursor):
        polars_empty_df = pl.DataFrame()
        with pytest.raises(
            duckdb.InvalidInputException, match='Provided table/dataframe must have at least one column'
        ):
            duckdb_cursor.sql("from polars_empty_df")

    def test_polars_from_json(self, duckdb_cursor):
        from io import StringIO

        duckdb_cursor.sql("set arrow_lossless_conversion=false")
        string = StringIO("""{"entry":[{"content":{"ManagedSystem":{"test":null}}}]}""")
        res = duckdb_cursor.read_json(string).pl()
        assert str(res['entry'][0][0]) == "{'content': {'ManagedSystem': {'test': None}}}"

    @pytest.mark.skipif(
        not hasattr(pl.exceptions, "PanicException"), reason="Polars has no PanicException in this version"
    )
    def test_polars_from_json_error(self, duckdb_cursor):
        from io import StringIO

        duckdb_cursor.sql("set arrow_lossless_conversion=true")
        string = StringIO("""{"entry":[{"content":{"ManagedSystem":{"test":null}}}]}""")
        res = duckdb_cursor.read_json(string).pl()
        assert duckdb_cursor.execute("FROM res").fetchall() == [([{'content': {'ManagedSystem': {'test': None}}}],)]

    def test_polars_from_json_error(self, duckdb_cursor):
        conn = duckdb.connect()
        my_table = conn.query("select 'x' my_str").pl()
        my_res = duckdb.query("select my_str from my_table where my_str != 'y'")
        assert my_res.fetchall() == [('x',)]

    def test_polars_lazy(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("Create table names (a varchar, b integer)")
        con.execute("insert into names values ('Pedro',32),  ('Mark',31), ('Thijs', 29)")
        rel = con.sql("FROM names")
        lazy_df = rel.pl(lazy=True)

        assert isinstance(lazy_df, pl.LazyFrame)
        assert lazy_df.collect().to_dicts() == [
            {'a': 'Pedro', 'b': 32},
            {'a': 'Mark', 'b': 31},
            {'a': 'Thijs', 'b': 29},
        ]

        assert lazy_df.select('a').collect().to_dicts() == [{'a': 'Pedro'}, {'a': 'Mark'}, {'a': 'Thijs'}]
        assert lazy_df.limit(1).collect().to_dicts() == [{'a': 'Pedro', 'b': 32}]
        assert lazy_df.filter(pl.col("b") < 32).collect().to_dicts() == [
            {'a': 'Mark', 'b': 31},
            {'a': 'Thijs', 'b': 29},
        ]
        assert lazy_df.filter(pl.col("b") < 32).select('a').collect().to_dicts() == [{'a': 'Mark'}, {'a': 'Thijs'}]

    @pytest.mark.parametrize(
        'data_type',
        [
            'TINYINT',
            'SMALLINT',
            'INTEGER',
            'BIGINT',
            'UTINYINT',
            'USMALLINT',
            'UINTEGER',
            'UBIGINT',
            'FLOAT',
            'DOUBLE',
            'HUGEINT',
            'DECIMAL(4,1)',
            'DECIMAL(9,1)',
            'DECIMAL(18,4)',
            'DECIMAL(30,12)',
        ],
    )
    def test_polars_lazy_pushdown_numeric(self, data_type, duckdb_cursor):
        con = duckdb.connect()
        tbl_name = "test"
        con.execute(
            f"""
        CREATE TABLE {tbl_name} (
            a {data_type},
            b {data_type},
            c {data_type}
        )
        """
        )
        con.execute(
            f"""
            INSERT INTO {tbl_name} VALUES
                (1,1,1),
                (10,10,10),
                (100,10,100),
                (NULL,NULL,NULL)
        """
        )
        rel = con.sql(f"FROM {tbl_name}")
        lazy_df = rel.pl(lazy=True)

        # Equality
        assert lazy_df.filter(pl.col("a") == 1).select("a").collect().to_dicts() == [{"a": 1}]
        # Greater than
        assert lazy_df.filter(pl.col("a") > 1).select("a").collect().to_dicts() == [{"a": 10}, {"a": 100}]
        # Greater than or equal
        assert lazy_df.filter(pl.col("a") >= 10).select("a").collect().to_dicts() == [{"a": 10}, {"a": 100}]
        # Less than
        assert lazy_df.filter(pl.col("a") < 10).select("a").collect().to_dicts() == [{"a": 1}]
        # Less than or equal
        assert lazy_df.filter(pl.col("a") <= 10).select("a").collect().to_dicts() == [{"a": 1}, {"a": 10}]

        # IS NULL
        assert lazy_df.filter(pl.col("a").is_null()).select("a").collect().to_dicts() == [{"a": None}]
        # IS NOT NULL
        assert lazy_df.filter(pl.col("a").is_not_null()).select("a").collect().to_dicts() == [
            {"a": 1},
            {"a": 10},
            {"a": 100},
        ]

        # AND
        assert lazy_df.filter((pl.col("a") == 10) & (pl.col("b") == 1)).collect().to_dicts() == []
        assert lazy_df.filter(
            (pl.col("a") == 100) & (pl.col("b") == 10) & (pl.col("c") == 100)
        ).collect().to_dicts() == [{"a": 100, "b": 10, "c": 100}]

        # OR
        assert lazy_df.filter((pl.col("a") == 100) | (pl.col("b") == 1)).select("a", "b").collect().to_dicts() == [
            {"a": 1, "b": 1},
            {"a": 100, "b": 10},
        ]
    
    def test_polars_lazy_pushdown_bool(self, duckdb_cursor):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_bool (
                a BOOL,
                b BOOL
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_bool VALUES
                (TRUE,TRUE),
                (TRUE,FALSE),
                (FALSE,TRUE),
                (NULL,NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test_bool")
        
        lazy_df = duck_tbl.pl(lazy=True)
        # == True
        assert lazy_df.filter(pl.col("a") == True).select(pl.len()).collect().item() == 2

        # IS NULL
        assert lazy_df.filter(pl.col("a").is_null()).select(pl.len()).collect().item() == 1

        # IS NOT NULL
        assert lazy_df.filter(pl.col("a").is_not_null()).select(pl.len()).collect().item() == 3

        # AND
        assert lazy_df.filter((pl.col("a") == True) & (pl.col("b") == True)).select(pl.len()).collect().item() == 1

        # OR
        assert lazy_df.filter((pl.col("a") == True) | (pl.col("b") == True)).select(pl.len()).collect().item() == 3