from re import S
import duckdb
import os
import pytest
import tempfile
from conftest import pandas_supports_arrow_backend

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
ds = pytest.importorskip("pyarrow.dataset")
np = pytest.importorskip("numpy")
re = pytest.importorskip("re")


def create_pyarrow_pandas(rel):
    if not pandas_supports_arrow_backend():
        pytest.skip(reason="Pandas version doesn't support 'pyarrow' backend")
    return rel.df().convert_dtypes(dtype_backend='pyarrow')


def create_pyarrow_table(rel):
    return rel.arrow()


def create_pyarrow_dataset(rel):
    table = create_pyarrow_table(rel)
    return ds.dataset(table)


def numeric_operators(connection, data_type, tbl_name, create_table):
    connection.execute(
        f"""
        CREATE TABLE {tbl_name} (
            a {data_type},
            b {data_type},
            c {data_type}
        )
    """
    )
    connection.execute(
        f"""
        INSERT INTO {tbl_name} VALUES
            (1,1,1),
            (10,10,10),
            (100,10,100),
            (NULL,NULL,NULL)
    """
    )
    duck_tbl = connection.table(tbl_name)
    arrow_table = create_table(duck_tbl)

    # Try ==
    assert connection.execute("SELECT count(*) from arrow_table where a = 1").fetchone()[0] == 1
    # Try >
    assert connection.execute("SELECT count(*) from arrow_table where a > 1").fetchone()[0] == 2
    # Try >=
    assert connection.execute("SELECT count(*) from arrow_table where a >= 10").fetchone()[0] == 2
    # Try <
    assert connection.execute("SELECT count(*) from arrow_table where a < 10").fetchone()[0] == 1
    # Try <=
    assert connection.execute("SELECT count(*) from arrow_table where a <= 10").fetchone()[0] == 2

    # Try Is Null
    assert connection.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
    # Try Is Not Null
    assert connection.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

    # Try And
    assert connection.execute("SELECT count(*) from arrow_table where a = 10 and b = 1").fetchone()[0] == 0
    assert (
        connection.execute("SELECT count(*) from arrow_table where a = 100 and b = 10 and c = 100").fetchone()[0] == 1
    )

    # Try Or
    assert connection.execute("SELECT count(*) from arrow_table where a = 100 or b = 1").fetchone()[0] == 2

    connection.execute("EXPLAIN SELECT count(*) from arrow_table where a = 100 or b = 1")
    print(connection.fetchall())


def numeric_check_or_pushdown(connection, tbl_name, create_table):
    duck_tbl = connection.table(tbl_name)
    arrow_table = create_table(duck_tbl)

    # Multiple column in the root OR node, don't push down
    query_res = connection.execute(
        """
        EXPLAIN SELECT * FROM arrow_table WHERE
            a = 1 OR b = 2 AND (a > 3 OR b < 5)
    """
    ).fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match

    # Single column in the root OR node
    query_res = connection.execute(
        """
        EXPLAIN SELECT * FROM arrow_table WHERE
            a = 1 OR a = 10
    """
    ).fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a=1 OR a=10.*|$", query_res[0][1])
    assert match

    # Single column + root OR node with AND
    query_res = connection.execute(
        """
        EXPLAIN SELECT * FROM arrow_table
            WHERE a = 1 OR (a > 3 AND a < 5)
    """
    ).fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a=1 OR a>3 AND a<5.*|$", query_res[0][1])
    assert match

    # Single column multiple ORs
    query_res = connection.execute("EXPLAIN SELECT * FROM arrow_table WHERE a=1 OR a>3 OR a<5").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a=1 OR a>3 OR a<5.*|$", query_res[0][1])
    assert match

    # Testing not equal
    query_res = connection.execute("EXPLAIN SELECT * FROM arrow_table WHERE a!=1 OR a>3 OR a<2").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a!=1 OR a>3 OR a<2.*|$", query_res[0][1])
    assert match

    # Multiple OR filters connected with ANDs
    query_res = connection.execute(
        "EXPLAIN SELECT * FROM arrow_table WHERE (a<2 OR a>3) AND (a=1 OR a=4) AND (b=1 OR b<5)"
    ).fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a<2 OR a>3 AND a=1|\n.*OR a=4.*\n.*b=2 OR b<5.*|$", query_res[0][1])
    assert match


def string_check_or_pushdown(connection, tbl_name, create_table):
    duck_tbl = connection.table(tbl_name)
    arrow_table = create_table(duck_tbl)

    # Check string zonemap
    query_res = connection.execute("EXPLAIN SELECT * FROM arrow_table WHERE a >= '1' OR a <= '10'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters: a>=1 OR a<=10.*|$", query_res[0][1])
    assert match

    # No support for OR with is null
    query_res = connection.execute("EXPLAIN SELECT * FROM arrow_table WHERE a IS NULL or a = '1'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match

    # No support for OR with is not null
    query_res = connection.execute("EXPLAIN SELECT * FROM arrow_table WHERE a IS NOT NULL OR a = '1'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match

    # OR with the like operator
    query_res = connection.execute("EXPLAIN SELECT * FROM arrow_table WHERE a = 1 OR a LIKE '10%'").fetchall()
    match = re.search(".*ARROW_SCAN.*Filters:.*", query_res[0][1])
    assert not match


class TestArrowFilterPushdown(object):
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
    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_numeric(self, data_type, duckdb_cursor, create_table):
        tbl_name = "tbl"
        numeric_operators(duckdb_cursor, data_type, tbl_name, create_table)
        numeric_check_or_pushdown(duckdb_cursor, tbl_name, create_table)

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_varchar(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_varchar (
                a VARCHAR,
                b VARCHAR,
                c VARCHAR
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_varchar VALUES
                ('1','1','1'),
                ('10','10','10'),
                ('100','10','100'),
                (NULL, NULL, NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test_varchar")
        arrow_table = create_table(duck_tbl)

        # Try ==
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '1'").fetchone()[0] == 1
        # Try >
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a > '1'").fetchone()[0] == 2
        # Try >=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a >= '10'").fetchone()[0] == 2
        # Try <
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a < '10'").fetchone()[0] == 1
        # Try <=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a <= '10'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '10' and b = '1'").fetchone()[0] == 0
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '100' and b = '10' and c = '100'"
            ).fetchone()[0]
            == 1
        )
        # Try Or
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '100' or b ='1'").fetchone()[0] == 2

        # More complex tests for OR pushed down on string
        string_check_or_pushdown(duckdb_cursor, "test_varchar", create_table)

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_bool(self, duckdb_cursor, create_table):
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
        arrow_table = create_table(duck_tbl)

        # Try ==
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = True").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a= True and b = True").fetchone()[0] == 1
        # Try Or
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = True or b = True").fetchone()[0] == 3

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_time(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_time (
                a TIME,
                b TIME,
                c TIME
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_time VALUES
                ('00:01:00','00:01:00','00:01:00'),
                ('00:10:00','00:10:00','00:10:00'),
                ('01:00:00','00:10:00','01:00:00'),
                (NULL,NULL,NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test_time")
        arrow_table = create_table(duck_tbl)

        # Try ==
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a ='00:01:00'").fetchone()[0] == 1
        # Try >
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a >'00:01:00'").fetchone()[0] == 2
        # Try >=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a >='00:10:00'").fetchone()[0] == 2
        # Try <
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a <'00:10:00'").fetchone()[0] == 1
        # Try <=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a <='00:10:00'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a='00:10:00' and b ='00:01:00'").fetchone()[0]
            == 0
        )
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a ='01:00:00' and b = '00:10:00' and c = '01:00:00'"
            ).fetchone()[0]
            == 1
        )
        # Try Or
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '01:00:00' or b ='00:01:00'").fetchone()[
                0
            ]
            == 2
        )

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_timestamp(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_timestamp (
                a TIMESTAMP,
                b TIMESTAMP,
                c TIMESTAMP
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_timestamp VALUES
                ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),
                ('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),
                ('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),
                (NULL,NULL,NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test_timestamp")
        arrow_table = create_table(duck_tbl)

        # Try ==
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a ='2008-01-01 00:00:01'").fetchone()[0] == 1
        )
        # Try >
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a >'2008-01-01 00:00:01'").fetchone()[0] == 2
        )
        # Try >=
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a >='2010-01-01 10:00:01'").fetchone()[0] == 2
        )
        # Try <
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a <'2010-01-01 10:00:01'").fetchone()[0] == 1
        )
        # Try <=
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a <='2010-01-01 10:00:01'").fetchone()[0] == 2
        )

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a='2010-01-01 10:00:01' and b ='2008-01-01 00:00:01'"
            ).fetchone()[0]
            == 0
        )
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a ='2020-03-01 10:00:01' and b = '2010-01-01 10:00:01' and c = '2020-03-01 10:00:01'"
            ).fetchone()[0]
            == 1
        )
        # Try Or
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2020-03-01 10:00:01' or b ='2008-01-01 00:00:01'"
            ).fetchone()[0]
            == 2
        )

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_timestamp_TZ(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_timestamptz (
                a TIMESTAMPTZ,
                b TIMESTAMPTZ,
                c TIMESTAMPTZ
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_timestamptz VALUES
                ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),
                ('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),
                ('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),
                (NULL,NULL,NULL)
            """
        )
        duck_tbl = duckdb_cursor.table("test_timestamptz")
        arrow_table = create_table(duck_tbl)

        # Try ==
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '2008-01-01 00:00:01'").fetchone()[0] == 1
        )
        # Try >
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a > '2008-01-01 00:00:01'").fetchone()[0] == 2
        )
        # Try >=
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a >= '2010-01-01 10:00:01'").fetchone()[0]
            == 2
        )
        # Try <
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a < '2010-01-01 10:00:01'").fetchone()[0] == 1
        )
        # Try <=
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a <= '2010-01-01 10:00:01'").fetchone()[0]
            == 2
        )

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2010-01-01 10:00:01' and b = '2008-01-01 00:00:01'"
            ).fetchone()[0]
            == 0
        )
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2020-03-01 10:00:01' and b = '2010-01-01 10:00:01' and c = '2020-03-01 10:00:01'"
            ).fetchone()[0]
            == 1
        )
        # Try Or
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2020-03-01 10:00:01' or b ='2008-01-01 00:00:01'"
            ).fetchone()[0]
            == 2
        )

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    @pytest.mark.parametrize(
        ['data_type', 'value'],
        [
            ['TINYINT', 127],
            ['SMALLINT', 32767],
            ['INTEGER', 2147483647],
            ['BIGINT', 9223372036854775807],
            ['UTINYINT', 255],
            ['USMALLINT', 65535],
            ['UINTEGER', 4294967295],
            ['UBIGINT', 18446744073709551615],
        ],
    )
    def test_filter_pushdown_integers(self, duckdb_cursor, data_type, value, create_table):
        duckdb_cursor.execute(
            f"""
            CREATE TABLE tbl as select {value}::{data_type} as i
        """
        )
        expected = duckdb_cursor.table('tbl').fetchall()
        filter = "i > 0"
        rel = duckdb_cursor.table('tbl')
        arrow_table = create_table(rel)
        actual = duckdb_cursor.sql(f"select * from arrow_table where {filter}").fetchall()
        assert expected == actual

        # Test with equivalent prepared statement
        actual = duckdb_cursor.execute("select * from arrow_table where i > ?", (0,)).fetchall()
        assert expected == actual
        # Test equality
        actual = duckdb_cursor.execute("select * from arrow_table where i = ?", (value,)).fetchall()
        assert expected == actual

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_date(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_date (
                a DATE,
                b DATE,
                c DATE
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_date VALUES
                ('2000-01-01','2000-01-01','2000-01-01'),
                ('2000-10-01','2000-10-01','2000-10-01'),
                ('2010-01-01','2000-10-01','2010-01-01'),
                (NULL,NULL,NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test_date")
        arrow_table = create_table(duck_tbl)

        # Try ==
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '2000-01-01'").fetchone()[0] == 1
        # Try >
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a > '2000-01-01'").fetchone()[0] == 2
        # Try >=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a >= '2000-10-01'").fetchone()[0] == 2
        # Try <
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a < '2000-10-01'").fetchone()[0] == 1
        # Try <=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a <= '2000-10-01'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2000-10-01' and b = '2000-01-01'"
            ).fetchone()[0]
            == 0
        )
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2010-01-01' and b = '2000-10-01' and c = '2010-01-01'"
            ).fetchone()[0]
            == 1
        )
        # Try Or
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '2010-01-01' or b = '2000-01-01'"
            ).fetchone()[0]
            == 2
        )

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_blob(self, duckdb_cursor, create_table):
        import pandas

        df = pandas.DataFrame(
            {
                'a': [bytes([1]), bytes([2]), bytes([3]), None],
                'b': [bytes([1]), bytes([2]), bytes([3]), None],
                'c': [bytes([1]), bytes([2]), bytes([3]), None],
            }
        )
        rel = duckdb.from_df(df)
        arrow_table = create_table(rel)

        # Try ==
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '\x01'").fetchone()[0] == 1
        # # Try >
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a > '\x01'").fetchone()[0] == 2
        # Try >=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a >= '\x02'").fetchone()[0] == 2
        # Try <
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a < '\x02'").fetchone()[0] == 1
        # Try <=
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a <= '\x02'").fetchone()[0] == 2

        # Try Is Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NULL").fetchone()[0] == 1
        # Try Is Not Null
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a IS NOT NULL").fetchone()[0] == 3

        # Try And
        assert duckdb_cursor.execute("SELECT count(*) from arrow_table where a='\x02' and b ='\x01'").fetchone()[0] == 0
        assert (
            duckdb_cursor.execute(
                "SELECT count(*) from arrow_table where a = '\x02' and b = '\x02' and c = '\x02'"
            ).fetchone()[0]
            == 1
        )
        # Try Or
        assert (
            duckdb_cursor.execute("SELECT count(*) from arrow_table where a = '\x01' or b = '\x02'").fetchone()[0] == 2
        )

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table, create_pyarrow_dataset])
    def test_filter_pushdown_no_projection(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test_int (
                a INTEGER,
                b INTEGER,
                c INTEGER
            )
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test_int VALUES
                (1,1,1),
                (10,10,10),
                (100,10,100),
                (NULL,NULL,NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test_int")
        arrow_table = create_table(duck_tbl)

        assert duckdb_cursor.execute("SELECT * FROM arrow_table VALUES where a = 1").fetchall() == [(1, 1, 1)]

    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_pushdown_2145(self, duckdb_cursor, tmp_path, create_table):
        import pandas

        date1 = pandas.date_range("2018-01-01", "2018-12-31", freq="B")
        df1 = pandas.DataFrame(np.random.randn(date1.shape[0], 5), columns=list("ABCDE"))
        df1["date"] = date1

        date2 = pandas.date_range("2019-01-01", "2019-12-31", freq="B")
        df2 = pandas.DataFrame(np.random.randn(date2.shape[0], 5), columns=list("ABCDE"))
        df2["date"] = date2

        data1 = tmp_path / 'data1.parquet'
        data2 = tmp_path / 'data2.parquet'
        duckdb_cursor.execute(f"copy (select * from df1) to '{data1.as_posix()}'")
        duckdb_cursor.execute(f"copy (select * from df2) to '{data2.as_posix()}'")

        glob_pattern = tmp_path / 'data*.parquet'
        table = duckdb_cursor.read_parquet(glob_pattern.as_posix()).arrow()

        output_df = duckdb.arrow(table).filter("date > '2019-01-01'").df()
        expected_df = duckdb.from_parquet(glob_pattern.as_posix()).filter("date > '2019-01-01'").df()
        pandas.testing.assert_frame_equal(expected_df, output_df)

    # https://github.com/duckdb/duckdb/pull/4817/files#r1339973721
    @pytest.mark.parametrize('create_table', [create_pyarrow_pandas, create_pyarrow_table])
    def test_filter_column_removal(self, duckdb_cursor, create_table):
        duckdb_cursor.execute(
            """
            CREATE TABLE test AS SELECT
                range a,
                100 - range b
            FROM range(100)
        """
        )
        duck_test_table = duckdb_cursor.table("test")
        arrow_table = create_table(duck_test_table)

        # PR 4817 - remove filter columns that are unused in the remainder of the query plan from the table function
        query_res = duckdb_cursor.execute(
            """
            EXPLAIN SELECT count(*) FROM arrow_table WHERE
                a > 25 AND b > 25
        """
        ).fetchall()

        # scanned columns that come out of the scan are displayed like this, so we shouldn't see them
        match = re.search("│ +a +│", query_res[0][1])
        assert not match
        match = re.search("│ +b +│", query_res[0][1])
        assert not match
