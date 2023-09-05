import duckdb
import numpy as np
import tempfile
import os
import pandas as pd
import pytest

from duckdb.typing import BIGINT, VARCHAR, TINYINT, BOOLEAN


def get_relation(conn):
    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
    conn.register("test_df", test_df)
    return conn.from_df(test_df)


class TestRelation(object):
    def test_csv_auto(self, duckdb_cursor):
        conn = duckdb.connect()
        df_rel = get_relation(conn)
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        test_df.to_csv(temp_file_name, index=False)

        # now create a relation from it
        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert df_rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_filter_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.filter('i > 1').execute().fetchall() == [(2, 'two'), (3, 'three'), (4, 'four')]

    def test_projection_operator_single(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.project('i').execute().fetchall() == [(1,), (2,), (3,), (4,)]

    def test_projection_operator_double(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.order('j').execute().fetchall() == [(4, 'four'), (1, 'one'), (3, 'three'), (2, 'two')]

    def test_limit_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.limit(2).execute().fetchall() == [(1, 'one'), (2, 'two')]
        assert rel.limit(2, offset=1).execute().fetchall() == [(2, 'two'), (3, 'three')]

    def test_intersect_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        conn.register("test_df", test_df)
        test_df_2 = pd.DataFrame.from_dict({"i": [3, 4, 5, 6]})
        conn.register("test_df", test_df_2)
        rel = conn.from_df(test_df)
        rel_2 = conn.from_df(test_df_2)

        assert rel.intersect(rel_2).order('i').execute().fetchall() == [(3,), (4,)]

    def test_aggregate_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.aggregate("sum(i)").execute().fetchall() == [(10,)]
        assert rel.aggregate("j, sum(i)").order('#2').execute().fetchall() == [
            ('one', 1),
            ('two', 2),
            ('three', 3),
            ('four', 4),
        ]

    def test_distinct_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.distinct().order('all').execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]

    def test_union_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        print(rel.union(rel).execute().fetchall())
        assert rel.union(rel).execute().fetchall() == [
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
        ]

    def test_join_operator(self, duckdb_cursor):
        # join rel with itself on i
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = conn.from_df(test_df)
        rel2 = conn.from_df(test_df)
        assert rel.join(rel2, 'i').execute().fetchall() == [
            (1, 'one', 'one'),
            (2, 'two', 'two'),
            (3, 'three', 'three'),
            (4, 'four', 'four'),
        ]

    def test_except_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = conn.from_df(test_df)
        rel2 = conn.from_df(test_df)
        assert rel.except_(rel2).execute().fetchall() == []

    def test_create_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = conn.from_df(test_df)
        rel.create("test_df")
        assert conn.query("select * from test_df").execute().fetchall() == [
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
        ]

    def test_create_view_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = conn.from_df(test_df)
        rel.create_view("test_df")
        assert conn.query("select * from test_df").execute().fetchall() == [
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
        ]

    def test_insert_into_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = conn.from_df(test_df)
        rel.create("test_table2")
        # insert the relation's data into an existing table
        conn.execute("CREATE TABLE test_table3 (i INTEGER, j STRING)")
        rel.insert_into("test_table3")

        # Inserting elements into table_3
        print(conn.values([5, 'five']).insert_into("test_table3"))
        rel_3 = conn.table("test_table3")
        rel_3.insert([6, 'six'])

        assert rel_3.execute().fetchall() == [
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
            (5, 'five'),
            (6, 'six'),
        ]

    def test_write_csv_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        df_rel = get_relation(conn)
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        df_rel.write_csv(temp_file_name)

        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert df_rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_get_attr_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (i INTEGER)")
        rel = conn.table("test")
        assert rel.alias == "test"
        assert rel.type == "TABLE_RELATION"
        assert rel.columns == ['i']
        assert rel.types == ['INTEGER']

    def test_query_fail(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (i INTEGER)")
        rel = conn.table("test")
        with pytest.raises(TypeError, match='incompatible function arguments'):
            rel.query("select j from test")

    def test_execute_fail(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (i INTEGER)")
        rel = conn.table("test")
        with pytest.raises(TypeError, match='incompatible function arguments'):
            rel.execute("select j from test")

    def test_df_proj(self, duckdb_cursor):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = duckdb.project(test_df, 'i')
        assert rel.execute().fetchall() == [(1,), (2,), (3,), (4,)]

    def test_project_on_types(self, duckdb_cursor):
        con = duckdb_cursor
        con.sql(
            """
            create table tbl(
                c0 BIGINT,
                c1 TINYINT,
                c2 VARCHAR,
                c3 TIMESTAMP,
                c4 VARCHAR,
                c5 STRUCT(a VARCHAR, b BIGINT)
            )
            """
        )
        rel = con.table("tbl")
        # select only the varchar columns
        projection = rel.select_types(["varchar"])
        assert projection.columns == ["c2", "c4"]

        # select bigint, tinyint and a type that isn't there
        projection = rel.select_types([BIGINT, "tinyint", con.struct_type({'a': VARCHAR, 'b': TINYINT})])
        assert projection.columns == ["c0", "c1"]

        ## select with empty projection list, not possible
        with pytest.raises(duckdb.Error):
            projection = rel.select_types([])

        # select with type-filter that matches nothing
        with pytest.raises(duckdb.Error):
            projection = rel.select_types([BOOLEAN])

    def test_df_alias(self, duckdb_cursor):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = duckdb.alias(test_df, 'dfzinho')
        assert rel.alias == "dfzinho"

    def test_df_filter(self, duckdb_cursor):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = duckdb.filter(test_df, 'i > 1')
        assert rel.execute().fetchall() == [(2, 'two'), (3, 'three'), (4, 'four')]

    def test_df_order_by(self, duckdb_cursor):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = duckdb.order(test_df, 'j')
        assert rel.execute().fetchall() == [(4, 'four'), (1, 'one'), (3, 'three'), (2, 'two')]

    def test_df_distinct(self, duckdb_cursor):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        rel = duckdb.distinct(test_df).order('i')
        assert rel.execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]

    def test_df_write_csv(self, duckdb_cursor):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        duckdb.write_csv(test_df, temp_file_name)
        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert csv_rel.execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]

    def test_join_types(self, duckdb_cursor):
        test_df1 = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        test_df2 = pd.DataFrame.from_dict({"j": [3, 4, 5, 6]})
        rel1 = duckdb_cursor.from_df(test_df1)
        rel2 = duckdb_cursor.from_df(test_df2)

        assert rel1.join(rel2, 'i=j', 'inner').aggregate('count()').fetchone()[0] == 2

        assert rel1.join(rel2, 'i=j', 'left').aggregate('count()').fetchone()[0] == 4

    def test_fetchnumpy(self, duckdb_cursor):
        start, stop = -1000, 2000
        count = stop - start

        con = duckdb.connect()
        con.execute(f"CREATE table t AS SELECT range AS a FROM range({start}, {stop});")
        rel = con.table("t")

        # empty
        res = rel.limit(0, offset=count + 1).fetchnumpy()
        assert set(res.keys()) == {"a"}
        assert len(res["a"]) == 0

        # < vector_size, == vector_size, > vector_size
        for size in [1000, 1024, 1100]:
            res = rel.project("a").limit(size).fetchnumpy()
            assert set(res.keys()) == {"a"}
            # For some reason, this return a masked array. Shouldn't it be
            # known that there can't be NULLs?
            if isinstance(res, np.ma.MaskedArray):
                assert res.count() == size
                res = res.compressed()
            else:
                assert len(res["a"]) == size
            assert np.all(res["a"] == np.arange(start, start + size))

        with pytest.raises(duckdb.ConversionException, match="Conversion Error.*out of range.*"):
            # invalid conversion of negative integer to UINTEGER
            rel.project("CAST(a as UINTEGER)").fetchnumpy()

    def test_close(self):
        def counter():
            counter.count += 1
            return 42

        counter.count = 0
        conn = duckdb.connect()
        conn.create_function('my_counter', counter, [], BIGINT)

        # Create a relation
        rel = conn.sql('select my_counter()')
        # Execute the relation once
        rel.fetchall()
        assert counter.count == 1
        # Close the result
        rel.close()
        # Verify that the query was not run again
        assert counter.count == 1
        rel.fetchall()
        assert counter.count == 2

        # Verify that the query is run at least once if it's closed before it was executed.
        rel = conn.sql('select my_counter()')
        rel.close()
        assert counter.count == 3

    def test_relation_print(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("Create table t1 as select * from range(1000000)")
        rel1 = con.table('t1')
        text1 = str(rel1)
        assert '? rows' in text1
        assert '>9999 rows' in text1
