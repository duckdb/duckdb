import duckdb
import numpy
import tempfile
import os
import pandas as pd

def get_relation(conn):
    test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    conn.register("test_df", test_df)
    return conn.from_df(test_df)

class TestRelation(object):
    def test_csv_auto(self, duckdb_cursor):
        conn = duckdb.connect()
        df_rel = get_relation(conn)
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
        test_df.to_csv(temp_file_name, index=False)

        # now create a relation from it
        csv_rel = duckdb.from_csv_auto(temp_file_name)
        assert df_rel.execute().fetchall() == csv_rel.execute().fetchall()

    def test_filter_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.filter('i > 1').execute().fetchall() == [(2, 'two'), (3, 'three'), (4, 'four')]

    def test_projection_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.project('i').execute().fetchall() == [(1,), (2,), (3,), (4,)]

    def test_projection_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.order('j').execute().fetchall() == [(4, 'four'), (1, 'one'), (3, 'three'), (2, 'two')]

    def test_limit_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.limit(2).execute().fetchall() == [(1, 'one'), (2, 'two')]

    def test_aggregate_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.aggregate("sum(i)").execute().fetchall() == [(10,)]
        assert rel.aggregate("j, sum(i)").execute().fetchall() == [('one', 1), ('two', 2), ('three', 3), ('four', 4)]

    def test_distinct_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        assert rel.distinct().execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'),(4, 'four')]

    def test_union_operator(self, duckdb_cursor):
        conn = duckdb.connect()
        rel = get_relation(conn)
        print(rel.union(rel).execute().fetchall())
        assert rel.union(rel).execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]

    def test_join_operator(self, duckdb_cursor):
        # join rel with itself on i
        conn = duckdb.connect()
        test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
        conn.register("test_df", test_df)
        rel = conn.from_df(test_df)
        rel2 = conn.from_df(test_df)
        assert rel.join(rel2, 'i').execute().fetchall()  == [(1, 'one', 'one'), (2, 'two', 'two'), (3, 'three', 'three'), (4, 'four', 'four')]

# def("except_", &DuckDBPyRelation::Except,
#              "Create the set except of this relation object with another relation object in other_rel",
#              py::arg("other_rel"))
# .def("create", &DuckDBPyRelation::Create,
#      "Creates a new table named table_name with the contents of the relation object", py::arg("table_name"))
# .def("create_view", &DuckDBPyRelation::CreateView,
#      "Creates a view named view_name that refers to the relation object", py::arg("view_name"),
# .def("write_csv", &DuckDBPyRelation::WriteCsv, "Write the relation object to a CSV file in file_name",
#      py::arg("file_name"))
# .def("insert_into", &DuckDBPyRelation::InsertInto,
#      "Inserts the relation object into an existing table named table_name", py::arg("table_name"))
# for explicit join conditions the relations can be named using alias()
# print(rel.set_alias('a').join(rel.set_alias('b'), 'a.i=b.i'))
