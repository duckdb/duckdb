# import duckdb
# import numpy
# import tempfile
# import os

# class TestRelation(object):

#     def test_csv_auto(self, duckdb_cursor):
#         conn = duckdb.connect()
#         test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
#         conn.register("test_df", test_df)
#         df_rel = conn.from_df(test_df)
#         temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
#         test_df.to_csv(temp_file_name, index=False)

#         # now create a relation from it
#         csv_rel = duckdb.from_csv_auto(temp_file_name)
#         assert df_rel.execute() == csv_rel.execute()


#     def test_filter_operator(self, duckdb_cursor):
#         conn = duckdb.connect()
#         test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
#         conn.register("test_df", test_df)
#         rel = conn.from_df(test_df)