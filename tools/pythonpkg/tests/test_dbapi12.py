import duckdb
import tempfile
import os
import pandas as pd

class TestRelationApi(object):
	def test_readonly(self, duckdb_cursor):

		test_df = pd.DataFrame.from_dict({"i":[1, 2, 3], "j":["one", "two", "three"]})

		def test_rel(rel, duckdb_cursor):
			res = rel.filter('i < 3').order('j').project('i').union(rel.filter('i > 2').project('i')).join(rel.set_alias('a1'), 'i').project('CAST(i as BIGINT) i, j').order('i')
			pd.testing.assert_frame_equal(res.to_df(), test_df)
			res3 = duckdb_cursor.from_df(res.to_df()).to_df()
			pd.testing.assert_frame_equal(res3, test_df)

			df_sql = res.query('x', 'select CAST(i as BIGINT) i, j from x')
			pd.testing.assert_frame_equal(df_sql.fetchdf(), test_df)

			res2 = res.aggregate('i, count(j) as cj', 'i').order('i')
			cmp_df = pd.DataFrame.from_dict({"i":[1, 2, 3], "cj":[1, 1, 1]})
			pd.testing.assert_frame_equal(res2.to_df(), cmp_df)

			rel.create('a2')
			rel_a2 = duckdb_cursor.table('a2').project('CAST(i as BIGINT) i, j').to_df()
			pd.testing.assert_frame_equal(rel_a2, test_df)

			duckdb_cursor.execute('DROP TABLE IF EXISTS a3')
			duckdb_cursor.execute('CREATE TABLE a3 (i INTEGER, j STRING)')
			rel.insert_into('a3')
			rel_a3 = duckdb_cursor.table('a3').project('CAST(i as BIGINT) i, j').to_df()
			pd.testing.assert_frame_equal(rel_a3, test_df)

		duckdb_cursor.execute('CREATE TABLE a (i INTEGER, j STRING)')
		duckdb_cursor.execute("INSERT INTO a VALUES (1, 'one'), (2, 'two'), (3, 'three')")
		duckdb_cursor.execute('CREATE VIEW v AS SELECT * FROM a')

		duckdb_cursor.execute('CREATE TEMPORARY TABLE at (i INTEGER)')
		duckdb_cursor.execute('CREATE TEMPORARY VIEW vt AS SELECT * FROM at')

		rel_a = duckdb_cursor.table('a')
		rel_v = duckdb_cursor.view('v')
		#rel_at = duckdb_cursor.table('at')
		#rel_vt = duckdb_cursor.view('vt')

		rel_df = duckdb_cursor.from_df(test_df)

		test_rel(rel_a, duckdb_cursor)
		test_rel(rel_v, duckdb_cursor)
		test_rel(rel_df, duckdb_cursor)

# cursor = duckdb.connect().cursor()
# TestRelationApi().test_readonly(cursor)