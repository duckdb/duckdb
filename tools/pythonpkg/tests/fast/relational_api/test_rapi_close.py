import duckdb
import pytest

# A closed connection should invalidate all relation's methods
class TestRAPICloseConnRel(object):
	def test_close_conn_rel(self, duckdb_cursor):
		con = duckdb.connect()
		con.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
		con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
		rel = con.table("items")
		con.close()
		with pytest.raises(Exception, match='This connection is closed'):
			print(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			len(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			rel.filter("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.project("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.order("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.aggregate("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.sum("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.count("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.median("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.quantile("","")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.apply("","")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.min("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.max("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.mean("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.var("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.std("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.value_counts("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.unique("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.union(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			rel.except_(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			rel.intersect(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			rel.join(rel, "")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.distinct()
		with pytest.raises(Exception, match='This connection is closed'):
			print(rel.limit(1))
		with pytest.raises(Exception, match='This connection is closed'):
			rel.query("","")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.execute()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.write_csv("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.insert_into("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.insert("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.create("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.create_view("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.to_arrow_table()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.arrow()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.to_df()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.df()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.fetchone()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.fetchall()
		with pytest.raises(Exception, match='This connection is closed'):
			rel.map(lambda df : df['col0'].add(42).to_frame())
		with pytest.raises(Exception, match='This connection is closed'):
			rel.mad("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.mode("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.abs("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.prod("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.skew("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.kurt("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.sem("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.cumsum("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.cumprod("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.cummax("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.cummin("")
		with pytest.raises(Exception, match='This connection is closed'):
			rel.describe()
		con = duckdb.connect()
		con.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
		con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
		valid_rel = con.table("items")

		# Test these bad boys when left relation is valid
		with pytest.raises(Exception, match='This connection is closed'):
			valid_rel.union(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			valid_rel.except_(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			valid_rel.intersect(rel)
		with pytest.raises(Exception, match='This connection is closed'):
			valid_rel.join(rel, "rel.items = valid_rel.items")

	def test_del_conn(self, duckdb_cursor):
		con = duckdb.connect()
		con.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
		con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
		rel = con.table("items")
		del con
		with pytest.raises(Exception, match='This connection is closed'):
			print(rel)