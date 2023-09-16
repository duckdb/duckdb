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

        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            print(rel)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            len(rel)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.aggregate("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.any_value("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.apply("", "")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.arg_max("", "")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.arg_min("", "")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.arrow()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.avg("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.bit_and("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.bit_or("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.bit_xor("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.bitstring_agg("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.bool_and("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.bool_or("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.count("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.create("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.create_view("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.cume_dist("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.dense_rank("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.describe()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.df()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.distinct()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.execute()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.favg("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.fetchall()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.fetchnumpy()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.fetchone()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.filter("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.first("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.first_value("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.fsum("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.geomean("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.histogram("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.insert("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.insert_into("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.lag("", "")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.last("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.last_value("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.lead("", "")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            print(rel.limit(1))
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.list("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.map(lambda df: df['col0'].add(42).to_frame())
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.max("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.mean("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.median("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.min("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.mode("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.n_tile("", 1)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.nth_value("", "", 1)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.order("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.percent_rank("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.product("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.project("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.quantile("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.quantile_cont("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.quantile_disc("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.query("", "")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.rank("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.rank_dense("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.row_number("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.std("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.stddev("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.stddev_pop("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.stddev_samp("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.string_agg("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.sum("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.to_arrow_table()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.to_df()
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.var("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.var_pop("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.var_samp("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.variance("")
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            rel.write_csv("")

        con = duckdb.connect()
        con.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
        con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
        valid_rel = con.table("items")

        # Test these bad boys when left relation is valid
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            valid_rel.union(rel)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            valid_rel.except_(rel)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            valid_rel.intersect(rel)
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            valid_rel.join(rel.set_alias('rel'), "rel.items = valid_rel.items")

    def test_del_conn(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
        con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
        rel = con.table("items")
        del con
        with pytest.raises(duckdb.ConnectionException, match='Connection has already been closed'):
            print(rel)
