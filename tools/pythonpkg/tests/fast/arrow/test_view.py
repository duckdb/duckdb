import duckdb
import os

try:
    import pyarrow
    import pyarrow.parquet

    can_run = True
except:
    can_run = False


class TestArrowView(object):
    def test_arrow_view(self, duckdb_cursor):
        if not can_run:
            return
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        cols = 'id, first_name, last_name, email, gender, ip_address, cc, country, birthdate, salary, title, comments'
        duckdb_conn = duckdb.connect()
        userdata_parquet_table = pyarrow.parquet.read_table(parquet_filename)
        userdata_parquet_table.validate(full=True)
        duckdb_conn.from_arrow(userdata_parquet_table).create_view('arrow_view')
        assert duckdb_conn.execute("PRAGMA show_tables").fetchone() == ('arrow_view',)
        assert duckdb_conn.execute("select avg(salary)::INT from arrow_view").fetchone()[0] == 149005
