import duckdb
import os
try:
    import pyarrow
    import pyarrow.parquet
    can_run = True
except:
    can_run = False

class TestArrowReads(object):
    def test_multiple_queries_same_relation(self, duckdb_cursor):
        if not can_run:
            return
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')
        cols = 'id, first_name, last_name, email, gender, ip_address, cc, country, birthdate, salary, title, comments'

        userdata_parquet_table = pyarrow.parquet.read_table(parquet_filename)
        userdata_parquet_table.validate(full=True)
        rel = duckdb.from_arrow(userdata_parquet_table)
        assert(rel.aggregate("(avg(salary))::INT").execute().fetchone()[0] == 149005)
        assert(rel.aggregate("(avg(salary))::INT").execute().fetchone()[0] == 149005)
