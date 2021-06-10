import duckdb
import os
import sys
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

class TestArrowIntegration(object):
    def test_parquet_roundtrip(self, duckdb_cursor):
        if not can_run:
            return
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')
        cols = 'id, first_name, last_name, email, gender, ip_address, cc, country, birthdate, salary, title, comments'

        # TODO timestamp

        userdata_parquet_table = pyarrow.parquet.read_table(parquet_filename)
        userdata_parquet_table.validate(full=True)
        rel_from_arrow = duckdb.arrow(userdata_parquet_table).project(cols).arrow()
        rel_from_arrow.validate(full=True)

        rel_from_duckdb = duckdb.from_parquet(parquet_filename).project(cols).arrow()
        rel_from_duckdb.validate(full=True)

        # batched version, lets use various values for batch size
        for i in [7, 51, 99, 100, 101, 500, 1000, 2000]:
            userdata_parquet_table2 = pyarrow.Table.from_batches(userdata_parquet_table.to_batches(i))
            assert userdata_parquet_table.equals(userdata_parquet_table2, check_metadata=True)

            rel_from_arrow2 = duckdb.arrow(userdata_parquet_table2).project(cols).arrow()
            rel_from_arrow2.validate(full=True)

            assert rel_from_arrow.equals(rel_from_arrow2, check_metadata=True)
            assert rel_from_arrow.equals(rel_from_duckdb, check_metadata=True)

    def test_unsigned_roundtrip(self,duckdb_cursor):
        if not can_run:
            return
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','unsigned.parquet')
        data = (pyarrow.array([1,2,3,4,5,255], type=pyarrow.uint8()),pyarrow.array([1,2,3,4,5,65535], \
            type=pyarrow.uint16()),pyarrow.array([1,2,3,4,5,4294967295], type=pyarrow.uint32()),\
                pyarrow.array([1,2,3,4,5,18446744073709551615], type=pyarrow.uint64()))

        tbl = pyarrow.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        pyarrow.parquet.write_table(tbl, parquet_filename)

        cols = 'a, b, c, d'

        unsigned_parquet_table = pyarrow.parquet.read_table(parquet_filename)
        unsigned_parquet_table.validate(full=True)
        rel_from_arrow = duckdb.arrow(unsigned_parquet_table).project(cols).arrow()
        rel_from_arrow.validate(full=True)

        rel_from_duckdb = duckdb.from_parquet(parquet_filename).project(cols).arrow()
        rel_from_duckdb.validate(full=True)

        assert rel_from_arrow.equals(rel_from_duckdb, check_metadata=True)

        con = duckdb.connect()
        con.execute("select NULL c_null, (c % 4 = 0)::bool c_bool, (c%128)::tinyint c_tinyint, c::smallint*1000 c_smallint, c::integer*100000 c_integer, c::bigint*1000000000000 c_bigint, c::float c_float, c::double c_double, 'c_' || c::string c_string from (select case when range % 2 == 0 then range else null end as c from range(-10000, 10000)) sq")
        arrow_result = con.fetch_arrow_table()
        arrow_result.validate(full=True)
        arrow_result.combine_chunks()
        arrow_result.validate(full=True)

        round_tripping = duckdb.from_arrow_table(arrow_result).to_arrow_table()
        round_tripping.validate(full=True)

        assert round_tripping.equals(arrow_result, check_metadata=True)

    def test_decimals_roundtrip(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        duckdb_conn.execute("CREATE TABLE test (a DECIMAL(4,2), b DECIMAL(9,2), c DECIMAL (18,2), d DECIMAL (30,2))")

        duckdb_conn.execute("INSERT INTO  test VALUES (1.11,1.11,1.11,1.11),(NULL,NULL,NULL,NULL)")

        true_result = duckdb_conn.execute("SELECT sum(a), sum(b), sum(c),sum(d) from test").fetchall()

        duck_tbl = duckdb_conn.table("test")

        duck_from_arrow = duckdb_conn.from_arrow_table(duck_tbl.arrow())

        duck_from_arrow.create("testarrow")

        arrow_result = duckdb_conn.execute("SELECT sum(a), sum(b), sum(c),sum(d) from testarrow").fetchall()

        assert(arrow_result == true_result)

        arrow_result = duckdb_conn.execute("SELECT typeof(a), typeof(b), typeof(c),typeof(d) from testarrow").fetchone()

        assert (arrow_result[0] == 'DECIMAL(4,2)') 
        assert (arrow_result[1] == 'DECIMAL(9,2)') 
        assert (arrow_result[2] == 'DECIMAL(18,2)') 
        assert (arrow_result[3] == 'DECIMAL(30,2)') 

        #Lets also test big number comming from arrow land
        data = (pyarrow.array(np.array([9999999999999999999999999999999999]), type=pyarrow.decimal128(38,0)))
        arrow_tbl = pyarrow.Table.from_arrays([data],['a'])
        duckdb_conn = duckdb.connect()
        duckdb_conn.from_arrow_table(arrow_tbl).create("bigdecimal")
        result = duckdb_conn.execute('select * from bigdecimal')
        assert (result.fetchone()[0] == 9999999999999999999999999999999999)
