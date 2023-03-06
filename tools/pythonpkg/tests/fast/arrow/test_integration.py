import duckdb
import os
import datetime
from pandas import DateOffset
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

        round_tripping = duckdb.from_arrow(arrow_result).to_arrow_table()
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

        duck_from_arrow = duckdb_conn.from_arrow(duck_tbl.arrow())

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
        duckdb_conn.from_arrow(arrow_tbl).create("bigdecimal")
        result = duckdb_conn.execute('select * from bigdecimal')
        assert (result.fetchone()[0] == 9999999999999999999999999999999999)

    def test_intervals_roundtrip(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        # test for import from apache arrow
        expected_value = DateOffset(months=2, days=8,
                                    nanoseconds=(datetime.timedelta(seconds=1, microseconds=1,
                                                                    milliseconds=1, minutes=1,
                                                                    hours=1) //
                                                                    datetime.timedelta(microseconds=1)) * 1000)
        arr = [
            pyarrow.MonthDayNano([2, 8,
                         (datetime.timedelta(seconds=1, microseconds=1,
                                             milliseconds=1, minutes=1,
                                             hours=1) //
                          datetime.timedelta(microseconds=1)) * 1000])]

        data = pyarrow.array(arr, pyarrow.month_day_nano_interval())
        arrow_tbl = pyarrow.Table.from_arrays([data],['a'])
        duckdb_conn = duckdb.connect()
        duckdb_conn.from_arrow(arrow_tbl).create("intervaltbl")
        result = duckdb_conn.execute('select * from intervaltbl')

        assert  (result.fetchone()[0] == expected_value)

        # test for select interval from duckdb
        result = duckdb_conn.execute('SELECT INTERVAL 1 YEAR + INTERVAL 1 DAY + INTERVAL 1 SECOND')
        expected_value = DateOffset(months=12, days=1, nanoseconds=1000000000)
        result_value = result.fetchone()[0]
        assert  (result_value.months == expected_value.months)
        assert  (result_value.days == expected_value.days)
        assert  (result_value.nanoseconds == expected_value.nanoseconds)

    def test_null_intervals_roundtrip(self,duckdb_cursor):
        if not can_run:
            return
        # test for null interval
        expected_value = DateOffset(months=2, days=8,
                                    nanoseconds=(datetime.timedelta(seconds=1, microseconds=1,
                                                                    milliseconds=1, minutes=1,
                                                                    hours=1) //
                                                                    datetime.timedelta(microseconds=1)) * 1000)
        arr = [
            None,
            pyarrow.MonthDayNano([2, 8,
                         (datetime.timedelta(seconds=1, microseconds=1,
                                             milliseconds=1, minutes=1,
                                             hours=1) //
                          datetime.timedelta(microseconds=1)) * 1000])
        ]
        data = pyarrow.array(arr, pyarrow.month_day_nano_interval())
        arrow_tbl = pyarrow.Table.from_arrays([data],['a'])
        duckdb_conn = duckdb.connect()
        duckdb_conn.from_arrow(arrow_tbl).create("intervalnulltbl")
        result = duckdb_conn.execute('select * from intervalnulltbl')
        all_result = result.fetchall()

        assert  (all_result[0][0] == None)
        assert  (all_result[1][0] == expected_value)

    def test_nested_interval_roundtrip(self,duckdb_cursor):
        if not can_run:
            return
        # Dictionary
        indices = pyarrow.array([0, 1, 0, 1, 2, 1, 0, 2])
        first_value = pyarrow.MonthDayNano([0, 1, 2000000])
        first_expected_value = DateOffset(months=0, days=1, nanoseconds=2000000)
        second_value = pyarrow.MonthDayNano([90, 12, 0])
        second_expected_value = DateOffset(months=90, days=12, nanoseconds=0)
        dictionary = pyarrow.array([first_value, second_value, None])
        dict_array = pyarrow.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pyarrow.Table.from_arrays([dict_array],['a'])
        rel = duckdb.from_arrow(arrow_table)

        assert rel.execute().fetchall() == [(first_expected_value,), (second_expected_value,), (first_expected_value,), (second_expected_value,),
                                            (None,), (second_expected_value,), (first_expected_value,), (None,)]
        
        # List
        query = duckdb.query("SELECT a from (select list_value(INTERVAL 3 MONTHS, INTERVAL 5 DAYS, INTERVAL 10 SECONDS, NULL) as a) as t").arrow()['a']
        assert query[0][0].value == pyarrow.MonthDayNano([3, 0, 0])
        assert query[0][1].value == pyarrow.MonthDayNano([0, 5, 0])
        assert query[0][2].value == pyarrow.MonthDayNano([0, 0, 10000000000])
        assert query[0][3].value == None
        
        # Struct
        query = "SELECT a from (SELECT STRUCT_PACK(a := INTERVAL 1 MONTHS, b := INTERVAL 10 DAYS, c:= INTERVAL 20 SECONDS) as a) as t"
        true_answer = duckdb.query(query).fetchall()
        from_arrow = duckdb.from_arrow(duckdb.query(query).arrow()).fetchall()
        assert true_answer[0][0]['a'] == from_arrow[0][0]['a']
        assert true_answer[0][0]['b'] == from_arrow[0][0]['b']
        assert true_answer[0][0]['c'] == from_arrow[0][0]['c']

    def test_min_max_interval_roundtrip(self, duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()
        interval_min_value = pyarrow.MonthDayNano([0, 0, 0])
        expected_min_value = DateOffset(months=0, days=0, nanoseconds=0)
        interval_max_value = pyarrow.MonthDayNano([2147483647, 2147483647, 9223372036854775000])
        expected_max_value = DateOffset(months=2147483647, days=2147483647, nanoseconds=9223372036854775000)
        data = pyarrow.array([interval_min_value, interval_max_value], pyarrow.month_day_nano_interval())
        arrow_tbl = pyarrow.Table.from_arrays([data],['a'])
        duckdb_conn = duckdb.connect()
        duckdb_conn.from_arrow(arrow_tbl).create("intervalminmaxtbl")
        result = duckdb_conn.execute('select * from intervalminmaxtbl')
        all_result = result.fetchall()
        assert  all_result[0][0] == expected_min_value
        assert  all_result[1][0] == expected_max_value

        duck_arrow_tbl = duckdb_conn.table("intervalminmaxtbl").arrow()['a']
        assert duck_arrow_tbl[0].value == pyarrow.MonthDayNano([0, 0, 0])
        assert duck_arrow_tbl[1].value == pyarrow.MonthDayNano([2147483647, 2147483647, 9223372036854775000])

    def test_strings_roundtrip(self,duckdb_cursor):
        if not can_run:
            return

        duckdb_conn = duckdb.connect()

        duckdb_conn.execute("CREATE TABLE test (a varchar)")

        # Test Small, Null and Very Big String
        for i in range (0,1000):
            duckdb_conn.execute("INSERT INTO  test VALUES ('Matt Damon'),(NULL), ('Jeffffreeeey Jeeeeef Baaaaaaazos'), ('X-Content-Type-Options')")

        true_result = duckdb_conn.execute("SELECT * from test").fetchall()

        duck_tbl = duckdb_conn.table("test")

        duck_from_arrow = duckdb_conn.from_arrow(duck_tbl.arrow())

        duck_from_arrow.create("testarrow")

        arrow_result = duckdb_conn.execute("SELECT * from testarrow").fetchall()

        assert(arrow_result == true_result)
