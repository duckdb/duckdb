#!/usr/bin/env python
# -*- coding: utf-8 -*-
import duckdb
import pandas as pd
import numpy
import datetime
import sys

def run_parallel_queries(main_table, left_join_table, expected_df, iteration_count = 5):
    for i in range(0, iteration_count):
        output_df = None
        sql = """
        select
            main_table.*
            ,t1.*
            ,t2.*
        from main_table
        left join left_join_table t1
            on main_table.join_column = t1.join_column
        left join left_join_table t2
            on main_table.join_column = t2.join_column
        """
        try:
            duckdb_conn = duckdb.connect()
            duckdb_conn.execute("PRAGMA threads=4")
            duckdb_conn.register('main_table', main_table)
            duckdb_conn.register('left_join_table', left_join_table)
            output_df = duckdb_conn.execute(sql).fetchdf()
            pd.testing.assert_frame_equal(expected_df, output_df)
            print(output_df)
        except Exception as err:
            print(err)
        finally:
            duckdb_conn.close()

class TestParallelPandasScan(object):
    def test_parallel_numeric_scan(self, duckdb_cursor):
        main_table = pd.DataFrame([{"join_column": 3}])
        left_join_table = pd.DataFrame([{"join_column": 3,"other_column": 4}])
        run_parallel_queries(main_table, left_join_table, left_join_table)

    def test_parallel_ascii_text(self, duckdb_cursor):
        main_table = pd.DataFrame([{"join_column":"text"}])
        left_join_table = pd.DataFrame([{"join_column":"text","other_column":"more text"}])
        run_parallel_queries(main_table, left_join_table, left_join_table)

    def test_parallel_unicode_text(self, duckdb_cursor):
        main_table = pd.DataFrame([{"join_column":u"mühleisen"}])
        left_join_table = pd.DataFrame([{"join_column": u"mühleisen","other_column":u"höhöhö"}])
        run_parallel_queries(main_table, left_join_table, left_join_table)

    def test_parallel_complex_unicode_text(self, duckdb_cursor):
        if sys.version_info.major < 3:
            return
        main_table = pd.DataFrame([{"join_column":u"鴨"}])
        left_join_table = pd.DataFrame([{"join_column": u"鴨","other_column":u"數據庫"}])
        run_parallel_queries(main_table, left_join_table, left_join_table)

    def test_parallel_emojis(self, duckdb_cursor):
        if sys.version_info.major < 3:
            return
        main_table = pd.DataFrame([{"join_column":u"🤦🏼‍♂️ L🤦🏼‍♂️R 🤦🏼‍♂️"}])
        left_join_table = pd.DataFrame([{"join_column": u"🤦🏼‍♂️ L🤦🏼‍♂️R 🤦🏼‍♂️","other_column":u"🦆🍞🦆"}])
        run_parallel_queries(main_table, left_join_table, left_join_table)

    def test_parallel_numeric_object(self, duckdb_cursor):
        main_table = pd.DataFrame({ 'join_column': pd.Series([3], dtype="Int8") })
        left_join_table = pd.DataFrame({ 'join_column': pd.Series([3], dtype="Int8"), 'other_column': pd.Series([4], dtype="Int8") })
        expected_df = pd.DataFrame({ "join_column": numpy.array([3], dtype=numpy.int8), "other_column": numpy.array([4], dtype=numpy.int8)})
        run_parallel_queries(main_table, left_join_table, expected_df)

    def test_parallel_timestamp(self, duckdb_cursor):
        main_table = pd.DataFrame({ 'join_column': [pd.Timestamp('20180310T11:17:54Z')] })
        left_join_table = pd.DataFrame({ 'join_column': [pd.Timestamp('20180310T11:17:54Z')], 'other_column': [pd.Timestamp('20190310T11:17:54Z')] })
        expected_df = pd.DataFrame({ "join_column": numpy.array([datetime.datetime(2018, 3, 10, 11, 17, 54)], dtype='datetime64[ns]'), "other_column": numpy.array([datetime.datetime(2019, 3, 10, 11, 17, 54)], dtype='datetime64[ns]')})
        run_parallel_queries(main_table, left_join_table, expected_df)
