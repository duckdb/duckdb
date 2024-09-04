# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
from conftest import json_extension
import os

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
class TestReadFromStdin(object):
    def test_read_stdin_csv(self, shell):
        test = (
            ShellTest(shell)
            .input_file('data/csv/test/test.csv')
            .statement("""
                create table mytable as select * from
                read_csv('/dev/stdin',
                    columns=STRUCT_PACK(foo := 'INTEGER', bar := 'INTEGER', baz := 'VARCHAR'),
                    AUTO_DETECT='false'
                )
            """)
            .statement("select * from mytable limit 1;")
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout("foo,bar,baz")
        result.check_stdout('0,0, test')

    def test_read_stdin_csv_where_filename(self, shell):
        test = (
            ShellTest(shell)
            .input_file('data/csv/test/test.csv')
            .statement("""
                SELECT * FROM read_csv_auto(
                    'data/csv/bug_9005/teste*.csv',
                    header=TRUE,
                    filename=true,
                    union_by_name=True
                ) where filename='data/csv/bug_9005/teste1.csv'
            """)
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout([
            'id,name,factor,filename',
            '1,Ricardo,1.5,data/csv/bug_9005/teste1.csv',
            '2,Jose,2.0,data/csv/bug_9005/teste1.csv'
        ])

    def test_read_stdin_csv_auto(self, shell):
        test = (
            ShellTest(shell)
            .input_file('data/csv/test/test.csv')
            .statement("""
                create table mytable as select * from
                read_csv_auto('/dev/stdin')
            """)
            .statement("select * from mytable limit 1;")
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout("column0,column1,column2")
        result.check_stdout('0,0, test')

    def test_split_part_csv(self, shell):
        test = (
            ShellTest(shell)
            .input_file('data/csv/split_part.csv')
            .statement("""
                FROM read_csv('/dev/stdin') select split_part(C1, ',', 2) as res;
            """)
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout("res")
        result.check_stdout('12')
        result.check_stdout('12')

    def test_read_stdin_csv_auto_projection(self, shell):
        test = (
            ShellTest(shell)
            .input_file('data/csv/tpcds_14.csv')
            .statement("""
                create table mytable as select * from
                read_csv_auto('/dev/stdin')
            """)
            .statement("select channel,i_brand_id,sum_sales,number_sales from mytable;")
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout("web,8006004,844.21,21")


    def test_read_stdin_ndjson(self, shell, json_extension):
        test = (
            ShellTest(shell)
            .input_file('data/json/example_rn.ndjson')
            .statement("""
                create table mytable as select * from
                read_ndjson_objects('/dev/stdin')
            """)
            .statement("select * from mytable;")
            .add_argument(
                '-list',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout([
            "json",
            '{"id":1,"name":"O Brother, Where Art Thou?"}',
            '{"id":2,"name":"Home for the Holidays"}',
            '{"id":3,"name":"The Firm"}',
            '{"id":4,"name":"Broadcast News"}',
            '{"id":5,"name":"Raising Arizona"}'
        ])


    def test_read_stdin_json_auto(self, shell, json_extension):
        test = (
            ShellTest(shell)
            .input_file('data/json/example_rn.ndjson')
            .statement("""
                create table mytable as select * from
                read_json_auto('/dev/stdin')
            """)
            .statement("select * from mytable;")
            .add_argument(
                '-list',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout([
            'id|name',
            '1|O Brother, Where Art Thou?',
            '2|Home for the Holidays',
            '3|The Firm',
            '4|Broadcast News',
            '5|Raising Arizona'
        ])

    def test_read_stdin_json_array(self, shell, json_extension):
        test = (
            ShellTest(shell)
            .input_file('data/json/11407.json')
            .statement("""
                create table mytable as select * from
                read_json_auto('/dev/stdin')
            """)
            .statement("select * from mytable;")
            .add_argument(
                '-list',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout([
            'k',
            'v',
            'v2'
        ])

    def test_read_stdin_json_auto_recursive_cte(self, shell, json_extension):
        test = (
            ShellTest(shell)
            .input_file('data/json/filter_keystage.ndjson')
            .statement("""
                CREATE TABLE mytable AS
                WITH RECURSIVE nums AS (
                    SELECT 0 AS n
                UNION ALL
                    SELECT n + 1
                    FROM nums
                    WHERE n < (
                        SELECT MAX(JSON_ARRAY_LENGTH(filter_keystage))::int - 1
                        FROM read_json_auto('/dev/stdin'))
                ) SELECT * FROM nums;
            """)
            .statement("select * from mytable;")
            .add_argument(
                '-list',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout([
            'n',
            '0',
            '1',
            '2',
        ])


    @pytest.mark.parametrize("alias", [
        "'/dev/stdout'",
        'stdout'
    ])
    def test_copy_to_stdout(self, shell, alias):
        test = (
            ShellTest(shell)
            .statement(f"COPY (SELECT 42) TO {alias};")
        )
        result = test.run()
        result.check_stdout('42')


    @pytest.mark.parametrize("alias", [
        "'/dev/stdout'",
        'stdout'
    ])
    def test_copy_csv_to_stdout(self, shell, alias):
        test = (
            ShellTest(shell)
            .statement(f"COPY (SELECT 42) TO {alias} WITH (FORMAT 'csv');")
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stdout('42')


    @pytest.mark.parametrize("alias", [
        "'/dev/stderr'"
    ])
    def test_copy_csv_to_stderr(self, shell, alias):
        test = (
            ShellTest(shell)
            .statement(f"COPY (SELECT 42) TO {alias} WITH (FORMAT 'csv');")
            .add_argument(
                '-csv',
                ':memory:'
            )
        )
        result = test.run()
        result.check_stderr('42')


    def test_copy_non_inlined_string(self, shell):
        test = (
            ShellTest(shell)
            .statement("select list(concat('thisisalongstring', range::VARCHAR)) i from range(10000)")
        )
        result = test.run()
        result.check_stdout('thisisalongstring')


    def test_write_to_stdout_piped_to_file(self, shell, random_filepath):
        test = (
            ShellTest(shell)
            .statement("copy (select * from range(10000) tbl(i)) to '/dev/stdout' (format csv)")
            .output_file(random_filepath.as_posix())
        )
        result = test.run()
        result.check_stdout('9999')

# fmt: on
