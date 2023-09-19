# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
from conftest import json_extension
import os

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_read_stdin_csv(shell):
    test = (
        ShellTest(shell)
        .input_file('test/sql/copy/csv/data/test/test.csv')
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
    result.check_stdout('0,0," test"')

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_read_stdin_csv_auto(shell):
    test = (
        ShellTest(shell)
        .input_file('test/sql/copy/csv/data/test/test.csv')
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
    result.check_stdout('0,0," test"')

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_read_stdin_csv_auto_projection(shell):
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

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_read_stdin_ndjson(shell, json_extension):
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

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_read_stdin_json_auto(shell, json_extension):
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

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
@pytest.mark.parametrize("alias", [
    "'/dev/stdout'",
    'stdout'
])
def test_copy_to_stdout(shell, alias):
    test = (
        ShellTest(shell)
        .statement(f"COPY (SELECT 42) TO {alias};")
    )
    result = test.run()
    result.check_stdout('42')

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
@pytest.mark.parametrize("alias", [
    "'/dev/stdout'",
    'stdout'
])
def test_copy_csv_to_stdout(shell, alias):
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

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
@pytest.mark.parametrize("alias", [
    "'/dev/stderr'",
    'stderr'
])
def test_copy_csv_to_stderr(shell, alias):
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

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_copy_non_inlined_string(shell):
    test = (
        ShellTest(shell)
        .statement("select list(concat('thisisalongstring', range::VARCHAR)) i from range(10000)")
    )
    result = test.run()
    result.check_stdout('thisisalongstring')

@pytest.mark.skipif(os.name == 'nt', reason="Skipped on windows")
def test_write_to_stdout_piped_to_file(shell, random_filepath):
    test = (
        ShellTest(shell)
        .statement("copy (select * from range(10000) tbl(i)) to '/dev/stdout' (format csv)")
        .output_file(random_filepath.as_posix())
    )
    result = test.run()
    result.check_stdout('9999')

# fmt: on
