# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
from conftest import autocomplete_extension
import os

# 'autocomplete_extension' is a fixture which will skip the test if 'autocomplete' is not loaded
def test_autocomplete_select(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CALL sql_auto_complete('SEL')")
    )
    result = test.run()
    result.check_stdout('SELECT')

def test_autocomplete_first_from(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CALL sql_auto_complete('FRO')")
    )
    result = test.run()
    result.check_stdout('FROM')

def test_autocomplete_column(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('SELECT my_') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('my_column')

def test_autocomplete_where(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('SELECT my_column FROM my_table WH') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('WHERE')

def test_autocomplete_insert(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('INS') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('INSERT')

def test_autocomplete_into(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('INSERT IN') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('INTO')

def test_autocomplete_into_table(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('INSERT INTO my_t') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('my_table')

def test_autocomplete_values(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('INSERT INTO my_table VAL') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('VALUES')

def test_autocomplete_delete(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('DEL') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('DELETE')

def test_autocomplete_delete_from(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('DELETE F') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('FROM')

def test_autocomplete_from_table(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('DELETE FROM m') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('my_table')

def test_autocomplete_update(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('UP') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('UPDATE')

def test_autocomplete_update_table(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('UPDATE m') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('my_table')

    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("""SELECT * FROM sql_auto_complete('UPDATE "m') LIMIT 1;""")
    )
    result = test.run()
    result.check_stdout('my_table')

def test_autocomplete_update_column(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE my_table(my_column INTEGER);")
        .statement("SELECT * FROM sql_auto_complete('UPDATE my_table SET m') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('my_column')

def test_autocomplete_funky_table(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("""CREATE TABLE "Funky Table With Spaces"(my_column INTEGER);""")
        .statement("SELECT suggestion FROM sql_auto_complete('SELECT * FROM F') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('"Funky Table With Spaces"')

    test = (
        ShellTest(shell)
        .statement("""CREATE TABLE "Funky Table With Spaces"("Funky Column" int);""")
        .statement("""SELECT suggestion FROM sql_auto_complete('select "Funky Column" FROM f') LIMIT 1;""")
    )
    result = test.run()
    result.check_stdout('"Funky Table With Spaces"')

def test_autocomplete_funky_column(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("""CREATE TABLE "Funky Table With Spaces"("Funky Column" int);""")
        .statement("SELECT * FROM sql_auto_complete('select f') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('"Funky Column"')

def test_autocomplete_semicolon(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("SELECT * FROM sql_auto_complete('SELECT 42; SEL') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('SELECT')

def test_autocomplete_comments(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("""
SELECT * FROM sql_auto_complete('--SELECT * FROM
SEL') LIMIT 1;""")
    )
    result = test.run()
    result.check_stdout('SELECT')

def test_autocomplete_scalar_functions(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("SELECT * FROM sql_auto_complete('SELECT regexp_m') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('regexp_matches')

def test_autocomplete_aggregates(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("SELECT * FROM sql_auto_complete('SELECT approx_c') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('approx_count_distinct')

def test_autocomplete_builtin_views(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("SELECT * FROM sql_auto_complete('SELECT * FROM sqlite_ma') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('sqlite_master')

def test_autocomplete_table_function(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("SELECT * FROM sql_auto_complete('SELECT * FROM read_csv_a') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('read_csv_auto')

def test_autocomplete_tpch(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE partsupp(ps_suppkey int);")
        .statement("CREATE TABLE supplier(s_suppkey int);")
        .statement("CREATE TABLE nation(n_nationkey int);")
        .statement("SELECT * FROM sql_auto_complete('DROP TABLE na') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('nation')

    test = (
        ShellTest(shell)
        .statement("CREATE TABLE partsupp(ps_suppkey int);")
        .statement("CREATE TABLE supplier(s_suppkey int);")
        .statement("CREATE TABLE nation(n_nationkey int);")
        .statement("SELECT * FROM sql_auto_complete('SELECT s_supp') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('s_suppkey')

    test = (
        ShellTest(shell)
        .statement("CREATE TABLE partsupp(ps_suppkey int);")
        .statement("CREATE TABLE supplier(s_suppkey int);")
        .statement("CREATE TABLE nation(n_nationkey int);")
        .statement("SELECT * FROM sql_auto_complete('SELECT * FROM partsupp JOIN supp') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('supplier')

    test = (
        ShellTest(shell)
        .statement("CREATE TABLE partsupp(ps_suppkey int);")
        .statement("CREATE TABLE supplier(s_suppkey int);")
        .statement("CREATE TABLE nation(n_nationkey int);")
        .statement(".mode csv")
        .statement("SELECT l,l FROM sql_auto_complete('SELECT * FROM partsupp JOIN supplier ON (s_supp') t(l) LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('s_suppkey,s_suppkey')

    test = (
        ShellTest(shell)
        .statement("CREATE TABLE partsupp(ps_suppkey int);")
        .statement("CREATE TABLE supplier(s_suppkey int);")
        .statement("CREATE TABLE nation(n_nationkey int);")
        .statement("SELECT * FROM sql_auto_complete('SELECT * FROM partsupp JOIN supplier USING (ps_su') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('ps_suppkey')

def test_autocomplete_from(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("SELECT * FROM sql_auto_complete('SELECT * FR') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('FROM')

def test_autocomplete_disambiguation_column(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE MyTable(MyColumn Varchar);")
        .statement("SELECT * FROM sql_auto_complete('SELECT My') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('MyColumn')
    
def test_autocomplete_disambiguation_table(shell, autocomplete_extension):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE MyTable(MyColumn Varchar);")
        .statement("SELECT * FROM sql_auto_complete('SELECT MyColumn FROM My') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout('MyTable')

def test_autocomplete_directory(shell, autocomplete_extension, tmp_path):
    shell_test_dir = tmp_path / 'shell_test_dir'
    extra_path = tmp_path / 'shell_test_dir' / 'extra_path'
    shell_test_dir.mkdir()
    extra_path.mkdir()

    # Create the files
    base_files = ['extra.parquet', 'extra.file']
    for fname in base_files:
        with open(shell_test_dir / fname, 'w+') as f:
            f.write('')

    # Complete the directory
    partial_directory = tmp_path / 'shell_test'
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE MyTable(MyColumn Varchar);")
        .statement(f"SELECT * FROM sql_auto_complete('SELECT * FROM ''{partial_directory.as_posix()}') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout("shell_test_dir")

    # Complete the sub directory as well
    partial_subdirectory = tmp_path / 'shell_test_dir' / 'extra'
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE MyTable(MyColumn Varchar);")
        .statement(f"SELECT * FROM sql_auto_complete('SELECT * FROM ''{partial_subdirectory.as_posix()}') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout("extra_path")

    # Complete the parquet file in the sub directory
    partial_parquet = tmp_path / 'shell_test_dir' / 'extra.par'
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE MyTable(MyColumn Varchar);")
        .statement(f"SELECT * FROM sql_auto_complete('SELECT * FROM ''{partial_parquet.as_posix()}') LIMIT 1;")
    )
    result = test.run()
    result.check_stdout("extra.parquet")

# fmt: on
