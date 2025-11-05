# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

lineitem_ddl = 'CREATE TABLE lineitem(l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);'

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_highlight_column_header(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_results on")
        .statement('select NULL AS r;')
    )
    result = test.run()
    result.check_stdout('\x1b[90mNULL\x1b[00m')
@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_custom_highlight(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_results on")
        .statement(".highlight_colors column_name red bold")
        .statement(".highlight_colors column_type yellow")
        .statement(lineitem_ddl)
        .statement('select * from lineitem;')
    )
    result = test.run()
    result.check_stdout('\x1b[1m\x1b[31ml_comment\x1b[00m')
    result.check_stdout('\x1b[33mvarchar\x1b[00m')

def test_custom_highlight_error(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_colors column_nameXX red")
        .statement(".highlight_colors column_name redXX")
        .statement(".highlight_colors column_name red boldXX")
        .statement(".highlight_colors column_name red bold zz")
    )
    result = test.run()
    result.check_stderr("Unknown element 'column_nameXX'")
    result.check_stderr("Unknown highlighting color 'redXX'")
    result.check_stderr("Unknown intensity 'boldXX'")
    result.check_stderr("Usage")

# fmt: on
