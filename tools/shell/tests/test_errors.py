# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

lineitem_ddl = 'CREATE TABLE lineitem(l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);'

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_incorrect_column(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_errors on")
        .statement(lineitem_ddl)
        .statement('select * from lineitem where l_extendedpric=5;')
    )
    result = test.run()
    result.check_stderr('"\x1b[33ml_extendedprice')
    result.check_stderr('"\x1b[33ml_extendedpric\x1b[00m')

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_missing_table(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_errors on")
        .statement(lineitem_ddl)
        .statement('select * from lineite where l_extendedprice=5;')
    )
    result = test.run()
    result.check_stderr('"\x1b[33mlineitem\x1b[00m')

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_long_error(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_errors on")
        .statement(lineitem_ddl)
        .statement('''SELECT
      l_returnflag,
      l_linestatus,
      sum(l_quantity) AS sum_qty,
      sum(l_extendedprice) AS sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
      avg(l_quantity) AS avg_qty,
      avg(l_extendedprice) AS avg_price,
      avg(l_discount) AS avg_disc,
      count(*) AS count_order
  FROM
      lineitem
  WHERE
      l_shipdate <= CAST('1998-09-02' AS date) + timestamp '2020-01-01'
  GROUP BY
      l_returnflag,
      l_linestatus
  ORDER BY
      l_returnflag,
      l_linestatus;''')
    )
    result = test.run()
    result.check_stderr('\x1b[33m+(DATE, TIMESTAMP)\x1b[00m')
    result.check_stderr('\x1b[32mCAST\x1b[00m')

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_single_quotes_in_error(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_errors on")
        .statement("select \"I'm an error\"")
    )
    result = test.run()
    result.check_stderr('"\x1b[33mI\'m an error\x1b[00m')

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_double_quotes_in_error(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_errors on")
        .statement("select error('''I\"m an error''')")
    )
    result = test.run()
    result.check_stderr('\x1b[33mI"m an error\x1b[00m')

@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_unterminated_quote(shell):
    test = (
        ShellTest(shell)
        .statement(".highlight_errors on")
        .statement("select error('I''m an error')")
    )
    result = test.run()
    result.check_stderr('I\'m an error')
