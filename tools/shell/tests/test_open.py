# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path


# test import from a parquet file
def test_open(shell, tmp_path):
    target_dir = tmp_path / 'open_test'
    os.mkdir(target_dir)
    test = (
        ShellTest(shell)
        .statement(f"ATTACH '{target_dir}/test.duckdb' as test")
        .statement("USE test")
        .statement("CREATE TABLE tbl AS SELECT 42")
        .statement("USE memory")
        .statement("DETACH test")
        .statement(f"COPY (SELECT 43) TO '{target_dir}/test.parquet'")
        .statement(f"COPY (SELECT 44) TO '{target_dir}/test.csv'")
        .statement(f"ATTACH '{target_dir}/test.parquet' as test2")
        .statement(f"ATTACH '{target_dir}/test.csv' as test3")
        .statement(f".open {target_dir}/test.duckdb")
        .statement("FROM tbl")
        .statement(f".open {target_dir}/test.parquet")
        .statement("FROM file")
        .statement(f".open {target_dir}/test.csv")
        .statement("FROM file")
    )

    result = test.run()
    result.check_stdout("42")
    result.check_stdout("43")
    result.check_stdout("44")
