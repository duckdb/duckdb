# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path


# test import from a parquet file
def test_import_parquet(shell):
    test = (
        ShellTest(shell)
        .statement(f'.import data/parquet-testing/unsigned.parquet a')
        .statement("SELECT MAX(d) FROM a")
    )

    result = test.run()
    result.check_stdout("18446744073709551615")

# test import from a json file
def test_import_json(shell):
    test = (
        ShellTest(shell)
        .statement(f'.import data/json/example_n.ndjson a')
        .statement("SELECT name FROM a")
    )

    result = test.run()
    result.check_stdout("Broadcast News")


# fmt: on
