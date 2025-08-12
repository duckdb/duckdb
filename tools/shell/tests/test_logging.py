# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def test_logging(shell):
    test = (
        ShellTest(shell)
        .statement("CALL enable_logging('QueryLog', storage='stdout')")
        .statement('SELECT 1 as a')
    )
    result = test.run()
    result.check_stdout("QueryLog\tINFO\tSELECT 1 as a;\n┌───────┐")

# fmt: on
