# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

def test_invalid_explain(shell):
    test = (
        ShellTest(shell)
        .statement("EXPLAIN SELECT 'any_string' IN ?;")
    )
    result = test.run()
