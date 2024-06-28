# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def test_profiling_json(shell, tmp_path):
    target_dir = tmp_path / 'export_test'
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(".changes off")
        .statement("CREATE TABLE integers(i INTEGER);")
        .statement("CREATE TABLE integers2(i INTEGER);")
        .statement("INSERT INTO integers SELECT * FROM range(100);")
        .statement("INSERT INTO integers2 VALUES (1), (3), (99);")
        .statement(f"EXPORT DATABASE '{target_dir.as_posix()}';")
        .statement("DROP TABLE integers;")
        .statement("DROP TABLE integers2;")
        .statement(f"IMPORT DATABASE '{target_dir.as_posix()}';")
        .statement("SELECT SUM(i)*MAX(i) FROM integers JOIN integers2 USING (i);")
    )
    result = test.run()
    result.check_stdout('10197')

# fmt: on
