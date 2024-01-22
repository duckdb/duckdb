# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

def test_temp_directory(shell, tmp_path):
    temp_dir = tmp_path / 'random_dir'
    temp_dir.mkdir()
    temp_file = temp_dir / 'myfile'
    with open(temp_file, 'w+') as f:
        f.write('hello world')
    test = (
        ShellTest(shell)
        .statement(f"SET temp_directory='{temp_dir.as_posix()}';")
        .statement("PRAGMA memory_limit='2MB';")
        .statement("CREATE TABLE t1 AS SELECT * FROM range(1000000);")
    )
    result = test.run()

    # make sure the temp directory or existing files are not deleted
    assert os.path.isdir(temp_dir)
    with open(temp_file, 'r') as f:
        assert f.read() == "hello world"

    # all other files are gone
    assert os.listdir(temp_dir) == ['myfile']

    os.remove(temp_file)
    os.rmdir(temp_dir)

    test = (
        ShellTest(shell)
        .statement(f"SET temp_directory='{temp_dir.as_posix()}';")
        .statement("PRAGMA memory_limit='2MB';")
        .statement("CREATE TABLE t1 AS SELECT * FROM range(1000000);")
    )
    result = test.run()

    # make sure the temp directory is deleted
    assert not os.path.exists(temp_dir)
    assert not os.path.isdir(temp_dir)

# fmt: on
