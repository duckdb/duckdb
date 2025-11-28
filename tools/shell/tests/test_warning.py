import os
from conftest import ShellTest


def test_warning(shell):
    test = ShellTest(shell).statement("SELECT list_transform([1], x -> x);")

    result = test.run()
    result.check_stdout("Warning:")
    result.check_stdout("Deprecated lambda arrow (->) detected.")
    result.check_stdout("[1]")
