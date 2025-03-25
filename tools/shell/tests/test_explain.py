# fmt: off

from conftest import ShellTest

def test_invalid_explain(shell):
    test = (
        ShellTest(shell)
        .statement("EXPLAIN SELECT 'any_string' IN ?;")
    )
    result = test.run()
