# fmt: off

import pytest
from conftest import ShellTest


# a result within the fetch cap is rendered like duckbox: exact row count, first and last rows
def test_preview_small_result(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox_preview")
        .statement("SELECT * FROM range(100) t(i)")
    )
    result = test.run()
    result.check_stdout("100 rows")
    result.check_stdout("99")
    result.check_not_exist("+ rows")


# beyond the fetch cap only the first rows are rendered, and the row count is a lower bound
def test_preview_capped_result(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox_preview")
        .statement("SELECT * FROM range(3_000_000) t(i)")
    )
    result = test.run()
    result.check_stdout("+ rows")
    result.check_stdout("40 shown")
    result.check_stdout("39")
    # the head ends with a divider, as the result continues
    result.check_stdout("·")
    result.check_not_exist("2999999")


# `_` fetches the rest of a capped result
def test_preview_last_result_consumes_rest(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox_preview")
        .statement("SELECT * FROM range(3_000_000) t(i)")
        .statement("SELECT count(*), max(i) FROM _")
    )
    result = test.run()
    result.check_stdout("3000000")
    result.check_stdout("2999999")


# a `_` inside a string also counts
def test_preview_last_result_in_string(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox_preview")
        .statement("SELECT * FROM range(3_000_000) t(i)")
        .statement("FROM query('SELECT count(*) AS cnt FROM _')")
    )
    result = test.run()
    result.check_stdout("3000000")


# a statement that does not refer to `_` cancels the open query - `_` is then its own result
def test_preview_next_statement_cancels(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox_preview")
        .statement("SELECT * FROM range(3_000_000) t(i)")
        .statement("SELECT 42 AS answer")
        .statement("SELECT count(*) AS cnt, max(answer) FROM _")
    )
    result = test.run()
    assert result.stderr == ""
    result.check_stdout("42")
    result.check_not_exist("3000000")


# .last fetches the rest of a capped result and renders all of it
def test_preview_dot_last(shell):
    test = (
        ShellTest(shell)
        .statement(".mode duckbox_preview")
        .statement("SELECT * FROM range(1_100_000) t(i)")
        .statement(".last")
    )
    result = test.run()
    result.check_stdout("+ rows")
    result.check_stdout("1099999")


# an error within the fetched rows is reported
def test_preview_error_within_cap(shell):
    test = (
        ShellTest(shell)
        .statement("SET threads=1")
        .statement(".mode duckbox_preview")
        .statement("SELECT CASE WHEN i = 500_000 THEN error('boom') ELSE i END AS i FROM range(3_000_000) t(i)")
    )
    result = test.run()
    result.check_stderr("boom")
    assert "+ rows" not in result.stdout


# an error beyond the fetched rows is not seen - unless the rest is fetched into `_`
def test_preview_error_beyond_cap(shell):
    query = "SELECT CASE WHEN i = 2_000_000 THEN error('boom') ELSE i END AS i FROM range(3_000_000) t(i)"
    test = (
        ShellTest(shell)
        .statement("SET threads=1")
        .statement(".mode duckbox_preview")
        .statement(query)
        .statement("SELECT 42")
    )
    result = test.run()
    assert "boom" not in result.stderr
    result.check_stdout("+ rows")

    test = (
        ShellTest(shell)
        .statement("SET threads=1")
        .statement(".mode duckbox_preview")
        .statement(query)
        .statement("SELECT count(*) FROM _")
    )
    result = test.run()
    result.check_stderr("boom")


# .materialize preview makes duckbox mode fetch only the first rows
def test_materialize_preview_setting(shell):
    test = (
        ShellTest(shell)
        .statement(".materialize preview")
        .statement(".mode duckbox")
        .statement("SELECT * FROM range(3_000_000) t(i)")
        .statement(".materialize auto")
        .statement("SELECT * FROM range(3_000_000) t(i)")
    )
    result = test.run()
    result.check_stdout("+ rows")
    result.check_stdout("  3000000 rows")


# .materialize without argument fetches the rest of the previous result into `_`, and reports the row count
def test_materialize_previous_result(shell):
    test = (
        ShellTest(shell)
        .statement(".materialize preview")
        .statement("SELECT * FROM range(3_000_000) t(i)")
        .statement(".materialize")
        .statement("SELECT max(i) FROM _")
    )
    result = test.run()
    result.check_stdout("3000000 rows")
    result.check_stdout("2999999")


# .materialize without argument reports an error beyond the fetched rows
def test_materialize_previous_result_error(shell):
    test = (
        ShellTest(shell)
        .statement("SET threads=1")
        .statement(".materialize preview")
        .statement("SELECT CASE WHEN i = 2_000_000 THEN error('boom') ELSE i END AS i FROM range(3_000_000) t(i)")
        .statement(".materialize")
    )
    result = test.run()
    result.check_stderr("boom")


# .materialize full also fetches the whole result first in a streamed mode - which then keeps it as `_`
def test_materialize_full_streamed_mode(shell):
    test = (
        ShellTest(shell)
        .statement(".mode csv")
        .statement(".materialize full")
        .statement("SELECT 42 AS a")
        .statement("SELECT a + 1 AS b FROM _")
    )
    result = test.run()
    result.check_stdout("43")


# .materialize rows N sets the cap - the rest is still fetched into `_` when asked for
def test_materialize_rows(shell):
    test = (
        ShellTest(shell)
        .statement(".materialize rows 5K")
        .statement("SELECT * FROM range(1_000_000) t(i)")
        .statement("SELECT count(*) AS cnt FROM _")
    )
    result = test.run()
    result.check_stdout("+ rows")
    result.check_stdout("1000000")
    # far fewer rows than the default cap were fetched before rendering
    assert "1001472+ rows" not in result.stdout


# a result that ends just past the cap has an exact row count
def test_materialize_exact_past_cap(shell):
    test = (
        ShellTest(shell)
        .statement(".materialize preview")
        .statement("SELECT * FROM range(1_000_123) t(i)")
    )
    result = test.run()
    result.check_stdout("1000123 rows")
    assert "+ rows" not in result.stdout


def test_materialize_rows_invalid(shell):
    test = (
        ShellTest(shell)
        .statement(".materialize rows 0")
    )
    result = test.run()
    result.check_stderr("positive row count")


# an error deep in the result (row 10M): not seen by the preview, cancelled by a plain next statement, and rethrown
# when the rest of the result is fetched
def test_preview_error_at_10m(shell):
    query = "SELECT CASE WHEN i = 10_000_000 THEN error('boom at 10M') ELSE i END AS i FROM range(20_000_000) t(i)"
    test = (
        ShellTest(shell)
        .statement(".materialize preview")
        .statement(query)
        .statement("SELECT 42 AS answer")
    )
    result = test.run()
    result.check_stdout("+ rows")
    result.check_stdout("answer")
    assert "boom" not in result.stderr

    for fetch_rest in [".materialize", ".last", "SELECT count(*) FROM _"]:
        test = (
            ShellTest(shell)
            .statement(".materialize preview")
            .statement(query)
            .statement(fetch_rest)
        )
        result = test.run()
        assert result.status_code == 1
        assert "+ rows" in result.stdout
        assert "boom at 10M" in result.stderr


# fmt: on
