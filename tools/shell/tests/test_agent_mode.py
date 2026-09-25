# fmt: off

import pytest
from conftest import ShellTest

# Agent mode renders output for an AI coding agent that reads it through a pipe: every row as a compact markdown
# table (no padding, the type in the header cell, a row count when it is not obvious), JSON errors, compact plans,
# and estimate/progress lines on stderr. It is detected from the environment variables coding agents set for the
# commands they run, and can be forced with -agent / -no-agent.

FIFTY_ROWS = "SELECT range AS r FROM range(50)"

def agent_shell(shell, agent_var="AI_AGENT", agent_value="test-agent"):
    return ShellTest(shell).env_var(agent_var, agent_value)

def test_default_is_unchanged(shell):
    # without an agent in the environment, nothing changes: unicode borders, 40-row limit
    test = ShellTest(shell).statement(FIFTY_ROWS)
    result = test.run()
    result.check_stdout("┌")
    result.check_stdout("50 rows")
    result.check_stdout("40 shown")

@pytest.mark.parametrize("agent_var,agent_value", [
    ("AI_AGENT", "claude-code_2-1-282_agent"),
    ("AGENT", "amp"),
    ("CLAUDECODE", "1"),
    ("CODEX_CI", "1"),
    ("CURSOR_AGENT", "1"),
    ("GEMINI_CLI", "1"),
    ("COPILOT_AGENT", "1"),
])
def test_detect_agent(shell, agent_var, agent_value):
    test = agent_shell(shell, agent_var, agent_value).statement(FIFTY_ROWS)
    result = test.run()
    # compact markdown, types in the header, all rows
    result.check_stdout("| r:BIGINT |\n|---|\n| 0 |")
    result.check_stdout("| 49 |\n50 rows")
    result.check_not_exist("shown")
    result.check_not_exist("┌")

def test_empty_variable_is_not_an_agent(shell):
    test = agent_shell(shell, "AI_AGENT", "").statement(FIFTY_ROWS)
    result = test.run()
    result.check_stdout("┌")
    result.check_stdout("40 shown")

def test_compact_table(shell):
    # no alignment padding, the type in the header cell, no row count for a handful of rows
    test = agent_shell(shell).statement("SELECT 42 AS a, 'hello' AS b, NULL AS c, 'x|y' AS d")
    result = test.run()
    result.check_stdout("""| a:INTEGER | b:VARCHAR | c:NULL | d:VARCHAR |
|---|---|---|---|
| 42 | hello | NULL | x\\|y |""")
    result.check_not_exist("row")

def test_row_count_footer(shell):
    # the row count is rendered from 10 rows on, where counting is error-prone
    test = agent_shell(shell).statement("SELECT range AS r FROM range(9)").statement("SELECT range AS r FROM range(10)")
    result = test.run()
    result.check_stdout("| 8 |\n| r:BIGINT |")
    result.check_stdout("| 9 |\n10 rows")

def test_nested_types_in_header(shell):
    test = agent_shell(shell).statement("SELECT sum(i) AS total, list(i) AS l, {'x': max(i)} AS s FROM range(3) t(i)")
    result = test.run()
    result.check_stdout("| total:HUGEINT | l:BIGINT[] | s:STRUCT(x BIGINT) |")

def test_empty_result(shell):
    test = agent_shell(shell).statement("SELECT 42 AS a WHERE false")
    result = test.run()
    result.check_stdout("| a:INTEGER |\n|---|\n0 rows")

def test_no_agent_flag(shell):
    test = agent_shell(shell).add_argument("-no-agent").statement(FIFTY_ROWS)
    result = test.run()
    result.check_stdout("┌")
    result.check_stdout("40 shown")

def test_agent_flag(shell):
    # no agent in the environment, but forced on
    test = ShellTest(shell).add_argument("-agent").statement(FIFTY_ROWS)
    result = test.run()
    result.check_stdout("| r:BIGINT |")
    result.check_stdout("| 49 |\n50 rows")
    result.check_not_exist("shown")

def test_explicit_mode_wins(shell):
    # an explicit output mode overrides the agent default
    test = agent_shell(shell).add_argument("-csv").statement("SELECT 42 AS a")
    result = test.run()
    result.check_stdout("a\n42")
    result.check_not_exist("|")

def test_duckbox_all_rows(shell):
    # switching back to duckbox keeps every row (unless .maxrows says otherwise)
    test = agent_shell(shell).statement(".mode duckbox").statement(FIFTY_ROWS)
    result = test.run()
    result.check_stdout("│    49 │")
    result.check_not_exist("shown")

def test_maxrows_wins(shell):
    test = agent_shell(shell).statement(".mode duckbox").statement(".maxrows 10").statement(FIFTY_ROWS)
    result = test.run()
    result.check_stdout("10 shown")

def test_show(shell):
    test = agent_shell(shell, "AI_AGENT", "some-agent").statement(".show")
    result = test.run()
    result.check_stdout("agent: some-agent")

def test_show_marker_name(shell):
    test = agent_shell(shell, "CLAUDECODE", "1").statement(".show")
    result = test.run()
    result.check_stdout("agent: claude-code")

def test_json_error(shell):
    test = agent_shell(shell).statement("SELECT foo FROM (SELECT 1 AS bar)")
    result = test.run()
    assert result.status_code == 1
    result.check_stderr('"exception_type":"Binder"')
    result.check_stderr('"exception_message":"Referenced column \\"foo\\" not found in FROM clause!')
    result.check_stderr('"position":"7"')
    # the LINE/caret block is gone, and so is the candidates field (the message already lists them)
    assert "LINE 1" not in result.stderr
    assert "^^^" not in result.stderr
    assert '"candidates"' not in result.stderr

def test_json_parser_error(shell):
    test = agent_shell(shell).statement("SELEC 1")
    result = test.run()
    assert result.status_code == 1
    result.check_stderr('"exception_type":"Parser"')

def test_json_shell_error(shell):
    # errors raised by the shell itself (not the engine) are JSON too
    test = agent_shell(shell).statement(".nonsense")
    result = test.run()
    result.check_stderr('"exception_type":"Invalid Input"')
    result.check_stderr('"exception_message":"Unknown Command Error: Unrecognized command \'nonsense\'')

def test_error_without_agent(shell):
    test = ShellTest(shell).statement("SELECT foo FROM (SELECT 1 AS bar)")
    result = test.run()
    result.check_stderr("LINE 1:")
    assert "exception_type" not in result.stderr

def test_compact_explain(shell):
    test = agent_shell(shell).statement("EXPLAIN SELECT count(*) FROM range(10) r JOIN range(5) s ON r.range = s.range")
    result = test.run()
    result.check_stdout("UNGROUPED_AGGREGATE (est=")
    result.check_stdout("\n  HASH_JOIN (est=")
    result.check_stdout("Join Type: INNER; Conditions:")
    result.check_not_exist("╭")

def test_compact_explain_analyze(shell):
    test = agent_shell(shell).statement("EXPLAIN ANALYZE SELECT count(*) FROM range(10)")
    result = test.run()
    result.check_stdout("UNGROUPED_AGGREGATE (est=")
    result.check_stdout(", rows=1, time=")

def test_explicit_explain_format_wins(shell):
    test = agent_shell(shell).statement("EXPLAIN (FORMAT json) SELECT 42")
    result = test.run()
    result.check_stdout('"name":')
    result.check_not_exist("(est=")

def test_progress_on_stderr(shell):
    # progress lines are printed to stderr; progress_bar_time=0 makes them appear immediately
    test = (
        agent_shell(shell)
        .statement("SET progress_bar_time=0")
        .statement("SELECT count(*) FROM range(50000000)")
    )
    result = test.run()
    result.check_stdout("50000000")
    result.check_stderr("progress: ")
    result.check_stderr("% (elapsed ")
    result.check_stderr("progress: done (elapsed ")

def test_no_progress_without_agent(shell):
    test = (
        ShellTest(shell)
        .statement("SET progress_bar_time=0")
        .statement("SELECT count(*) FROM range(50000000)")
    )
    result = test.run()
    result.check_stdout("50000000")
    assert "progress:" not in result.stderr

def test_estimate_on_stderr(shell):
    # before a query runs, the planner's estimate of what it reads and returns goes to stderr
    test = (
        agent_shell(shell)
        .statement("CREATE TABLE t AS SELECT range AS i FROM range(1000)")
        .statement("SELECT count(*) FROM t JOIN range(10) r ON t.i = r.range")
    )
    result = test.run()
    result.check_stdout("| count_star():BIGINT |\n|---|\n| 10 |")
    # no "rows returned" for a statement that does not return rows
    result.check_stderr("estimate: ~1000 rows read (range ~1000)\n")
    result.check_stderr("estimate: ~1010 rows read (t ~1000, range ~10), ~1 rows returned")

def test_no_estimate_without_scans(shell):
    test = (
        agent_shell(shell)
        .statement("CREATE TABLE t(i INTEGER)")
        .statement("SET threads=2")
        .statement("SELECT 42")
    )
    result = test.run()
    result.check_stdout("42")
    assert "estimate:" not in result.stderr

def test_no_estimate_without_agent(shell):
    test = ShellTest(shell).statement("SELECT count(*) FROM range(1000)")
    result = test.run()
    assert "estimate:" not in result.stderr

def test_echo_second_statement(shell):
    # the statement text the shell works with must be each statement's own text
    test = ShellTest(shell).add_argument("-echo", "-c", "SELECT 1 AS a; SELECT 2 AS b")
    result = test.run()
    result.check_stdout("SELECT 2 AS b")


def test_tables_compact(shell):
    # .tables lists one line per table instead of the box layout
    test = (
        agent_shell(shell)
        .statement("CREATE TABLE t1(a INTEGER PRIMARY KEY, b VARCHAR)")
        .statement("CREATE VIEW v1 AS SELECT a FROM t1")
        .statement(".tables")
    )
    result = test.run()
    result.check_stdout("memory.main.t1 (table, ~0 rows): a INTEGER PK, b VARCHAR")
    result.check_stdout("memory.main.v1 (view): a INTEGER")
    result.check_not_exist("\x1b[")
