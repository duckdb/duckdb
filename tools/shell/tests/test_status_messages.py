# fmt: off

import pytest
from conftest import ShellTest

# Status messages (duckdb::ClientStatus) report what a statement is doing - e.g. provisioning an external resource -
# on the progress display. In agent mode that display prints them as "status:" lines on stderr.

# a resource type whose status reports CREATE_IN_PROGRESS for the first polls, then the given final state
def resource_type(final_state):
    return [
        "CREATE SEQUENCE polls",
        "CREATE MACRO t_create(p) AS TABLE SELECT MAP {'id': 'x'} AS handle",
        "CREATE MACRO t_status(h) AS TABLE SELECT CASE WHEN nextval('polls') < 3 THEN 'CREATE_IN_PROGRESS' ELSE '"
        + final_state + "' END AS state, MAP {'uri': 'localhost', 'attached_db_type': 'quack'} AS result",
        "CREATE MACRO t_destroy(h) AS TABLE SELECT 'gone' AS status",
        "CALL register_external_resource_type('test@local', kind := 'catalog', create_function := 't_create', "
        "status_function := 't_status', destroy_function := 't_destroy')",
    ]

CREATE_RESOURCE = "FROM create_external_resource('test@local', resource_name := 'r', poll_interval_seconds := 1)"


def agent_shell(shell):
    return ShellTest(shell).add_argument("-agent")


def test_status_lines_in_agent_mode(shell):
    test = agent_shell(shell).query(*resource_type("ready")).statement(CREATE_RESOURCE)
    result = test.run()
    result.check_stderr("status: Creating resource r (elapsed ")
    result.check_stderr("status: Waiting for resource r (CREATE_IN_PROGRESS) (elapsed ")
    result.check_stdout("localhost")


def test_failure_reports_status(shell):
    # a statement that fails inside a status scope says what it was doing
    test = agent_shell(shell).query(*resource_type("failed")).statement(CREATE_RESOURCE)
    result = test.run()
    assert result.status_code == 1
    assert "while: Waiting for resource r (failed)" in result.stderr
    assert "reported state 'failed'" in result.stderr


def test_no_failure_context_outside_scope(shell):
    # an error outside any status scope has no "while:" line - also not one left over from an earlier statement
    test = agent_shell(shell).query(*resource_type("failed")).statement(CREATE_RESOURCE).statement("SELECT 1/0::INT")
    result = test.run()
    assert result.stderr.count("while:") == 1


def test_no_status_without_progress_display(shell):
    # without a terminal or agent mode there is no progress display - and so no status output
    test = ShellTest(shell).add_argument("-no-agent").query(*resource_type("ready")).statement(CREATE_RESOURCE)
    result = test.run()
    assert "status:" not in result.stderr
    result.check_stdout("localhost")


# fmt: on
