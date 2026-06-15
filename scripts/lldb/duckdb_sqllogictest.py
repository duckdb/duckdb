"""LLDB helpers for DuckDB sqllogictest debugging."""

from __future__ import annotations

import lldb

COMMAND_NAME = "current_statement"
NEXT_COMMAND_NAME = "next_statement"
_NEXT_STATE = None


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand(
        "command script add --overwrite -h \"Show the active DuckDB sqllogictest location and SQL\" "
        "-f duckdb_sqllogictest.current_statement {}".format(COMMAND_NAME)
    )
    debugger.HandleCommand(
        "command script add --overwrite -h \"Continue until the next sqllogictest statement\" "
        "-f duckdb_sqllogictest.next_statement {}".format(NEXT_COMMAND_NAME)
    )


def current_statement(debugger, command, result, _internal_dict):
    if command.strip():
        result.SetError("usage: {}".format(COMMAND_NAME))
        return

    target = debugger.GetSelectedTarget()
    process = target.GetProcess()
    if not process.IsValid():
        result.SetError("no selected process")
        return

    thread = process.GetThreadAtIndex(0)
    if not thread.IsValid():
        result.SetError("thread 1 not found")
        return

    match = _find_sqllogictest_frame(thread)
    if match is None:
        result.SetError("no sqllogictest frame found on thread 1")
        return

    result.AppendMessage(_format_current_statement(match))


def next_statement(debugger, command, result, _internal_dict):
    if command.strip():
        result.SetError("usage: {}".format(NEXT_COMMAND_NAME))
        return

    target = debugger.GetSelectedTarget()
    process = target.GetProcess()
    if not process.IsValid():
        result.SetError("no selected process")
        return
    if process.GetState() != lldb.eStateStopped:
        result.SetError("process must be stopped")
        return

    thread = process.GetThreadAtIndex(0)
    if not thread.IsValid():
        result.SetError("thread 1 not found")
        return

    _restore_next_state(target)

    helper_breakpoint = target.BreakpointCreateByName("query_break")
    if not helper_breakpoint.IsValid() or helper_breakpoint.GetNumLocations() == 0:
        if helper_breakpoint.IsValid():
            target.BreakpointDelete(helper_breakpoint.GetID())
        result.SetError("could not set a breakpoint on query_break")
        return

    helper_breakpoint.SetOneShot(True)
    helper_breakpoint.SetThreadID(thread.GetThreadID())
    helper_breakpoint.SetScriptCallbackFunction("duckdb_sqllogictest._next_statement_callback")

    saved_auto_continue = {}
    for breakpoint in _iter_breakpoints(target):
        if breakpoint.GetID() == helper_breakpoint.GetID():
            continue
        saved_auto_continue[breakpoint.GetID()] = breakpoint.GetAutoContinue()
        if breakpoint.IsEnabled():
            breakpoint.SetAutoContinue(True)

    global _NEXT_STATE
    _NEXT_STATE = {
        "helper_breakpoint_id": helper_breakpoint.GetID(),
        "saved_auto_continue": saved_auto_continue,
    }

    command_interpreter = debugger.GetCommandInterpreter()
    command_result = lldb.SBCommandReturnObject()
    command_interpreter.HandleCommand("process continue", command_result)
    if not command_result.Succeeded():
        _restore_next_state(target)
        result.SetError(command_result.GetError() or "process continue failed")


def _find_sqllogictest_frame(thread):
    best_match = None
    for frame in thread:
        function_name = frame.GetFunctionName() or ""
        frame_kind = _classify_frame(function_name)
        if frame_kind is None:
            continue

        match = {
            "frame": frame,
            "priority": frame_kind["priority"],
            "file_name": None,
            "query_line": None,
            "sql_text": None,
        }

        if frame_kind["style"] == "execute_internal":
            match["file_name"] = _read_cpp_string(frame, "this->file_name")
            match["query_line"] = _read_int(frame, "this->query_line")
            match["sql_text"] = _read_cpp_string(frame, "context.sql_query")
            if not match["sql_text"]:
                match["sql_text"] = _read_cpp_string(frame, "this->base_sql_query")
        else:
            match["file_name"] = _read_cpp_string(frame, "file_name")
            match["query_line"] = _read_int(frame, "query_line")
            match["sql_text"] = _read_cpp_string(frame, "context.sql_query")

        if best_match is None or match["priority"] < best_match["priority"]:
            best_match = match

    return best_match


def _classify_frame(function_name):
    if "duckdb::Query::ExecuteInternal" in function_name:
        return {"priority": 0, "style": "execute_internal"}
    if "duckdb::Statement::ExecuteInternal" in function_name:
        return {"priority": 0, "style": "execute_internal"}
    if "duckdb::Command::ExecuteQuery" in function_name:
        return {"priority": 1, "style": "execute_query"}
    return None


def _read_cpp_string(frame, expression):
    value = frame.EvaluateExpression("(const char *)({}).c_str()".format(expression))
    if not value.IsValid() or not value.GetError().Success():
        return None

    summary = value.GetSummary()
    if summary:
        return _strip_quotes(summary)

    raw_value = value.GetValue()
    if raw_value:
        return raw_value
    return None


def _read_int(frame, expression):
    value = frame.EvaluateExpression("(long long)({})".format(expression))
    if not value.IsValid() or not value.GetError().Success():
        return None
    return value.GetValueAsSigned()


def _strip_quotes(text):
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1]
    return text


def _iter_breakpoints(target):
    for index in range(target.GetNumBreakpoints()):
        yield target.GetBreakpointAtIndex(index)


def _restore_next_state(target):
    global _NEXT_STATE
    if not _NEXT_STATE:
        return

    for breakpoint_id, auto_continue in _NEXT_STATE["saved_auto_continue"].items():
        breakpoint = target.FindBreakpointByID(breakpoint_id)
        if breakpoint.IsValid():
            breakpoint.SetAutoContinue(auto_continue)

    helper_breakpoint_id = _NEXT_STATE["helper_breakpoint_id"]
    helper_breakpoint = target.FindBreakpointByID(helper_breakpoint_id)
    if helper_breakpoint.IsValid():
        target.BreakpointDelete(helper_breakpoint_id)

    _NEXT_STATE = None


def _next_statement_callback(frame, _breakpoint_location, _internal_dict):
    target = frame.GetThread().GetProcess().GetTarget()
    _restore_next_state(target)

    thread = frame.GetThread().GetProcess().GetThreadAtIndex(0)
    if thread.IsValid():
        match = _find_sqllogictest_frame(thread)
        if match is not None:
            print(_format_current_statement(match))

    return True


def _format_current_statement(match):
    lines = []
    file_name = match["file_name"] or "<unknown file>"
    query_line = match["query_line"]
    if query_line is not None:
        lines.append("{}:{}".format(file_name, query_line))
    else:
        lines.append(file_name)

    sql_text = match["sql_text"]
    if sql_text:
        lines.append(sql_text)
    return "\n".join(lines)
