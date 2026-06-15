"""LLDB helpers for DuckDB sqllogictest debugging."""

from __future__ import annotations

COMMAND_NAME = "current_statement"


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand(
        "command script add --overwrite -h \"Show the active DuckDB sqllogictest location and SQL\" "
        "-f duckdb_sqllogictest.current_statement {}".format(COMMAND_NAME)
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

    result.AppendMessage("\n".join(lines))


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
